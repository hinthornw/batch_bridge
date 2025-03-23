import asyncio
import datetime
import typing
import uuid
from collections import defaultdict

from langgraph.config import get_config
from langgraph.constants import CONF, CONFIG_KEY_TASK_ID
from langgraph.errors import GraphInterrupt
from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Send, interrupt
from langgraph_sdk import get_client
from typing_extensions import Annotated, TypedDict

T = typing.TypeVar("T")
U = typing.TypeVar("U")
V = typing.TypeVar("V")
MISSING = object()


_langgraph_client = get_client()


class BatchIngestException(Exception):
    def __init__(self, detail: str):
        self.detail = detail


class Origin(TypedDict):
    assistant_id: str
    thread_id: str
    task_id: str  # UUID


class InFlightBatch(TypedDict, typing.Generic[T]):
    batch_id: str
    batch_payload: T
    origins: typing.Sequence[Origin]


class QueueItem(TypedDict, typing.Generic[T]):
    origin: Origin
    task: T


class RemoveBatch(typing.NamedTuple):
    batch_id: str


def Bridge(
    submit: typing.Callable[[list[T]], typing.Awaitable[U]],
    poll: typing.Callable[[U], typing.Awaitable[V]],
    *,
    should_submit: typing.Optional[
        typing.Callable[[list[T], typing.Optional[datetime.datetime]], bool]
    ] = None,
    # job_ttl: typing.Optional[datetime.timedelta] = MISSING, # type: ignore
) -> CompiledStateGraph:
    if not asyncio.iscoroutinefunction(submit):
        raise ValueError("submit must be a coroutine function")
    if not asyncio.iscoroutinefunction(poll):
        raise ValueError("poll must be a coroutine function")

    class InputSchema(TypedDict):
        tasks: typing.Annotated[list[QueueItem[T]], _reduce_batch]
        event: typing.Optional[typing.Literal["poll", "submit"]]
        in_flight: Annotated[list[InFlightBatch[T]], _reduce_in_flight]

    class State(InputSchema):
        last_submit_time: datetime.datetime

    async def route_entry(
        state: State,
    ) -> typing.Union[
        typing.Literal["create_batch"],
        typing.Sequence[Send],
    ]:
        # The bridge stuff can be triggered when:
        # 1. A new task is enqueued
        # 2. A auto-cron to poll tasks is triggered
        if state.get("event") == "submit":
            return "create_batch"
        elif state.get("event") == "poll":
            return [Send("poll_batch", batch_val) for batch_val in state["in_flight"]]
        else:
            raise ValueError("Invalid event")

    async def create_batch(state: State) -> dict:
        tasks = state.get("tasks", [])
        if not tasks:
            return {}
        task_values = [task["task"] for task in tasks]
        if should_submit is not None and not should_submit(
            task_values, state["last_submit_time"]
        ):
            return
        # Known issue: if the submission is larger than a single batch can handle
        # ergonomics are bad.
        batch_payload = await submit(task_values)
        # Start the poller
        batch_id = str(uuid.uuid4())
        configurable = get_config()[CONF]
        assistant_id = configurable["assistant_id"]
        await _langgraph_client.crons.create(
            assistant_id=assistant_id,
            schedule="* * * * * *",
            input={
                "event": "poll",
                "in_flight": InFlightBatch(
                    batch_id=batch_id,
                    batch_payload=batch_payload,
                    origins=[task["origin"] for task in tasks],
                ),
            },
            multitask_strategy="reject",
        )
        return {
            "last_submit_time": datetime.datetime.now(datetime.timezone.utc),
            "tasks": "__clear__",
            "in_flight": [],
        }

    async def poll_batch(state: InFlightBatch) -> dict:
        poll_result = await poll(state["batch_payload"])

        if poll_result is not None:
            # Now we need to re-trigger ALL the original tasks
            to_resume = defaultdict(dict)
            if isinstance(poll_result, Exception):
                response = {
                    "__batch_bridge__": {
                        "kind": "exception",
                        "detail": str(poll_result),
                    }
                }
                for origin in state["origins"]:
                    to_resume[(origin["assistant_id"], origin["thread_id"])][
                        origin["task_id"]
                    ] = response
            else:
                for origin, result in zip(state["origins"], poll_result):
                    to_resume[(origin["assistant_id"], origin["thread_id"])][
                        origin["task_id"]
                    ] = result
            await asyncio.gather(
                *[
                    _langgraph_client.runs.create(
                        thread_id=thread_id,
                        assistant_id=assistant_id,
                        command={"resume": task_resumes},
                    )
                    for (assistant_id, thread_id), task_resumes in to_resume.items()
                ],
                # Ignore errors here - we'll just move on
                return_exceptions=True,
            )
            # Stop polling for this batch in particular
            configurable = get_config()[CONF]
            await _langgraph_client.crons.delete(configurable["cron_id"])
            return {
                "in_flight": RemoveBatch(state["batch_id"]),
            }

    return (
        StateGraph(State, input=InputSchema)
        .add_node(poll_batch)
        .add_node(create_batch)
        .add_conditional_edges("__start__", route_entry, ["create_batch", "poll_batch"])
        .compile(name="Bridge")
    )


async def wait(
    item: T,
    *,
    batcher: str,  # Name of the graph in your deployment. Would love to remove this
    batcher_thread_id: str = "b0be531d-55f6-4b87-a309-23d2ed28d9da",
) -> None:
    configurable = get_config()[CONF]
    task_id = configurable[CONFIG_KEY_TASK_ID]
    assistant_id = configurable["assistant_id"]
    origin_thread_id = configurable["thread_id"]
    task = {
        "task": item,
        "origin": {
            "assistant_id": assistant_id,
            "thread_id": origin_thread_id,
            "task_id": task_id,
        },
    }
    try:
        result = interrupt(task)
        if isinstance(result, dict) and (out_of_band := result.get("__batch_bridge__")):
            if out_of_band["kind"] == "exception":
                raise BatchIngestException(out_of_band["detail"])
            raise NotImplementedError(f"Unknown out of band type: {out_of_band}")
        return result
    except GraphInterrupt:
        await _langgraph_client.runs.create(
            thread_id=batcher_thread_id,
            assistant_id=batcher,
            if_not_exists="create",
            multitask_strategy="enqueue",
            input={
                "event": "submit",
                "tasks": task,
            },
        )
        raise


def _reduce_batch(
    existing: list[T] | None, new: T | typing.Literal["__clear__"]
) -> list[T]:
    if new == "__clear__":
        return []
    existing = existing if existing is not None else []
    return [*existing, new]


def _reduce_in_flight(
    existing: list[InFlightBatch[T]] | None, new: InFlightBatch[T] | RemoveBatch
) -> list[InFlightBatch[T]]:
    if isinstance(new, RemoveBatch):
        return [batch for batch in existing if batch["batch_id"] != new.batch_id]
    if isinstance(new, dict) and "batch_id" in new:
        new = [new]
    existing = existing if existing is not None else []
    return [*existing, *new]
