import asyncio
import datetime
import os
import typing
import uuid
from collections import defaultdict

from langgraph.config import get_config
from langgraph.constants import CONF, CONFIG_KEY_TASK_ID
from langgraph.errors import GraphInterrupt
from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command, Send, interrupt
from langgraph_api.graph import register_graph_sync
from langgraph_sdk import get_client
from typing_extensions import Annotated, TypedDict

from batch_bridge.errors import BatchIngestException
from batch_bridge.types import InFlightBatch, QueueItem, RemoveBatch, T, U, V

if typing.TYPE_CHECKING:
    pass

DEFAULT_THREAD_ID = "b0be531d-55f6-4b87-a309-23d2ed28d9da"
USE_CRONS = os.getenv("LANGSMITH_LANGGRAPH_API_VARIANT") != "local_dev"


_langgraph_client = get_client()


class CompiledBridge(CompiledStateGraph, typing.Generic[T, U]):
    def __init__(
        self,
        *args: typing.Any,
        graph_id: str = "BatchBridge",
        __output_coercer__: typing.Callable[[U], typing.Any] | None = None,
        **kwargs: typing.Any,
    ) -> None:
        graph_id_ = kwargs.pop("__graph_id__", graph_id)
        super().__init__(*args, **kwargs)
        if graph_id_ is not None:
            self.__graph_id__ = graph_id_
        else:
            self.__graph_id__ = graph_id
        self.__output_coercer__ = __output_coercer__

    async def wait(self, item: T, *, thread_id: str = DEFAULT_THREAD_ID) -> U:
        result = await wait(item, bridge_id=self.__graph_id__, thread_id=thread_id)
        if self.__output_coercer__ is not None:
            result = self.__output_coercer__(result)
        return result


class Bridge:
    """A bridge for batch processing."""

    def __new__(
        self,
        submit: typing.Callable[[list[T]], typing.Awaitable[U]],
        poll: typing.Callable[[U], typing.Awaitable[V]],
        *,
        should_submit: typing.Optional[
            typing.Callable[[list[T], typing.Optional[datetime.datetime]], bool]
        ] = None,
        graph_id: str = "BatchBridge",
        __output_coercer__: typing.Callable[[U], V] | None = None,
        # job_ttl: typing.Optional[datetime.timedelta] = MISSING, # type: ignore
    ) -> CompiledBridge:
        if not asyncio.iscoroutinefunction(submit):
            raise ValueError("submit must be a coroutine function")
        if not asyncio.iscoroutinefunction(poll):
            raise ValueError("poll must be a coroutine function")
        if should_submit is None:
            should_submit = _submit_after_minute

        class InputSchema(TypedDict):
            tasks: typing.Annotated[list[QueueItem[T]], _reduce_batch]
            event: typing.Optional[typing.Literal["poll", "submit"]]
            in_flight: Annotated[list[InFlightBatch[T]], _reduce_in_flight]

        class State(InputSchema):
            last_submit_time: datetime.datetime

        async def route_entry(
            state: State,
        ) -> typing.Union[
            typing.Literal["check_should_submit"],
            typing.Sequence[typing.Union[Send, typing.Literal["check_should_submit"]]],
        ]:
            # The bridge stuff can be triggered when:
            # 1. A new task is enqueued
            # 2. A auto-cron to poll tasks is triggered
            if state.get("event") == "submit":
                return "check_should_submit"
            elif state.get("event") == "poll":
                result = [
                    *[
                        Send("poll_batch", batch_val)
                        for batch_val in state["in_flight"]
                    ],
                    "check_should_submit",
                ]
                return result
            else:
                raise ValueError("Invalid event")

        async def create_batch(state: State) -> dict:
            tasks = state.get("tasks", [])
            task_values = [task["task"] for task in tasks]
            batch_payload = await submit(task_values)
            # Start the poller
            batch_id = str(uuid.uuid4())
            configurable = get_config()[CONF]
            assistant_id = configurable["assistant_id"]
            if USE_CRONS:
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
            else:
                await _langgraph_client.runs.create(
                    assistant_id=assistant_id,
                    input={
                        "event": "poll",
                        "in_flight": InFlightBatch(
                            batch_id=batch_id,
                            batch_payload=batch_payload,
                            origins=[task["origin"] for task in tasks],
                        ),
                    },
                    multitask_strategy="reject",
                    after_seconds=30,
                )
            return {
                "last_submit_time": datetime.datetime.now(datetime.timezone.utc),
                "tasks": "__clear__",
                "in_flight": [],
            }

        async def check_should_submit(state: State):
            last_submit_time = state.get("last_submit_time") or datetime.datetime.now(
                datetime.timezone.utc
            )
            if state.get("tasks"):
                resp = await should_submit(state["tasks"], last_submit_time)
                goto = "create_batch" if resp else "__end__"
            else:
                goto = "__end__"
            return Command(update={"last_submit_time": last_submit_time}, goto=goto)

        async def poll_batch(state: InFlightBatch) -> dict:
            try:
                poll_result = await poll(state["batch_payload"])
            except BatchIngestException as e:
                poll_result = e

            if poll_result is not None:
                # Now we need to re-trigger ALL the original tasks
                to_resume = defaultdict(dict)
                if isinstance(poll_result, Exception):
                    detail = (
                        poll_result.detail
                        if isinstance(poll_result, BatchIngestException)
                        else str(poll_result)
                    )
                    response = {
                        "__batch_bridge__": {
                            "kind": "exception",
                            "detail": detail,
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
                if USE_CRONS:
                    configurable = get_config()[CONF]
                    await _langgraph_client.crons.delete(configurable["cron_id"])
                return {
                    "in_flight": RemoveBatch(state["batch_id"]),
                }

        compiled: CompiledBridge[typing.Any, typing.Any] = CompiledBridge(
            **(
                StateGraph(State, input=InputSchema)
                .add_node(poll_batch)
                .add_node(create_batch)
                .add_node(check_should_submit)
                .add_conditional_edges(
                    "__start__", route_entry, ["poll_batch", "check_should_submit"]
                )
                .compile(name=graph_id)
                .__dict__
            ),
            graph_id=graph_id,
            __output_coercer__=__output_coercer__,
        )

        register_graph_sync(graph_id, compiled)

        return compiled

    def __init__(
        self,
        submit: typing.Callable[[list[T]], typing.Awaitable[U]],
        poll: typing.Callable[[U], typing.Awaitable[V]],
        *,
        should_submit: typing.Optional[
            typing.Callable[[list[T], typing.Optional[datetime.datetime]], bool]
        ] = None,
        graph_id: str = "BatchBridge",
        # job_ttl: typing.Optional[datetime.timedelta] = MISSING, # type: ignore
    ) -> None:
        """This is cool."""
        ...


async def wait(
    item: T,
    *,
    bridge_id: str,  # Graph ID (~ assistant_id) of the graph in your deployment.
    thread_id: str = DEFAULT_THREAD_ID,
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
            thread_id=thread_id,
            assistant_id=bridge_id,
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


async def _submit_after_minute(tasks: list, last_submit: datetime.datetime) -> bool:
    if not tasks:
        return False
    # Main goal here is to avoid getting rate limited
    return (
        datetime.datetime.now(datetime.timezone.utc) - last_submit
    ).total_seconds() > 60
