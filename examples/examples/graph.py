import io
import json
from typing import Any, List, TypedDict

from examples.batch_bridge import Bridge, wait
from openai import AsyncOpenAI

client = AsyncOpenAI()


class TaskBody(TypedDict):
    model: str
    temperature: float
    response_format: dict[str, str]
    messages: List[dict[str, str]]


class Task(TypedDict):
    custom_id: str
    method: str
    url: str
    body: TaskBody


async def submit_openai(tasks: list[list[dict[str, str]]]) -> dict:
    """
    Submits a batch of tasks to OpenAI using an in-memory JSONL file.

    Args:
        tasks (List[Task]): List of tasks to be submitted.

    Returns:
        dict: The created batch object from the API.
    """
    # Create in-memory JSONL string from tasks
    # Task is a list of messages
    jsonl_str = "\n".join(
        json.dumps(
            Task(
                custom_id=str(i),
                method="POST",
                url="/v1/chat/completions",
                body={"model": "gpt-3.5-turbo-0125", "messages": task},
            )
        )
        for i, task in enumerate(tasks)
    )
    import langsmith as ls

    print("TASKS", tasks)
    print("jsonl_str", jsonl_str)
    with ls.trace("submit_openai", inputs={"tasks": tasks, "jsonl_str": jsonl_str}):
        pass
    jsonl_bytes = jsonl_str.encode("utf-8")
    in_memory_file = io.BytesIO(jsonl_bytes)

    # Upload the in-memory file for batch processing
    batch_input_file = await client.files.create(file=in_memory_file, purpose="batch")
    batch_input_file_id = batch_input_file.id

    # Create the batch job with a 24h completion window
    batch_object = await client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "nightly eval job"},
    )

    return batch_object.id


async def poll_openai(batch_id: str) -> Any:
    """
    Polls a batch job and, if complete, retrieves and parses the results.

    Args:
        batch_id (str): The ID of the batch job to poll.

    Returns:
        List[Any] or None: The list of parsed results if the job is complete; otherwise, None.
    """
    batch_obj = await client.batches.retrieve(batch_id)
    if batch_obj.status == "failed":
        return ValueError(f"Batch {batch_id} failed")
    if batch_obj.status != "completed":
        return None

    result_file_id = batch_obj.output_file_id
    result_content = (await client.files.content(result_file_id)).content

    # Parse JSONL content from the result file into a list of objects
    results = []
    for line in result_content.decode("utf-8").splitlines():
        if line.strip():
            data = json.loads(line)
            if (response := data.get("response")) and (body := response.get("body")):
                results.append(body)
            else:
                results.append(data)
    return results


bridge = Bridge(
    submit=submit_openai,
    poll=poll_openai,
)


# Agent

from langgraph.graph import StateGraph
from typing_extensions import Annotated, TypedDict


def add_messages(old, new):
    if not isinstance(new, list):
        new = [new]
    return (old or []) + new


class State(TypedDict):
    messages: Annotated[list[dict], add_messages]


async def my_model(state: State):
    result = await wait(state["messages"], batcher="bridge")
    return {"messages": [result]}


graph = StateGraph(State).add_node(my_model).add_edge("__start__", "my_model").compile()
