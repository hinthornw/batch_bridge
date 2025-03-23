# BatchBridge

BatchBridge is a library for efficient batch processing with LangGraph. It provides a mechanism to collect items, process them in batches, and handle results asynchronously using LangGraph's interrupt semantics.

## Key Features

- 🚀 **Efficient Batch Processing**: Collect items and process them in batches to improve efficiency and reduce costs
- ⏱️ **Configurable Flush Criteria**: Define when batches should be processed based on size, time, or custom logic
- 🔄 **Asynchronous Processing**: Seamlessly integrate with LangGraph's interrupt mechanism for non-blocking batch processing
- 🔌 **Easy Integration**: Works with any batch processing API, including OpenAI's Batch API

## Installation

```bash
pip install -e .
```

## Basic Usage

Here's a simple example of how to use BatchBridge:

```python
from datetime import datetime, timedelta
from batch_bridge import Batcher

# Define functions for batch processing
def submit_batch(items):
    """Submit a batch of items for processing."""
    # In a real implementation, this would submit to an external API
    # and return a batch ID
    print(f"Submitting batch of {len(items)} items")
    return "batch_123"

def poll_batch(batch_id):
    """Poll for the results of a batch."""
    # In a real implementation, this would check the status of the batch
    # and return results when available
    import time
    time.sleep(2)  # Simulate processing time
    return [f"Processed: {item}" for item in ["item1", "item2"]]

# Create a batcher with default flush criteria
batcher = Batcher(
    submit_func=submit_batch,
    poll_func=poll_batch,
    max_batch_size=50000,
    max_batch_age=timedelta(minutes=30)
)

# Submit an item for batch processing
result = batcher.submit("item1")
print(result)  # Will print "Processed: item1" after batch is processed
```

## Integration with LangGraph

BatchBridge integrates seamlessly with LangGraph's interrupt mechanism for non-blocking batch processing:

```python
from langgraph.graph import StateGraph, START, END
from batch_bridge import Batcher

# Create a batcher
batcher = Batcher(submit_func=submit_batch, poll_func=poll_batch)

# Define a node that uses the batcher
def process_with_batch(state):
    item = state["item"]
    # This will interrupt the graph until the result is available
    result = batcher.submit(item)
    return {"result": result}

# Build the graph
builder = StateGraph(dict)
builder.add_node("process", process_with_batch)
builder.add_edge(START, "process")
builder.add_edge("process", END)
graph = builder.compile()

# Run the graph
result = graph.invoke({"item": "test_item"})
```

## Advanced Configuration

BatchBridge provides flexible configuration options:

```python
from datetime import datetime, timedelta
from batch_bridge import Batcher

# Custom flush criteria
def should_flush(items, last_flushed):
    # Flush if we have at least 100 items or if it's been more than 5 minutes
    return len(items) >= 100 or (datetime.now() - last_flushed) > timedelta(minutes=5)

# Create a batcher with custom configuration
batcher = Batcher(
    submit_func=submit_batch,
    poll_func=poll_batch,
    should_flush_func=should_flush,
    name="my_custom_batcher",
    poll_interval=30  # Poll every 30 seconds
)
```

## Integration with OpenAI's Batch API

BatchBridge works well with OpenAI's Batch API:

```python
import io
import json
from typing import Any, List, TypedDict

from openai import AsyncOpenAI
from batch_bridge import Bridge, wait

# Initialize AsyncOpenAI client
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
    """Submits a batch of tasks to OpenAI using an in-memory JSONL file."""
    # Create in-memory JSONL string from tasks
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
    
    # Convert to bytes and create in-memory file
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
        metadata={"description": "batch job example"},
    )

    return batch_object.id


async def poll_openai(batch_id: str) -> Any:
    """Polls a batch job and, if complete, retrieves and parses the results."""
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


# Create a bridge for OpenAI
bridge = Bridge(
    submit=submit_openai,
    poll=poll_openai,
)


# Integration with LangGraph
from langgraph.graph import StateGraph
from typing_extensions import Annotated, TypedDict


def add_messages(old, new):
    if not isinstance(new, list):
        new = [new]
    return (old or []) + new


class State(TypedDict):
    messages: Annotated[list[dict], add_messages]


async def my_model(state: State):
    # This will interrupt the graph until the result is available
    result = await wait(state["messages"], batcher="bridge")
    return {"messages": [result]}


# Build the graph
graph = StateGraph(State).add_node(my_model).add_edge("__start__", "my_model").compile()
```

## Examples

For more examples, see the `examples.py` file in the package.

## License

MIT