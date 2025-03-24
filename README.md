# BatchBridge

BatchBridge is a library for efficient batch processing with LangGraph. It provides a mechanism to collect items, process them in batches, and handle results asynchronously using LangGraph's interrupt semantics.

## What is BatchBridge?

Batch APIs can cut AI inference costs by 50% or more, but they're difficult to use in agent workflows. They force you to manually aggregate requests and design your entire agent loop around batch processing rather than focusing on individual tasks. This makes your code more complex, harder to maintain, and less shareable.

BatchBridge solves this by making batch APIs work like standard completion APIs in LangGraph. Your code makes normal API calls while BatchBridge handles batching, submission, polling, and resumption behind the scenes. This lets you design and improve an agent using single completions, then make a one-line change to let it economically scale.

We aim to give you significant cost savings with minimal code complexity.

## Installation

```bash
pip install -e .
```

Since BatchBridge relies on LangGraph's durable execution and cron functionality, it must be
run on the LangGraph platform.


## Example with OpenAI's Batch API

BatchBridge has a native integration with OpenAI's Batch API:

```python
from batch_bridge import patch_openai
from openai import AsyncOpenAI
from langgraph.graph import StateGraph
from typing_extensions import Annotated, TypedDict

# Patch the client at the global level
client = patch_openai(AsyncOpenAI())


class State(TypedDict):
    messages: Annotated[list[dict], lambda x, y: x + y]


async def my_model(state: State):
    # This will:
    # 1. submit the message to our bridge graph
    # 2. Interrupt this agent graph.
    # 3. resume once the bridge graph detects that the batch is complete
    result = await client.chat.completions.create(
        model="gpt-4o-mini", messages=state["messages"]
    )
    return {"messages": [result]}


graph = StateGraph(State).add_node(my_model).add_edge("__start__", "my_model").compile()
```

## Basic Usage

Under the hood, BatchBridge relies on two basic functions:
a submit() function and a poll() function.

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
)
```

## License

MIT