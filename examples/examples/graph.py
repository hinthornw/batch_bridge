from examples.batch_bridge import patch_openai
from openai import AsyncOpenAI
from langgraph.graph import StateGraph
from typing_extensions import Annotated, TypedDict

# This MUST be defined at the global level if you want
# it to still work through server revisions / restarts / etc.
client = patch_openai(AsyncOpenAI())


class State(TypedDict):
    messages: Annotated[list[dict], lambda x, y: x + y]


async def my_model(state: State):
    result = await client.chat.completions.create(
        model="gpt-4o-mini", messages=state["messages"]
    )
    return {"messages": [result]}


graph = StateGraph(State).add_node(my_model).add_edge("__start__", "my_model").compile()
