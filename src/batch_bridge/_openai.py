import io
import json
import typing

from openai import AsyncOpenAI
from typing_extensions import TypedDict

from examples.batch_bridge.errors import BatchIngestException
from batch_bridge._base import Bridge, CompiledBridge
import functools

if typing.TYPE_CHECKING:
    from openai import AsyncOpenAI
    from openai.types import ChatModel
    from openai.types.chat import (
        ChatCompletion,
        ChatCompletionAudioParam,
        ChatCompletionMessageParam,
        ChatCompletionModality,
        ChatCompletionPredictionContentParam,
        ChatCompletionReasoningEffort,
        ChatCompletionToolChoiceOptionParam,
        ChatCompletionToolParam,
    )
    from openai.types.chat.completion_create_params import (
        ResponseFormat,
    )

    CompletionOutputs = ChatCompletion
else:
    CompletionOutputs = typing.Any


class CompletionInputs(TypedDict, total=False):
    messages: typing.Required[typing.Sequence["ChatCompletionMessageParam"]]
    model: typing.Required[typing.Union[str, "ChatModel"]]
    audio: typing.Optional["ChatCompletionAudioParam"]
    frequency_penalty: typing.Optional[float]
    logit_bias: typing.Optional[dict[str, int]]
    logprobs: typing.Optional[bool]
    max_completion_tokens: typing.Optional[int]
    max_tokens: typing.Optional[int]
    metadata: typing.Optional[dict[str, str]]
    modalities: typing.Optional[list["ChatCompletionModality"]]
    n: typing.Optional[int]
    parallel_tool_calls: bool
    prediction: typing.Optional["ChatCompletionPredictionContentParam"]
    presence_penalty: typing.Optional[float]
    reasoning_effort: "ChatCompletionReasoningEffort"
    response_format: "ResponseFormat"
    seed: typing.Optional[int]
    service_tier: typing.Optional[typing.Literal["auto", "default"]]
    stop: typing.Union[typing.Optional[str], list[str]]
    store: typing.Optional[bool]
    temperature: typing.Optional[float]
    tool_choice: "ChatCompletionToolChoiceOptionParam"
    tools: typing.Sequence["ChatCompletionToolParam"]
    top_logprobs: typing.Optional[int]
    top_p: typing.Optional[float]
    user: str


class Task(TypedDict, total=False):
    custom_id: str
    method: str
    url: str
    body: CompletionInputs


def patch_openai(client: typing.Optional["AsyncOpenAI"] = None) -> AsyncOpenAI:
    from openai import AsyncOpenAI
    from openai._types import NotGiven

    if client is None:
        client = AsyncOpenAI()
    bridge = OpenAIBridge(client)

    @functools.wraps(client.chat.completions.create)
    async def async_openai_completions(
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        to_send = {k: v for k, v in kwargs.items() if not isinstance(v, NotGiven)}
        return await bridge.wait(CompletionInputs(**to_send))

    client.chat.completions.create = async_openai_completions

    return client


def OpenAIBridge(
    client: typing.Optional["AsyncOpenAI"] = None,
    *,
    graph_id: str = "BatchBridge",
) -> CompiledBridge[CompletionInputs, CompletionOutputs]:
    handler = OpenAIHandler(client)
    return Bridge(
        handler.submit,
        handler.poll,
        graph_id=graph_id,
        __output_coercer__=handler.coerce_response,
    )


class OpenAIHandler:
    def __init__(self, client: AsyncOpenAI):
        from openai import AsyncOpenAI
        from openai.types.chat import ChatCompletion

        if client is None:
            client = AsyncOpenAI()
        self.client = client
        self.coerce_response = lambda response: ChatCompletion(**response)

    async def submit(self, tasks: list[list[CompletionInputs]]) -> dict:
        """
        Submits a batch of tasks to OpenAI using an in-memory JSONL file.

        Args:
            tasks (list[list[dict[str, str]]]): list of tasks to be submitted.

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
                    body=task,
                )
            )
            for i, task in enumerate(tasks)
        )
        jsonl_bytes = jsonl_str.encode("utf-8")
        in_memory_file = io.BytesIO(jsonl_bytes)

        # Upload the in-memory file for batch processing
        batch_input_file = await self.client.files.create(
            file=in_memory_file, purpose="batch"
        )
        batch_input_file_id = batch_input_file.id

        # Create the batch job with a 24h completion window
        batch_object = await self.client.batches.create(
            input_file_id=batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": "nightly eval job"},
        )

        return batch_object.id

    async def poll(self, batch_id: str) -> typing.Optional[list[dict[str, str]]]:
        """
            Polls a batch job and, if complete, retrieves and parses the results.

            Args:
            batch_id (str): The ID of the batch job to poll.

        Returns:
            list[dict[str, str]] or None: The list of parsed results if the job is complete; otherwise, None.
        """
        batch_obj = await self.client.batches.retrieve(batch_id)
        if batch_obj.status == "failed":
            raise BatchIngestException(f"Batch {batch_id} failed")
        if batch_obj.status != "completed":
            return None

        result_file_id = batch_obj.output_file_id
        result_content = (await self.client.files.content(result_file_id)).content

        # Parse JSONL content from the result file into a list of objects
        results = []
        for line in result_content.decode("utf-8").splitlines():
            if line.strip():
                data = json.loads(line)
                if (response := data.get("response")) and (
                    body := response.get("body")
                ):
                    results.append(body)
                else:
                    results.append(data)
        return results
