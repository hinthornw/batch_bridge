import typing

from typing_extensions import TypedDict

T = typing.TypeVar("T")
U = typing.TypeVar("U")
V = typing.TypeVar("V")


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
