"""Queue port interface for event system."""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from .events import EventBase

E = TypeVar("E", bound=EventBase)


class QueuePort(ABC, Generic[E]):
    """
    An abstract port for a message queue.
    It defines the interface for queue operations.
    """

    @abstractmethod
    async def put(self, item: E) -> None:
        """Put an item into the queue."""
        raise NotImplementedError

    @abstractmethod
    async def get(self) -> E:
        """Get an item from the queue."""
        raise NotImplementedError

    @abstractmethod
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        raise NotImplementedError

    @abstractmethod
    def empty(self) -> bool:
        """Return True if the queue is empty."""
        raise NotImplementedError

    @abstractmethod
    def full(self) -> bool:
        """Return True if the queue is full."""
        raise NotImplementedError

    @abstractmethod
    def task_done(self) -> None:
        """
        Indicate that a formerly enqueued task is complete.
        Used by queue consumers.
        """
        raise NotImplementedError

    @abstractmethod
    async def join(self) -> None:
        """
        Block until all items in the queue have been gotten and processed.
        """
        raise NotImplementedError
