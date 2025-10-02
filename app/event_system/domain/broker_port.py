from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from .events import EventBase

E = TypeVar("E", bound=EventBase)


class BrokerPort(ABC, Generic[E]):
    """
    An abstract port for a message broker.
    It defines the interface for getting a queue for a specific topic.
    """

    @abstractmethod
    async def get_queue(self, topic: str) -> Any:
        """Returns a queue object for the given topic."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Closes the broker."""
        raise NotImplementedError

    @abstractmethod
    async def create_queue(self, topic: str) -> Any:
        """Creates a queue for the given topic."""
        raise NotImplementedError
