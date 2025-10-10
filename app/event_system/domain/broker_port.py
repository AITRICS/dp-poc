from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from app.event_system.domain.queue_port import QueuePort

from .events import EventBase

E = TypeVar("E", bound=EventBase)


class BrokerPort(ABC, Generic[E]):
    """
    An abstract port for a message broker.
    It defines the interface for getting a queue for a specific topic.
    """

    @abstractmethod
    async def get_queue(self, topic: str) -> QueuePort[E]:
        """Returns a queue object for the given topic."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Closes the broker."""
        raise NotImplementedError

    @abstractmethod
    async def publish(self, topic: str, event: E) -> None:
        """Publishes an event to the given topic."""
        raise NotImplementedError
