from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from .events import EventBase

E = TypeVar("E", bound=EventBase)


class PublisherPort(ABC, Generic[E]):
    """
    An abstract port for a message publisher.
    It defines the interface for publishing events to a topic.
    """

    @abstractmethod
    async def publish(self, topic: str, event: E) -> None:
        """Publishes an event to the specified topic."""
        raise NotImplementedError
