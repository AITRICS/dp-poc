from abc import ABC, abstractmethod
from typing import TypeVar, Generic
from .events import EventBase

E = TypeVar('E', bound=EventBase)

class PublisherPort(ABC, Generic[E]):
    """
    An abstract port for a message publisher.
    It defines the interface for publishing events to a topic.
    """
    @abstractmethod
    async def publish(self, topic: str, event: E):
        """Publishes an event to the specified topic."""
        raise NotImplementedError