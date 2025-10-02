from abc import ABC, abstractmethod
from typing import TypeVar, Generic
from .events import EventBase

E = TypeVar('E', bound=EventBase)

class ConsumerPort(ABC, Generic[E]):
    """
    An abstract port for a message consumer.
    It defines the interface for consuming events from a topic.
    """
    @abstractmethod
    async def consume(self, topic: str):
        """Consumes events from the specified topic."""
        raise NotImplementedError