from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Generic, TypeVar

from .events import EventBase

E = TypeVar("E", bound=EventBase)


class ConsumerPort(ABC, Generic[E]):
    """
    An abstract port for a message consumer.
    It defines the interface for consuming events from a topic.
    """

    @abstractmethod
    def consume(self, topic: str) -> AsyncGenerator[E, None]:
        """Consumes events from the specified topic as an async generator."""
        raise NotImplementedError
