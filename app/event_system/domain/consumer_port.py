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

    @abstractmethod
    def consume_pattern(self, topic_pattern: str) -> AsyncGenerator[E, None]:
        """
        Consumes events from all topics matching the pattern as an async generator.

        Supports wildcards:
        - * matches any single level (e.g., data.* matches data.pipeline)
        - ** matches multiple levels (e.g., data.** matches data.pipeline.ingestion)

        Args:
            topic_pattern: Topic pattern with wildcards.

        Yields:
            Events from all matching topics.
        """
        raise NotImplementedError
