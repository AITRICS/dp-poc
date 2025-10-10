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

    @abstractmethod
    async def publish_pattern(self, topic_pattern: str, event: E) -> None:
        """
        Publishes an event to all topics matching the pattern.

        Supports wildcards:
        - * matches any single level (e.g., data.* matches data.pipeline)
        - ** matches multiple levels (e.g., data.** matches data.pipeline.ingestion)

        Args:
            topic_pattern: Topic pattern with wildcards.
            event: Event to publish.
        """
        raise NotImplementedError
