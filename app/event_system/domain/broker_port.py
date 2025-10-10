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

    @abstractmethod
    async def declare_topic(self, topic: str) -> QueuePort[E]:
        """
        Declares a topic, creating it if it doesn't exist.

        This operation is idempotent - if the topic already exists,
        it returns the existing queue without any changes.
        Similar to RabbitMQ's queue.declare or Kafka's topic creation.

        Args:
            topic: The topic name to declare.

        Returns:
            The queue for the topic.
        """
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

    @abstractmethod
    def get_topics(self) -> set[str]:
        """
        Returns all available topic names.

        Returns:
            Set of topic names.
        """
        raise NotImplementedError
