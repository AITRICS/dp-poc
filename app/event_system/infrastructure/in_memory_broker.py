import logging
from collections.abc import Callable
from typing import Generic, TypeVar

from app.event_system.domain.broker_port import BrokerPort
from app.event_system.domain.events import EventBase
from app.event_system.domain.queue_port import QueuePort
from app.event_system.utils.topic_matcher import get_matching_topics

from .in_memory_queue import InMemoryQueue

E = TypeVar("E", bound=EventBase)


class InMemoryBroker(BrokerPort[E], Generic[E]):
    """
    An in-memory, asyncio-based implementation of the BrokerPort.
    It uses InMemoryQueue to manage topics.
    """

    def __init__(
        self,
        queue_factory: Callable[[], QueuePort[E]] = InMemoryQueue[E],
    ) -> None:
        """
        Initialize the broker.

        Args:
            queue_factory: Factory function to create queue instances.
                          Defaults to InMemoryQueue.
        """
        self._queue_factory = queue_factory
        self.queues: dict[str, QueuePort[E]] = {}

    async def get_queue(self, topic: str) -> QueuePort[E]:
        """
        Get or create a queue for the given topic.

        Args:
            topic: The topic name.

        Returns:
            The queue for the topic.
        """
        return await self.declare_topic(topic)

    async def declare_topic(self, topic: str) -> QueuePort[E]:
        """
        Declares a topic, creating it if it doesn't exist.

        This operation is idempotent - if the topic already exists,
        it returns the existing queue without any changes.

        Args:
            topic: The topic name to declare.

        Returns:
            The queue for the topic.
        """
        if topic in self.queues:
            logging.debug(f"Topic '{topic}' already exists, returning existing queue")
        else:
            logging.info(f"Declaring new topic: '{topic}'")
            self.queues[topic] = self._queue_factory()
        return self.queues[topic]

    async def close(self) -> None:
        """
        Close the broker and wait for all queues to be processed.
        """
        for queue in self.queues.values():
            await queue.join()

    async def publish(self, topic: str, event: E) -> None:
        """
        Publish an event to the given topic.

        Args:
            topic: The topic name.
            event: The event to publish.
        """
        queue = await self.get_queue(topic)
        await queue.put(event)

    async def publish_pattern(self, topic_pattern: str, event: E) -> None:
        """
        Publish an event to all topics matching the pattern.

        Supports wildcards:
        - * matches any single level (e.g., data.* matches data.pipeline)
        - ** matches multiple levels (e.g., data.** matches data.pipeline.ingestion)

        Args:
            topic_pattern: Topic pattern with wildcards.
            event: Event to publish.
        """
        matching_topics = get_matching_topics(topic_pattern, set(self.queues.keys()))

        if not matching_topics:
            logging.warning(
                f"No topics match pattern '{topic_pattern}'. "
                f"Available topics: {list(self.queues.keys())}"
            )
            return

        logging.info(
            f"Publishing to {len(matching_topics)} topics matching pattern '{topic_pattern}': "
            f"{matching_topics}"
        )

        for topic in matching_topics:
            queue = await self.get_queue(topic)
            await queue.put(event)

    def get_topics(self) -> set[str]:
        """
        Returns all available topic names.

        Returns:
            Set of topic names.
        """
        return set(self.queues.keys())
