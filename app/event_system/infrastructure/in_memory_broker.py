from collections.abc import Callable
from typing import Generic, TypeVar

from app.event_system.domain.broker_port import BrokerPort
from app.event_system.domain.events import EventBase
from app.event_system.domain.queue_port import QueuePort

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
        if topic not in self.queues:
            print(f"Queue for topic {topic} does not exist so creating it")
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
