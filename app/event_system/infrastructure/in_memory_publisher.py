import logging
from typing import Generic, TypeVar

from app.event_system.domain.broker_port import BrokerPort
from app.event_system.domain.events import EventBase
from app.event_system.domain.publisher_port import PublisherPort

E = TypeVar("E", bound=EventBase)


class InMemoryPublisher(PublisherPort[E], Generic[E]):
    """
    An in-memory, asyncio-based implementation of the PublisherPort.
    It uses an InMemoryBroker to publish events.
    """

    def __init__(self, broker: BrokerPort[E]):
        self.broker = broker

    async def publish(self, topic: str, event: E) -> None:
        assert topic == event.meta.topic
        queue = await self.broker.get_queue(topic)
        await queue.put(event)
        logging.info(f"Published to {topic}: {event.__class__.__name__}(id={event.meta.event_id})")

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
        await self.broker.publish_pattern(topic_pattern, event)
        logging.info(
            f"Published to pattern {topic_pattern}: "
            f"{event.__class__.__name__}(id={event.meta.event_id})"
        )
