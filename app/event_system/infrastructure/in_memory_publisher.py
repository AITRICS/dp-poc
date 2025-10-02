from typing import Generic, TypeVar

from app.event_system.domain.events import EventBase
from app.event_system.domain.publisher_port import PublisherPort

from .in_memory_broker import InMemoryBroker

E = TypeVar("E", bound=EventBase)


class InMemoryPublisher(PublisherPort[E], Generic[E]):
    """
    An in-memory, asyncio-based implementation of the PublisherPort.
    It uses an InMemoryBroker to publish events.
    """

    def __init__(self, broker: InMemoryBroker[E]):
        self.broker = broker

    async def publish(self, topic: str, event: E) -> None:
        queue = await self.broker.get_queue(topic)
        await queue.put(event)
        print(f"Published to {topic}: {event.__class__.__name__}(id={event.event_id})")
