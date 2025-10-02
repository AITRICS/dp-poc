from typing import TypeVar, Generic

from app.event_system.domain.events import EventBase
from app.event_system.domain.consumer_port import ConsumerPort
from .in_memory_broker import InMemoryBroker

E = TypeVar('E', bound=EventBase)

class InMemoryConsumer(ConsumerPort[E], Generic[E]):
    """
    An in-memory, asyncio-based implementation of the ConsumerPort.
    It uses an InMemoryBroker to consume events.
    """
    def __init__(self, broker: InMemoryBroker[E]):
        self.broker = broker

    async def consume(self, topic: str):
        queue = await self.broker.get_queue(topic)
        while True:
            event = await queue.get()
            print(f"Consumed from {topic}: {event.__class__.__name__}(id={event.event_id}, content={event})")
            queue.task_done()