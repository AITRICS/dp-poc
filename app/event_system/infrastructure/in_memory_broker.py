import asyncio
from collections import defaultdict
from typing import TypeVar, Generic

from app.event_system.domain.events import EventBase
from app.event_system.domain.broker_port import BrokerPort

E = TypeVar('E', bound=EventBase)

class InMemoryBroker(BrokerPort[E]):
    """
    An in-memory, asyncio-based implementation of the BrokerPort.
    It uses a dictionary of asyncio.Queue to manage topics.
    """
    def __init__(self):
        self.queues: defaultdict[str, asyncio.Queue[E]] = defaultdict(asyncio.Queue)

    async def get_queue(self, topic: str) -> asyncio.Queue[E]:
        return self.queues[topic]