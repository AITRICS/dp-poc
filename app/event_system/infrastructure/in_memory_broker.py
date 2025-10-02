import asyncio
from collections import defaultdict
from typing import TypeVar, Generic

from app.event_system.domain.events import EventBase
from app.event_system.domain.broker_port import BrokerPort

E = TypeVar("E", bound=EventBase)


class InMemoryBroker(BrokerPort[E]):
    """
    An in-memory, asyncio-based implementation of the BrokerPort.
    It uses a dictionary of asyncio.Queue to manage topics.
    """

    def __init__(self):
        self.queues: defaultdict[str, asyncio.Queue[E]] = defaultdict(asyncio.Queue)

    async def get_queue(self, topic: str) -> asyncio.Queue[E]:
        if topic not in self.queues:
            print(f"Queue for topic {topic} does not exist so creating it")
        return self.queues[topic]

    async def close(self) -> None:
        for queue in self.queues.values():
            queue.join()

    async def create_queue(self, topic: str) -> None:
        if topic not in self.queues:
            raise ValueError(f"Queue for topic {topic} already exists")
        self.queues[topic] = asyncio.Queue()
        return self.queues[topic]
