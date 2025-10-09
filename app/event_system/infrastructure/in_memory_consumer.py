import logging
from typing import TypeVar, Generic
from collections.abc import AsyncGenerator

from app.event_system.domain.consumer_port import ConsumerPort
from app.event_system.domain.events import CompletedEvent, EventBase

from .in_memory_broker import InMemoryBroker

E = TypeVar("E", bound=EventBase)


class InMemoryConsumer(ConsumerPort[E], Generic[E]):
    """
    An in-memory, asyncio-based implementation of the ConsumerPort.
    It uses an InMemoryBroker to consume events as an async generator.
    """

    def __init__(self, broker: InMemoryBroker[E]) -> None:
        self.broker = broker

    async def consume(self, topic: str) -> AsyncGenerator[E, None]:
        """
        Consumes events from the specified topic as an async generator.
        Yields events one by one, allowing for flexible event processing.
        """
        queue = await self.broker.get_queue(topic)

        while True:
            event = await queue.get()
            logging.info(f"Consumed from {topic}: {event.__class__.__name__}(id={event.event_id}, content={event})")
            queue.task_done()
            
            # When CompletedEvent is received, end the stream
            if isinstance(event, CompletedEvent):
                logging.info("Completed event received, ending stream")
                queue.task_done()
                break

            # 이벤트를 yield하여 스트림으로 처리
            yield event
            queue.task_done()
