import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from typing import Generic, TypeVar

from app.event_system.domain.consumer_port import ConsumerPort
from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.utils.topic_matcher import get_matching_topics

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
            logging.info(
                f"Consumed from {topic}: {event.__class__.__name__}(id={event.meta.event_id})"
            )

            # When CompletedEvent is received, end the stream
            if isinstance(event, CompletedEvent):
                logging.info("Completed event received, ending stream")
                queue.task_done()
                break

            # 이벤트를 yield하여 스트림으로 처리
            yield event
            # 소비자가 이벤트 처리를 완료했으므로 task_done 호출
            queue.task_done()

    async def consume_pattern(self, topic_pattern: str) -> AsyncGenerator[E, None]:
        """
        Consumes events from all topics matching the pattern as an async generator.

        Dynamically detects new topics that match the pattern and starts consuming from them.

        Supports wildcards:
        - * matches any single level (e.g., data.* matches data.pipeline)
        - ** matches multiple levels (e.g., data.** matches data.pipeline.ingestion)

        Args:
            topic_pattern: Topic pattern with wildcards.

        Yields:
            Events from all matching topics.
        """
        # Get matching topics
        matching_topics = get_matching_topics(topic_pattern, self.broker.get_topics())

        if not matching_topics:
            logging.warning(
                f"No topics match pattern '{topic_pattern}'. "
                f"Available topics: {list(self.broker.get_topics())}"
            )
            return

        logging.info(
            f"Consuming from {len(matching_topics)} topics matching pattern "
            f"'{topic_pattern}': {matching_topics}"
        )

        # Create a merged queue for all matching topics
        merged_queue: asyncio.Queue[tuple[str, E]] = asyncio.Queue()
        active_topics = len(matching_topics)
        tasks: dict[str, asyncio.Task[None]] = {}
        is_running = True

        async def consume_topic(topic: str) -> None:
            """Consume from a single topic and put events into the merged queue."""
            queue = await self.broker.get_queue(topic)

            while is_running:
                try:
                    event = await queue.get()
                    logging.debug(
                        f"Consumed from {topic}: {event.__class__.__name__}(id={event.meta.event_id})"
                    )

                    # Put event with its source topic into merged queue
                    await merged_queue.put((topic, event))
                    queue.task_done()

                    # When CompletedEvent is received, end this topic's stream
                    if isinstance(event, CompletedEvent):
                        logging.debug(f"Completed event received from {topic}")
                        break
                except asyncio.CancelledError:
                    break

        # Start consuming from all matching topics
        for topic in matching_topics:
            tasks[topic] = asyncio.create_task(consume_topic(topic))

        try:
            # Yield events from the merged queue
            completed_topics = 0

            # Continue until all topics are completed
            while completed_topics < active_topics:
                source_topic, event = await merged_queue.get()

                # Check if this is a CompletedEvent
                if isinstance(event, CompletedEvent):
                    completed_topics += 1
                    merged_queue.task_done()
                    logging.info(
                        f"Topic {source_topic} completed "
                        f"({completed_topics}/{active_topics} topics done)"
                    )
                    continue

                # Yield non-completed events
                yield event
                merged_queue.task_done()

            logging.info(
                f"All {active_topics} topics matching pattern '{topic_pattern}' have completed"
            )

        finally:
            is_running = False

            # Ensure all tasks are properly cleaned up
            for task in tasks.values():
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
