"""
Event-based trigger implementation.
Triggers events when specific events are received from the event system.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Callable
from typing import Generic, TypeVar

from app.event_system.domain.consumer_port import ConsumerPort
from app.event_system.domain.events import EventBase
from app.scheduler.domain.trigger_port import TriggerPort

E = TypeVar("E", bound=EventBase)
SourceEvent = TypeVar("SourceEvent", bound=EventBase)


class EventTrigger(TriggerPort[E], Generic[E, SourceEvent]):
    """
    A trigger that fires when a specific event is received from the event system.

    Example:
        # Trigger when DataIngestionComplete event is received
        trigger = EventTrigger(
            consumer=consumer,
            source_topic="data.ingestion",
            event_factory=lambda source_event: ProcessingStartEvent(
                topic="data.processing",
                source_id=source_event.source_name
            ),
            filter_func=lambda event: event.rows_ingested > 1000
        )
    """

    def __init__(
        self,
        consumer: ConsumerPort[SourceEvent],
        source_topic: str,
        event_factory: Callable[[SourceEvent], E],
        filter_func: Callable[[SourceEvent], bool] | None = None,
    ) -> None:
        """
        Initialize an event-based trigger.

        Args:
            consumer: Consumer to listen for source events.
            source_topic: Topic or pattern to consume from.
            event_factory: Factory function that creates new events from source events.
            filter_func: Optional filter function. Only events passing this filter will trigger.
        """
        self.consumer = consumer
        self.source_topic = source_topic
        self.event_factory = event_factory
        self.filter_func = filter_func or (lambda _: True)
        self._running = False
        self._queue: asyncio.Queue[E] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._trigger_count = 0

    async def start(self) -> None:
        """Start the event trigger."""
        if self._running:
            logging.warning(f"EventTrigger for '{self.source_topic}' is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._consume_events())
        logging.info(f"EventTrigger started, listening to: {self.source_topic}")

    async def stop(self) -> None:
        """Stop the event trigger."""
        if not self._running:
            return

        self._running = False

        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logging.info(
            f"EventTrigger stopped for '{self.source_topic}' "
            f"(triggered {self._trigger_count} times)"
        )

    async def emit(self) -> AsyncGenerator[E, None]:
        """
        Emit events when source events are received.

        Yields:
            Events created from source events that pass the filter.
        """
        while self._running:
            with contextlib.suppress(asyncio.CancelledError):
                event = await self._queue.get()
                yield event

    def is_running(self) -> bool:
        """Check if the trigger is running."""
        return self._running

    def get_trigger_count(self) -> int:
        """
        Get the number of times this trigger has been fired.

        Returns:
            The trigger count.
        """
        return self._trigger_count

    async def _consume_events(self) -> None:
        """Internal coroutine that consumes source events and triggers new events."""
        try:
            # Check if source_topic contains wildcard patterns
            if "*" in self.source_topic:
                event_stream = self.consumer.consume_pattern(self.source_topic)
            else:
                event_stream = self.consumer.consume(self.source_topic)

            async for source_event in event_stream:
                if not self._running:
                    break

                # Apply filter
                try:
                    if not self.filter_func(source_event):
                        logging.debug(
                            f"Event filtered out by EventTrigger: {source_event.__class__.__name__}"
                        )
                        continue
                except Exception as e:
                    logging.error(f"Error in EventTrigger filter function: {e}")
                    continue

                # Create and queue the triggered event
                try:
                    triggered_event = self.event_factory(source_event)
                    await self._queue.put(triggered_event)
                    self._trigger_count += 1
                    logging.debug(
                        f"EventTrigger fired from source event: "
                        f"{source_event.__class__.__name__} -> "
                        f"{triggered_event.__class__.__name__}"
                    )
                except Exception as e:
                    logging.error(f"Error creating event in EventTrigger: {e}")

        except Exception as e:
            logging.error(f"Error in EventTrigger consumer loop: {e}")
