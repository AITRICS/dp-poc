"""
Scheduler implementation.
Manages multiple triggers and publishes their events to the event system.
"""

import asyncio
import contextlib
import logging
from typing import Generic, TypeVar

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.scheduler.domain.scheduler_port import SchedulerPort
from app.scheduler.domain.trigger_port import TriggerPort

E = TypeVar("E", bound=EventBase)


class Scheduler(SchedulerPort[E], Generic[E]):
    """
    A scheduler that manages multiple triggers and publishes their events.

    Example:
        scheduler = Scheduler(publisher)

        # Register triggers
        await scheduler.register_trigger("daily_report", cron_trigger, "reports.daily")
        await scheduler.register_trigger("api_webhook", api_trigger, "webhooks.incoming")

        # Start all triggers
        await scheduler.start()

        # Later, stop all triggers
        await scheduler.stop()
    """

    def __init__(self, publisher: InMemoryPublisher[E]) -> None:
        """
        Initialize the scheduler.

        Args:
            publisher: Publisher to use for publishing triggered events.
        """
        self.publisher = publisher
        self._triggers: dict[str, tuple[TriggerPort[E], str]] = {}
        self._running = False
        self._tasks: dict[str, asyncio.Task[None]] = {}

    async def register_trigger(self, trigger_id: str, trigger: TriggerPort[E], topic: str) -> None:
        """
        Register a trigger with the scheduler.

        Args:
            trigger_id: Unique identifier for this trigger.
            trigger: The trigger instance to register.
            topic: The topic to publish events to when this trigger fires.

        Raises:
            ValueError: If trigger_id already exists.
        """
        if trigger_id in self._triggers:
            raise ValueError(f"Trigger with id '{trigger_id}' already registered")

        self._triggers[trigger_id] = (trigger, topic)
        logging.info(f"Registered trigger '{trigger_id}' for topic '{topic}'")

        # If scheduler is already running, start this trigger immediately
        if self._running:
            await trigger.start()
            self._tasks[trigger_id] = asyncio.create_task(
                self._process_trigger(trigger_id, trigger, topic)
            )

    def is_running(self) -> bool:
        """
        Check if the scheduler is running.

        Returns:
            True if running, False otherwise.
        """
        return self._running

    async def unregister_trigger(self, trigger_id: str) -> None:
        """
        Unregister a trigger from the scheduler.

        Args:
            trigger_id: The ID of the trigger to unregister.

        Raises:
            KeyError: If trigger_id does not exist.
        """
        if trigger_id not in self._triggers:
            raise KeyError(f"Trigger with id '{trigger_id}' not found")

        # Stop the trigger if it's running
        trigger, _ = self._triggers[trigger_id]
        if trigger.is_running():
            await trigger.stop()

        # Cancel the processing task
        if trigger_id in self._tasks:
            task = self._tasks[trigger_id]
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            del self._tasks[trigger_id]

        del self._triggers[trigger_id]
        logging.info(f"Unregistered trigger '{trigger_id}'")

    async def start(self) -> None:
        """Start the scheduler and all registered triggers."""
        if self._running:
            logging.warning("Scheduler is already running")
            return

        self._running = True
        logging.info(f"Starting scheduler with {len(self._triggers)} triggers...")

        # Start all triggers
        for trigger_id, (trigger, topic) in self._triggers.items():
            await trigger.start()
            self._tasks[trigger_id] = asyncio.create_task(
                self._process_trigger(trigger_id, trigger, topic)
            )

        logging.info("Scheduler started successfully")

    async def stop(self) -> None:
        """Stop the scheduler and all registered triggers."""
        if not self._running:
            return

        self._running = False
        logging.info("Stopping scheduler...")

        # Stop all triggers
        for trigger_id, (trigger, _) in self._triggers.items():
            if trigger.is_running():
                await trigger.stop()
                logging.info(f"Stopped trigger '{trigger_id}'")

        # Cancel all processing tasks
        for task in self._tasks.values():
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._tasks.clear()
        logging.info("Scheduler stopped")

    def get_registered_triggers(self) -> dict[str, tuple[TriggerPort[E], str]]:
        """
        Get all registered triggers.

        Returns:
            Dictionary mapping trigger_id to (trigger, topic) tuple.
        """
        return self._triggers.copy()

    async def _process_trigger(self, trigger_id: str, trigger: TriggerPort[E], topic: str) -> None:
        """
        Process events from a trigger and publish them.

        Args:
            trigger_id: ID of the trigger.
            trigger: The trigger instance.
            topic: Topic to publish events to.
        """
        try:
            async for event in trigger.emit():
                if not self._running:
                    break

                try:
                    await self.publisher.publish(topic, event)
                    logging.debug(
                        f"Published event from trigger '{trigger_id}' to topic '{topic}': "
                        f"{event.__class__.__name__}"
                    )
                except Exception as e:
                    logging.error(f"Error publishing event from trigger '{trigger_id}': {e}")

        except asyncio.CancelledError:
            logging.debug(f"Trigger processing cancelled for '{trigger_id}'")
            raise
        except Exception as e:
            logging.error(f"Error in trigger '{trigger_id}' processing: {e}")
            # Don't stop the scheduler, just log the error
