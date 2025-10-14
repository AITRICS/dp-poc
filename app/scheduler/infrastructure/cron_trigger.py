"""
Cron-based trigger implementation.
Triggers events based on crontab schedule.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import Generic, TypeVar

from croniter import croniter  # type: ignore[import-untyped]

from app.event_system.domain.events import EventBase
from app.scheduler.domain.trigger_port import TriggerPort

E = TypeVar("E", bound=EventBase)


class CronTrigger(TriggerPort[E], Generic[E]):
    """
    A trigger that fires based on a cron schedule.

    Example:
        # Fire every minute
        trigger = CronTrigger("* * * * *", event_factory)

        # Fire every day at 3:30 AM
        trigger = CronTrigger("30 3 * * *", event_factory)
    """

    def __init__(
        self,
        cron_expression: str,
        event_factory: Callable[[], E],
        timezone: str | None = None,
    ) -> None:
        """
        Initialize a cron-based trigger.

        Args:
            cron_expression: Cron expression (e.g., "*/5 * * * *" for every 5 minutes).
            event_factory: Factory function that creates event instances.
            timezone: Timezone for cron schedule (e.g., "UTC", "Asia/Seoul").
        """
        self.cron_expression = cron_expression
        self.event_factory = event_factory
        self.timezone = timezone
        self._running = False
        self._stop_event: asyncio.Event | None = None
        self._queue: asyncio.Queue[E] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

        # Validate cron expression
        # Support both 5-field and 6-field (with seconds) cron expressions
        try:
            croniter(cron_expression, second_at_beginning=True)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{cron_expression}': {e}") from e

    async def start(self) -> None:
        """Start the cron trigger."""
        if self._running:
            logging.warning(f"CronTrigger '{self.cron_expression}' is already running")
            return

        self._running = True
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_cron())
        logging.info(f"CronTrigger started with schedule: {self.cron_expression}")

    async def stop(self) -> None:
        """Stop the cron trigger."""
        if not self._running:
            return

        self._running = False
        if self._stop_event:
            self._stop_event.set()

        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        logging.info(f"CronTrigger stopped: {self.cron_expression}")

    async def emit(self) -> AsyncGenerator[E, None]:
        """
        Emit events according to cron schedule.

        Yields:
            Events when cron schedule triggers.
        """
        while self._running:
            try:
                event = await self._queue.get()
                yield event
            except asyncio.CancelledError:
                # Gracefully handle cancellation
                break

    def is_running(self) -> bool:
        """Check if the trigger is running."""
        return self._running

    async def _run_cron(self) -> None:
        """Internal coroutine that monitors cron schedule and emits events."""
        # Support both 5-field and 6-field (with seconds) cron expressions
        cron = croniter(self.cron_expression, datetime.now(), second_at_beginning=True)

        while self._running:
            # Get next scheduled time
            next_run = cron.get_next(datetime)
            now = datetime.now()
            wait_seconds = (next_run - now).total_seconds()

            if wait_seconds > 0:
                with contextlib.suppress(TimeoutError):
                    # Wait until next scheduled time or stop signal
                    await asyncio.wait_for(
                        (
                            self._stop_event.wait()
                            if self._stop_event
                            else asyncio.sleep(wait_seconds)
                        ),
                        timeout=wait_seconds,
                    )

            if not self._running:
                break

            # Create and queue the event
            try:
                event = self.event_factory()
                await self._queue.put(event)
                logging.debug(f"CronTrigger '{self.cron_expression}' fired at {datetime.now()}")
            except Exception as e:
                logging.error(f"Error creating event in CronTrigger: {e}")
