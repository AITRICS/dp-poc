"""
Manual trigger implementation.
Triggers events when explicitly called (e.g., from API endpoints, CLI, tests).
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from typing import Any, Generic, TypeVar

from app.event_system.domain.events import EventBase
from app.scheduler.domain.trigger_port import TriggerPort

E = TypeVar("E", bound=EventBase)


class ManualTrigger(TriggerPort[E], Generic[E]):
    """
    A trigger that fires when explicitly triggered via manual call.

    This is the most flexible trigger type - can be called from:
    - API endpoints (FastAPI, Flask, etc.)
    - CLI commands
    - Test code
    - Other application logic

    Example:
        trigger = ManualTrigger(event_factory)
        await trigger.start()

        # Later, when needed:
        await trigger.trigger({"user_id": 123, "action": "login"})
    """

    def __init__(
        self,
        event_factory: Callable[[dict[str, Any]], E],
    ) -> None:
        """
        Initialize a manual trigger.

        Args:
            event_factory: Factory function that creates event instances from payload.
                          Should accept a dict of parameters.
        """
        self.event_factory = event_factory
        self._running = False
        self._queue: asyncio.Queue[E] = asyncio.Queue()
        self._trigger_count = 0

    async def start(self) -> None:
        """Start the manual trigger."""
        if self._running:
            logging.warning("ManualTrigger is already running")
            return

        self._running = True
        logging.info("ManualTrigger started")

    async def stop(self) -> None:
        """Stop the manual trigger."""
        if not self._running:
            return

        self._running = False
        logging.info(f"ManualTrigger stopped (triggered {self._trigger_count} times)")

    async def emit(self) -> AsyncGenerator[E, None]:
        """
        Emit events when triggered manually.

        Yields:
            Events when trigger() is called.
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

    async def trigger(self, payload: dict[str, Any] | None = None) -> None:
        """
        Manually trigger the event emission.

        This method should be called from API endpoints, CLI, tests,
        or any other external source that needs to trigger events.

        Args:
            payload: Optional payload data to pass to the event factory.

        Raises:
            RuntimeError: If trigger is not running.
        """
        if not self._running:
            raise RuntimeError("Cannot trigger - ManualTrigger is not running")

        try:
            event = self.event_factory(payload or {})
            await self._queue.put(event)
            self._trigger_count += 1
            logging.info(f"ManualTrigger fired (count: {self._trigger_count})")
        except Exception as e:
            logging.error(f"Error creating event in ManualTrigger: {e}")
            raise

    def get_trigger_count(self) -> int:
        """
        Get the number of times this trigger has been fired.

        Returns:
            The trigger count.
        """
        return self._trigger_count
