"""
Trigger port definitions for the scheduler system.
Defines the interface for different types of triggers.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Generic, TypeVar

from app.event_system.domain.events import EventBase

E = TypeVar("E", bound=EventBase)


class TriggerPort(ABC, Generic[E]):
    """
    Abstract port for triggers that emit events.
    A trigger monitors for conditions and emits events when those conditions are met.
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Start the trigger.
        Initialize any resources needed for the trigger to function.
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the trigger and clean up resources.
        """
        raise NotImplementedError

    @abstractmethod
    def emit(self) -> AsyncGenerator[E, None]:
        """
        Emit events as they are triggered.

        Yields:
            Events when trigger conditions are met.
        """
        raise NotImplementedError

    @abstractmethod
    def is_running(self) -> bool:
        """
        Check if the trigger is currently running.

        Returns:
            True if running, False otherwise.
        """
        raise NotImplementedError
