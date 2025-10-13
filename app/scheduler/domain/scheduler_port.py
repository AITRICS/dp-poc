"""
Scheduler port definition.
Defines the interface for the scheduler that manages triggers and publishes events.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from app.event_system.domain.events import EventBase
from app.scheduler.domain.trigger_port import TriggerPort

E = TypeVar("E", bound=EventBase)


class SchedulerPort(ABC, Generic[E]):
    """
    Abstract port for a scheduler.
    Manages triggers and publishes events when triggers fire.
    """

    @abstractmethod
    async def register_trigger(self, trigger_id: str, trigger: TriggerPort[E], topic: str) -> None:
        """
        Register a trigger with the scheduler.

        Args:
            trigger_id: Unique identifier for this trigger.
            trigger: The trigger instance to register.
            topic: The topic to publish events to when this trigger fires.
        """
        raise NotImplementedError

    @abstractmethod
    async def unregister_trigger(self, trigger_id: str) -> None:
        """
        Unregister a trigger from the scheduler.

        Args:
            trigger_id: The ID of the trigger to unregister.
        """
        raise NotImplementedError

    @abstractmethod
    async def start(self) -> None:
        """
        Start the scheduler and all registered triggers.
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the scheduler and all registered triggers.
        """
        raise NotImplementedError

    @abstractmethod
    def get_registered_triggers(self) -> dict[str, tuple[TriggerPort[E], str]]:
        """
        Get all registered triggers.

        Returns:
            Dictionary mapping trigger_id to (trigger, topic) tuple.
        """
        raise NotImplementedError
