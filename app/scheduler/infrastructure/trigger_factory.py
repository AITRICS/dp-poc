"""
Factory for creating different types of triggers.
Provides a unified interface for trigger creation.
"""

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Any, Generic, TypeVar

from app.event_system.domain.consumer_port import ConsumerPort
from app.event_system.domain.events import EventBase
from app.scheduler.infrastructure.cron_trigger import CronTrigger
from app.scheduler.infrastructure.event_trigger import EventTrigger
from app.scheduler.infrastructure.manual_trigger import ManualTrigger

E = TypeVar("E", bound=EventBase)
SourceEvent = TypeVar("SourceEvent", bound=EventBase)


class TriggerType(StrEnum):
    """Enumeration of available trigger types."""

    CRON = auto()
    MANUAL = auto()
    EVENT = auto()


class TriggerFactory(Generic[E]):
    """
    Factory for creating trigger instances.

    Example:
        factory = TriggerFactory()

        # Create a cron trigger
        cron_trigger = factory.create_trigger(
            TriggerType.CRON,
            cron_expression="*/5 * * * *",
            event_factory=lambda: MyEvent(topic="scheduled")
        )

        # Create a manual trigger
        manual_trigger = factory.create_trigger(
            TriggerType.MANUAL,
            event_factory=lambda payload: MyEvent(topic="manual", **payload)
        )

        # Create an event trigger
        event_trigger = factory.create_trigger(
            TriggerType.EVENT,
            consumer=consumer,
            source_topic="data.ingestion",
            event_factory=lambda source: ProcessEvent(topic="processing")
        )
    """

    @staticmethod
    def create_trigger(
        trigger_type: TriggerType | str, **kwargs: Any
    ) -> CronTrigger[E] | ManualTrigger[E] | EventTrigger[E, SourceEvent]:
        """
        Create a trigger instance based on the specified type.

        Args:
            trigger_type: Type of trigger to create.
            **kwargs: Arguments specific to the trigger type.

        Returns:
            A trigger instance.

        Raises:
            ValueError: If trigger_type is invalid or required arguments are missing.

        Trigger-specific arguments:

        CRON:
            - cron_expression (str): Cron expression (required)
            - event_factory (Callable[[], E]): Factory function (required)
            - timezone (str | None): Timezone (optional)

        MANUAL:
            - event_factory (Callable[[dict], E]): Factory function (required)

        EVENT:
            - consumer (ConsumerPort): Consumer instance (required)
            - source_topic (str): Source topic or pattern (required)
            - event_factory (Callable[[EventBase], E]): Factory function (required)
            - filter_func (Callable[[EventBase], bool] | None): Filter function (optional)
        """
        # Convert string to enum if necessary
        if isinstance(trigger_type, str):
            try:
                trigger_type = TriggerType(trigger_type.lower())
            except ValueError as e:
                raise ValueError(
                    f"Invalid trigger type '{trigger_type}'. "
                    f"Must be one of: {[t.value for t in TriggerType]}"
                ) from e

        if trigger_type == TriggerType.CRON:
            return TriggerFactory._create_cron_trigger(**kwargs)
        elif trigger_type == TriggerType.MANUAL:
            return TriggerFactory._create_manual_trigger(**kwargs)
        elif trigger_type == TriggerType.EVENT:
            return TriggerFactory._create_event_trigger(**kwargs)
        else:
            raise ValueError(f"Unsupported trigger type: {trigger_type}")

    @staticmethod
    def _create_cron_trigger(**kwargs: Any) -> CronTrigger[E]:
        """Create a cron trigger."""
        required = ["cron_expression", "event_factory"]
        TriggerFactory._validate_args(required, kwargs, "CronTrigger")

        return CronTrigger(
            cron_expression=kwargs["cron_expression"],
            event_factory=kwargs["event_factory"],
            timezone=kwargs.get("timezone"),
        )

    @staticmethod
    def _create_manual_trigger(**kwargs: Any) -> ManualTrigger[E]:
        """Create a manual trigger."""
        required = ["event_factory"]
        TriggerFactory._validate_args(required, kwargs, "ManualTrigger")

        return ManualTrigger(event_factory=kwargs["event_factory"])

    @staticmethod
    def _create_event_trigger(**kwargs: Any) -> EventTrigger[E, SourceEvent]:
        """Create an event trigger."""
        required = ["consumer", "source_topic", "event_factory"]
        TriggerFactory._validate_args(required, kwargs, "EventTrigger")

        return EventTrigger(
            consumer=kwargs["consumer"],
            source_topic=kwargs["source_topic"],
            event_factory=kwargs["event_factory"],
            filter_func=kwargs.get("filter_func"),
        )

    @staticmethod
    def _validate_args(
        required_args: list[str], provided_args: dict[str, Any], trigger_name: str
    ) -> None:
        """Validate that all required arguments are provided."""
        missing = [arg for arg in required_args if arg not in provided_args]
        if missing:
            raise ValueError(f"Missing required arguments for {trigger_name}: {missing}")


def create_cron_trigger[E: EventBase](
    cron_expression: str,
    event_factory: Callable[[], E],
    timezone: str | None = None,
) -> CronTrigger[E]:
    """
    Convenience function to create a cron trigger.

    Args:
        cron_expression: Cron expression (e.g., "*/5 * * * *").
        event_factory: Factory function that creates events.
        timezone: Optional timezone.

    Returns:
        CronTrigger instance.
    """
    return CronTrigger(
        cron_expression=cron_expression,
        event_factory=event_factory,
        timezone=timezone,
    )


def create_manual_trigger[E: EventBase](
    event_factory: Callable[[dict[str, Any]], E],
) -> ManualTrigger[E]:
    """
    Convenience function to create a manual trigger.

    Args:
        event_factory: Factory function that creates events from payload.

    Returns:
        ManualTrigger instance.
    """
    return ManualTrigger(event_factory=event_factory)


def create_event_trigger[SourceEvent: EventBase, E: EventBase](
    consumer: ConsumerPort[SourceEvent],
    source_topic: str,
    event_factory: Callable[[SourceEvent], E],
    filter_func: Callable[[SourceEvent], bool] | None = None,
) -> EventTrigger[E, SourceEvent]:
    """
    Convenience function to create an event trigger.

    Args:
        consumer: Consumer to listen for source events (any ConsumerPort implementation).
        source_topic: Source topic or pattern.
        event_factory: Factory function that creates events from source events.
        filter_func: Optional filter function.

    Returns:
        EventTrigger instance.
    """
    return EventTrigger(
        consumer=consumer,
        source_topic=source_topic,
        event_factory=event_factory,
        filter_func=filter_func,
    )
