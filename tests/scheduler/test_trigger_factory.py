"""Tests for TriggerFactory."""

from typing import cast

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.scheduler import TriggerFactory, TriggerType
from app.scheduler.infrastructure.cron_trigger import CronTrigger
from app.scheduler.infrastructure.event_trigger import EventTrigger
from app.scheduler.infrastructure.manual_trigger import ManualTrigger


# Test events
class FactoryTestEvent(EventBase):
    """Test event for factory."""

    def __init__(self, topic: str, data: str, **kwargs: str) -> None:
        super().__init__(topic=topic, data=data, **kwargs)
        self.data = data


class SourceTestEvent(EventBase):
    """Source event for event trigger."""

    def __init__(self, topic: str, value: int, **kwargs: str) -> None:
        super().__init__(topic=topic, value=value, **kwargs)
        self.value = value


class TestTriggerFactory:
    """Test cases for TriggerFactory."""

    def test_create_manual_trigger_with_enum(self) -> None:
        """Test creating manual trigger using enum."""
        trigger = cast(
            "ManualTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                TriggerType.MANUAL,
                event_factory=lambda p: FactoryTestEvent(topic="test", data=p.get("data", "")),
            ),
        )

        assert isinstance(trigger, ManualTrigger)

    def test_create_manual_trigger_with_string(self) -> None:
        """Test creating manual trigger using string."""
        trigger = cast(
            "ManualTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                "manual",
                event_factory=lambda p: FactoryTestEvent(topic="test", data=p.get("data", "")),
            ),
        )

        assert isinstance(trigger, ManualTrigger)

    def test_create_cron_trigger_with_enum(self) -> None:
        """Test creating cron trigger using enum."""
        trigger = cast(
            "CronTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                TriggerType.CRON,
                cron_expression="* * * * * *",
                event_factory=lambda: FactoryTestEvent(topic="test", data="cron"),
            ),
        )

        assert isinstance(trigger, CronTrigger)

    def test_create_cron_trigger_with_string(self) -> None:
        """Test creating cron trigger using string."""
        trigger = cast(
            "CronTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                "cron",
                cron_expression="*/5 * * * *",
                event_factory=lambda: FactoryTestEvent(topic="test", data="cron"),
            ),
        )

        assert isinstance(trigger, CronTrigger)

    def test_create_cron_trigger_with_timezone(self) -> None:
        """Test creating cron trigger with timezone."""
        trigger = cast(
            "CronTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                TriggerType.CRON,
                cron_expression="0 0 * * *",
                event_factory=lambda: FactoryTestEvent(topic="test", data="daily"),
                timezone="America/New_York",
            ),
        )

        assert isinstance(trigger, CronTrigger)

    def test_create_event_trigger_with_enum(self) -> None:
        """Test creating event trigger using enum."""
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        trigger = cast(
            "EventTrigger[FactoryTestEvent, EventBase]",
            TriggerFactory.create_trigger(
                TriggerType.EVENT,
                consumer=consumer,
                source_topic="source.topic",
                event_factory=lambda src: FactoryTestEvent(topic="target", data="triggered"),
            ),
        )

        assert isinstance(trigger, EventTrigger)

    def test_create_event_trigger_with_string(self) -> None:
        """Test creating event trigger using string."""
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        trigger = cast(
            "EventTrigger[FactoryTestEvent, EventBase]",
            TriggerFactory.create_trigger(
                "event",
                consumer=consumer,
                source_topic="source.topic",
                event_factory=lambda src: FactoryTestEvent(topic="target", data="triggered"),
            ),
        )

        assert isinstance(trigger, EventTrigger)

    def test_create_event_trigger_with_filter(self) -> None:
        """Test creating event trigger with filter."""
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        trigger = cast(
            "EventTrigger[FactoryTestEvent, EventBase]",
            TriggerFactory.create_trigger(
                TriggerType.EVENT,
                consumer=consumer,
                source_topic="source.topic",
                event_factory=lambda src: FactoryTestEvent(topic="target", data="filtered"),
                filter_func=lambda src: hasattr(src, "value") and src.value > 10,
            ),
        )

        assert isinstance(trigger, EventTrigger)

    def test_invalid_trigger_type_string(self) -> None:
        """Test that invalid trigger type string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid trigger type"):
            TriggerFactory.create_trigger(
                "invalid_type",
                event_factory=lambda: FactoryTestEvent(topic="test", data=""),
            )

    def test_missing_required_args_manual(self) -> None:
        """Test that missing required args for manual trigger raises ValueError."""
        with pytest.raises(ValueError, match="Missing required arguments"):
            TriggerFactory.create_trigger(TriggerType.MANUAL)

    def test_missing_required_args_cron(self) -> None:
        """Test that missing required args for cron trigger raises ValueError."""
        with pytest.raises(ValueError, match="Missing required arguments"):
            TriggerFactory.create_trigger(
                TriggerType.CRON,
                cron_expression="* * * * *",
                # Missing event_factory
            )

        with pytest.raises(ValueError, match="Missing required arguments"):
            TriggerFactory.create_trigger(
                TriggerType.CRON,
                event_factory=lambda: FactoryTestEvent(topic="test", data=""),
                # Missing cron_expression
            )

    def test_missing_required_args_event(self) -> None:
        """Test that missing required args for event trigger raises ValueError."""
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        with pytest.raises(ValueError, match="Missing required arguments"):
            TriggerFactory.create_trigger(
                TriggerType.EVENT,
                consumer=consumer,
                source_topic="source.topic",
                # Missing event_factory
            )

        with pytest.raises(ValueError, match="Missing required arguments"):
            TriggerFactory.create_trigger(
                TriggerType.EVENT,
                consumer=consumer,
                event_factory=lambda src: FactoryTestEvent(topic="test", data=""),
                # Missing source_topic
            )

    def test_case_insensitive_string_type(self) -> None:
        """Test that trigger type string is case-insensitive."""
        trigger1 = cast(
            "ManualTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                "MANUAL",
                event_factory=lambda p: FactoryTestEvent(topic="test", data=""),
            ),
        )
        trigger2 = cast(
            "ManualTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                "Manual",
                event_factory=lambda p: FactoryTestEvent(topic="test", data=""),
            ),
        )
        trigger3 = cast(
            "ManualTrigger[FactoryTestEvent]",
            TriggerFactory.create_trigger(
                "manual",
                event_factory=lambda p: FactoryTestEvent(topic="test", data=""),
            ),
        )

        assert isinstance(trigger1, ManualTrigger)
        assert isinstance(trigger2, ManualTrigger)
        assert isinstance(trigger3, ManualTrigger)


class TestConvenienceFunctions:
    """Test convenience functions for creating triggers."""

    def test_create_manual_trigger(self) -> None:
        """Test create_manual_trigger convenience function."""
        from app.scheduler import create_manual_trigger

        trigger = create_manual_trigger(
            event_factory=lambda p: FactoryTestEvent(topic="test", data=p.get("data", ""))
        )

        assert isinstance(trigger, ManualTrigger)

    def test_create_cron_trigger(self) -> None:
        """Test create_cron_trigger convenience function."""
        from app.scheduler import create_cron_trigger

        trigger = create_cron_trigger(
            cron_expression="0 0 * * *",
            event_factory=lambda: FactoryTestEvent(topic="test", data="daily"),
        )

        assert isinstance(trigger, CronTrigger)

    def test_create_cron_trigger_with_timezone(self) -> None:
        """Test create_cron_trigger with timezone."""
        from app.scheduler import create_cron_trigger

        trigger = create_cron_trigger(
            cron_expression="0 0 * * *",
            event_factory=lambda: FactoryTestEvent(topic="test", data="daily"),
            timezone="UTC",
        )

        assert isinstance(trigger, CronTrigger)

    def test_create_event_trigger(self) -> None:
        """Test create_event_trigger convenience function."""
        from app.scheduler import create_event_trigger

        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="source.topic",
            event_factory=lambda src: FactoryTestEvent(topic="target", data="triggered"),
        )

        assert isinstance(trigger, EventTrigger)

    def test_create_event_trigger_with_filter(self) -> None:
        """Test create_event_trigger with filter."""
        from app.scheduler import create_event_trigger

        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="source.topic",
            event_factory=lambda src: FactoryTestEvent(topic="target", data="filtered"),
            filter_func=lambda src: isinstance(src, SourceTestEvent) and src.value > 10,
        )

        assert isinstance(trigger, EventTrigger)
