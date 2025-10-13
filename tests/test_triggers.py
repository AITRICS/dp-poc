"""Tests for trigger implementations."""

import asyncio
from datetime import UTC, datetime

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.scheduler import (
    create_cron_trigger,
    create_event_trigger,
    create_manual_trigger,
)


# Test events
class TriggerTestEvent(EventBase):
    """Test event."""

    def __init__(self, topic: str, message: str, **kwargs: str) -> None:
        super().__init__(topic=topic, message=message, **kwargs)
        self.message = message


class SourceEvent(EventBase):
    """Source event for event trigger."""

    def __init__(self, topic: str, value: int, **kwargs: str) -> None:
        super().__init__(topic=topic, value=value, **kwargs)
        self.value = value


class TriggeredEvent(EventBase):
    """Event created by trigger."""

    def __init__(self, topic: str, triggered_by: int, **kwargs: str) -> None:
        super().__init__(topic=topic, triggered_by=triggered_by, **kwargs)
        self.triggered_by = triggered_by


@pytest.mark.asyncio
class TestManualTrigger:
    """Test cases for ManualTrigger."""

    async def test_manual_trigger_basic(self) -> None:
        """Test basic manual trigger functionality."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(
                topic="test.manual", message=payload.get("message", "")
            )
        )

        # Start trigger
        await trigger.start()
        assert trigger.is_running()

        # Trigger event
        await trigger.trigger({"message": "hello"})

        # Consume event using async for
        async for event in trigger.emit():
            assert isinstance(event, TriggerTestEvent)
            assert event.message == "hello"
            assert event.meta.topic == "test.manual"
            break

        # Stop trigger
        await trigger.stop()
        assert not trigger.is_running()

    async def test_manual_trigger_multiple_events(self) -> None:
        """Test triggering multiple events."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(
                topic="test.manual", message=payload.get("message", "")
            )
        )

        await trigger.start()

        # Trigger multiple events
        await trigger.trigger({"message": "first"})
        await trigger.trigger({"message": "second"})
        await trigger.trigger({"message": "third"})

        # Consume events
        messages = []
        async for event in trigger.emit():
            messages.append(event.message)
            if len(messages) >= 3:
                break

        assert messages == ["first", "second", "third"]

        await trigger.stop()

    async def test_manual_trigger_count(self) -> None:
        """Test trigger count tracking."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(topic="test", message="")
        )

        await trigger.start()

        assert trigger.get_trigger_count() == 0

        await trigger.trigger({})
        assert trigger.get_trigger_count() == 1

        await trigger.trigger({})
        await trigger.trigger({})
        assert trigger.get_trigger_count() == 3

        await trigger.stop()


@pytest.mark.asyncio
class TestCronTrigger:
    """Test cases for CronTrigger."""

    @pytest.mark.slow
    async def test_cron_trigger_basic(self) -> None:
        """Test basic cron trigger functionality."""
        event_count = 0

        def event_factory() -> TriggerTestEvent:
            nonlocal event_count
            event_count += 1
            return TriggerTestEvent(topic="test.cron", message=f"event_{event_count}")

        # Trigger every second
        trigger = create_cron_trigger(
            cron_expression="* * * * * *",  # Every second
            event_factory=event_factory,
        )

        await trigger.start()
        assert trigger.is_running()

        # Collect first 2 events
        collected = []
        async for event in trigger.emit():
            assert isinstance(event, TriggerTestEvent)
            collected.append(event.message)
            if len(collected) >= 2:
                break

        assert collected == ["event_1", "event_2"]

        await trigger.stop()
        assert not trigger.is_running()

    @pytest.mark.slow
    async def test_cron_trigger_expression(self) -> None:
        """Test cron trigger with specific expression."""
        events_created = []

        def event_factory() -> TriggerTestEvent:
            event = TriggerTestEvent(
                topic="test.cron",
                message=f"scheduled_{datetime.now(UTC).isoformat()}",
            )
            events_created.append(event)
            return event

        # Every 2 seconds
        trigger = create_cron_trigger(cron_expression="*/2 * * * * *", event_factory=event_factory)

        await trigger.start()

        # Collect events for ~5 seconds
        collected = []
        start_time = asyncio.get_event_loop().time()

        async for event in trigger.emit():
            collected.append(event)
            if asyncio.get_event_loop().time() - start_time > 5:
                break

        await trigger.stop()

        # Should have collected 2-3 events in 5 seconds (every 2 seconds)
        assert 2 <= len(collected) <= 3


@pytest.mark.asyncio
class TestEventTrigger:
    """Test cases for EventTrigger."""

    async def test_event_trigger_basic(self) -> None:
        """Test basic event trigger functionality."""
        broker: InMemoryBroker[SourceEvent] = InMemoryBroker()
        consumer: InMemoryConsumer[SourceEvent] = InMemoryConsumer(broker)

        await broker.declare_topic("source.topic")

        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="source.topic",
            event_factory=lambda src: TriggeredEvent(topic="target.topic", triggered_by=src.value),
        )

        await trigger.start()
        assert trigger.is_running()

        # Publish source event
        source = SourceEvent(topic="source.topic", value=42)
        queue = await broker.get_queue("source.topic")
        await queue.put(source)

        # Consume triggered event
        async for event in trigger.emit():
            assert isinstance(event, TriggeredEvent)
            assert event.triggered_by == 42
            assert event.meta.topic == "target.topic"
            break

        await trigger.stop()
        assert not trigger.is_running()

    async def test_event_trigger_with_filter(self) -> None:
        """Test event trigger with filter function."""
        broker: InMemoryBroker[SourceEvent] = InMemoryBroker()
        consumer: InMemoryConsumer[SourceEvent] = InMemoryConsumer(broker)

        await broker.declare_topic("source.topic")

        # Only trigger if value > 10
        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="source.topic",
            event_factory=lambda src: TriggeredEvent(topic="target.topic", triggered_by=src.value),
            filter_func=lambda src: src.value > 10,
        )

        await trigger.start()

        # Publish events
        queue = await broker.get_queue("source.topic")
        await queue.put(SourceEvent(topic="source.topic", value=5))  # Filtered out
        await queue.put(SourceEvent(topic="source.topic", value=15))  # Pass
        await queue.put(SourceEvent(topic="source.topic", value=8))  # Filtered out
        await queue.put(SourceEvent(topic="source.topic", value=20))  # Pass

        # Should only get events with value > 10
        values = []
        async for event in trigger.emit():
            values.append(event.triggered_by)
            if len(values) >= 2:
                break

        assert values == [15, 20]

        await trigger.stop()

    async def test_event_trigger_multiple_source_events(self) -> None:
        """Test event trigger with multiple source events."""
        broker: InMemoryBroker[SourceEvent] = InMemoryBroker()
        consumer: InMemoryConsumer[SourceEvent] = InMemoryConsumer(broker)

        await broker.declare_topic("source.topic")

        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="source.topic",
            event_factory=lambda src: TriggeredEvent(topic="target.topic", triggered_by=src.value),
        )

        await trigger.start()

        # Publish multiple source events
        queue = await broker.get_queue("source.topic")
        for i in range(5):
            await queue.put(SourceEvent(topic="source.topic", value=i))

        # Consume all triggered events
        triggered_values = []
        async for event in trigger.emit():
            triggered_values.append(event.triggered_by)
            if len(triggered_values) >= 5:
                break

        assert triggered_values == [0, 1, 2, 3, 4]

        await trigger.stop()

    async def test_event_trigger_pattern_matching(self) -> None:
        """Test event trigger with pattern matching."""
        broker: InMemoryBroker[SourceEvent] = InMemoryBroker()
        consumer: InMemoryConsumer[SourceEvent] = InMemoryConsumer(broker)

        # Declare multiple topics
        await broker.declare_topic("data.sensor.1")
        await broker.declare_topic("data.sensor.2")
        await broker.declare_topic("data.sensor.3")

        # Consume from pattern
        trigger = create_event_trigger(
            consumer=consumer,
            source_topic="data.sensor.*",
            event_factory=lambda src: TriggeredEvent(
                topic="alerts.sensors", triggered_by=src.value
            ),
        )

        await trigger.start()

        # Publish to different topics
        queue1 = await broker.get_queue("data.sensor.1")
        queue2 = await broker.get_queue("data.sensor.2")
        queue3 = await broker.get_queue("data.sensor.3")

        await queue1.put(SourceEvent(topic="data.sensor.1", value=10))
        await queue2.put(SourceEvent(topic="data.sensor.2", value=20))
        await queue3.put(SourceEvent(topic="data.sensor.3", value=30))

        # Collect triggered events
        values = []
        async for event in trigger.emit():
            values.append(event.triggered_by)
            if len(values) >= 3:
                break

        # Should receive all three values (order may vary)
        assert sorted(values) == [10, 20, 30]

        await trigger.stop()


@pytest.mark.asyncio
class TestTriggerLifecycle:
    """Test trigger lifecycle management."""

    async def test_trigger_start_stop_multiple_times(self) -> None:
        """Test starting and stopping trigger multiple times."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(topic="test", message="")
        )

        # Start/stop cycle 1
        await trigger.start()
        assert trigger.is_running()
        await trigger.stop()
        assert not trigger.is_running()

        # Start/stop cycle 2
        await trigger.start()
        assert trigger.is_running()
        await trigger.stop()
        assert not trigger.is_running()

    async def test_trigger_double_start(self) -> None:
        """Test that double start is handled gracefully."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(topic="test", message="")
        )

        await trigger.start()
        await trigger.start()  # Should be idempotent
        assert trigger.is_running()

        await trigger.stop()

    async def test_trigger_double_stop(self) -> None:
        """Test that double stop is handled gracefully."""
        trigger = create_manual_trigger(
            event_factory=lambda payload: TriggerTestEvent(topic="test", message="")
        )

        await trigger.start()
        await trigger.stop()
        await trigger.stop()  # Should be idempotent
        assert not trigger.is_running()
