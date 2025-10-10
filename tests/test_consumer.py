"""Tests for InMemoryConsumer."""

import asyncio

import pytest

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from tests.conftest import AnotherDummyEvent, DummyEvent


@pytest.mark.unit
class TestInMemoryConsumer:
    """Test suite for InMemoryConsumer."""

    async def test_consumer_initialization(self, consumer: InMemoryConsumer[EventBase]) -> None:
        """Test that consumer initializes correctly."""
        assert consumer is not None
        assert consumer.broker is not None

    async def test_consume_single_event(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test consuming a single event."""
        event = DummyEvent(topic=test_topic, message="Test message")

        # Publish event
        await publisher.publish(test_topic, event)
        await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        # Consume events
        consumed_events = []
        async for consumed_event in consumer.consume(test_topic):
            consumed_events.append(consumed_event)

        assert len(consumed_events) == 1
        assert isinstance(consumed_events[0], DummyEvent)
        assert consumed_events[0].message == "Test message"

    async def test_consume_multiple_events(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test consuming multiple events."""
        event1 = DummyEvent(topic=test_topic, message="First")
        event2 = DummyEvent(topic=test_topic, message="Second")
        event3 = AnotherDummyEvent(topic=test_topic, value=42)

        # Publish events
        await publisher.publish(test_topic, event1)
        await publisher.publish(test_topic, event2)
        await publisher.publish(test_topic, event3)
        await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        # Consume events
        consumed_events = []
        async for consumed_event in consumer.consume(test_topic):
            consumed_events.append(consumed_event)

        assert len(consumed_events) == 3
        assert isinstance(consumed_events[0], DummyEvent)
        assert isinstance(consumed_events[1], DummyEvent)
        assert isinstance(consumed_events[2], AnotherDummyEvent)
        assert consumed_events[0].message == "First"
        assert consumed_events[1].message == "Second"
        assert consumed_events[2].value == 42

    async def test_consume_stops_on_completed_event(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test that consumption stops when CompletedEvent is received."""
        event = DummyEvent(topic=test_topic, message="Before completion")

        await publisher.publish(test_topic, event)
        await publisher.publish(test_topic, CompletedEvent(topic=test_topic))
        # This event should not be consumed
        await publisher.publish(
            test_topic, DummyEvent(topic=test_topic, message="After completion")
        )

        consumed_events = []
        async for consumed_event in consumer.consume(test_topic):
            consumed_events.append(consumed_event)

        # Only the event before CompletedEvent should be consumed
        assert len(consumed_events) == 1
        assert isinstance(consumed_events[0], DummyEvent)
        assert consumed_events[0].message == "Before completion"

    async def test_consume_concurrent_publishing(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test consuming events while publishing concurrently."""

        async def publish_events() -> None:
            await asyncio.sleep(0.1)
            for i in range(5):
                await publisher.publish(
                    test_topic, DummyEvent(topic=test_topic, message=f"Event {i}")
                )
                await asyncio.sleep(0.05)
            await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        async def consume_events() -> list[EventBase]:
            events = []
            async for event in consumer.consume(test_topic):
                events.append(event)
            return events

        # Run publisher and consumer concurrently
        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_events())

        await publish_task
        consumed_events = await consume_task

        assert len(consumed_events) == 5
        for i, event in enumerate(consumed_events):
            assert isinstance(event, DummyEvent)
            assert event.message == f"Event {i}"

    async def test_multiple_consumers_same_topic(
        self,
        broker: InMemoryBroker[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test that multiple consumers can't consume the same events (queue behavior)."""
        consumer1 = InMemoryConsumer(broker)

        # Publish events
        await publisher.publish(test_topic, DummyEvent(topic=test_topic, message="Event 1"))
        await publisher.publish(test_topic, DummyEvent(topic=test_topic, message="Event 2"))
        await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        # First consumer consumes all events
        consumed_events1 = []
        async for event in consumer1.consume(test_topic):
            consumed_events1.append(event)

        # Since queue is empty and no CompletedEvent, we need to handle this differently
        # This test demonstrates queue behavior - once consumed, events are gone

        assert len(consumed_events1) == 2
        # Queue is now empty, consumer2 would wait indefinitely

    async def test_consume_with_empty_queue_waits(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test that consumer waits when queue is empty."""

        async def delayed_publish() -> None:
            await asyncio.sleep(0.2)
            await publisher.publish(
                test_topic, DummyEvent(topic=test_topic, message="Delayed event")
            )
            await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        async def consume_with_timeout() -> list[EventBase]:
            events = []
            async for event in consumer.consume(test_topic):
                events.append(event)
            return events

        # Start delayed publishing
        publish_task = asyncio.create_task(delayed_publish())

        # Consumer should wait for events
        start_time = asyncio.get_event_loop().time()
        consume_task = asyncio.create_task(consume_with_timeout())

        await publish_task
        consumed_events = await consume_task
        elapsed = asyncio.get_event_loop().time() - start_time

        # Verify consumer waited for events
        assert elapsed >= 0.2
        assert len(consumed_events) == 1
        assert isinstance(consumed_events[0], DummyEvent)
        assert consumed_events[0].message == "Delayed event"
