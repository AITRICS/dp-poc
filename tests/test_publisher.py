"""Tests for InMemoryPublisher."""

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from tests.conftest import DummyEvent


@pytest.mark.unit
class TestInMemoryPublisher:
    """Test suite for InMemoryPublisher."""

    async def test_publisher_initialization(self, publisher: InMemoryPublisher[EventBase]) -> None:
        """Test that publisher initializes correctly."""
        assert publisher is not None
        assert publisher.broker is not None

    async def test_publish_adds_event_to_queue(
        self,
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
        test_topic: str,
    ) -> None:
        """Test that publish adds an event to the broker's queue."""
        event = DummyEvent(message="Hello, World!")
        await publisher.publish(test_topic, event)

        queue = await broker.get_queue(test_topic)
        assert queue.qsize() == 1

        received_event = await queue.get()
        assert isinstance(received_event, DummyEvent)
        assert received_event.message == "Hello, World!"

    async def test_publish_multiple_events(
        self,
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
        test_topic: str,
    ) -> None:
        """Test that multiple events can be published."""
        event1 = DummyEvent(message="First")
        event2 = DummyEvent(message="Second")
        event3 = DummyEvent(message="Third")

        await publisher.publish(test_topic, event1)
        await publisher.publish(test_topic, event2)
        await publisher.publish(test_topic, event3)

        queue = await broker.get_queue(test_topic)
        assert queue.qsize() == 3

        # Verify events are in FIFO order
        received1 = await queue.get()
        received2 = await queue.get()
        received3 = await queue.get()

        assert isinstance(received1, DummyEvent)
        assert isinstance(received2, DummyEvent)
        assert isinstance(received3, DummyEvent)
        assert received1.message == "First"
        assert received2.message == "Second"
        assert received3.message == "Third"

    async def test_publish_to_different_topics(
        self,
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test that events can be published to different topics."""
        topic1 = "topic1"
        topic2 = "topic2"

        event1 = DummyEvent(message="Topic 1 message")
        event2 = DummyEvent(message="Topic 2 message")

        await publisher.publish(topic1, event1)
        await publisher.publish(topic2, event2)

        queue1 = await broker.get_queue(topic1)
        queue2 = await broker.get_queue(topic2)

        assert queue1.qsize() == 1
        assert queue2.qsize() == 1

        received1 = await queue1.get()
        received2 = await queue2.get()

        assert isinstance(received1, DummyEvent)
        assert isinstance(received2, DummyEvent)
        assert received1.message == "Topic 1 message"
        assert received2.message == "Topic 2 message"

    async def test_published_event_has_id_and_timestamp(
        self,
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
        test_topic: str,
    ) -> None:
        """Test that published events have event_id and timestamp."""
        event = DummyEvent(message="Test")
        await publisher.publish(test_topic, event)

        queue = await broker.get_queue(test_topic)
        received_event = await queue.get()

        assert received_event.event_id is not None
        assert received_event.timestamp is not None
