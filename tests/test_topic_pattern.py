"""Tests for topic pattern matching and wildcard subscriptions."""

import asyncio

import pytest

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.event_system.utils.topic_matcher import (
    compile_topic_pattern,
    get_matching_topics,
    match_topic_pattern,
)
from tests.conftest import AnotherDummyEvent, DummyEvent


class TestTopicMatcher:
    """Test topic pattern matching utilities."""

    def test_match_single_level_wildcard(self) -> None:
        """Test * wildcard matches single level."""
        assert match_topic_pattern("data.*", "data.pipeline")
        assert match_topic_pattern("data.*", "data.warehouse")
        assert not match_topic_pattern("data.*", "data.pipeline.ingestion")
        assert not match_topic_pattern("data.*", "data")

    def test_match_multi_level_wildcard(self) -> None:
        """Test ** wildcard matches multiple levels."""
        assert match_topic_pattern("data.**", "data.pipeline")
        assert match_topic_pattern("data.**", "data.warehouse")
        assert match_topic_pattern("data.**", "data.pipeline.ingestion")
        assert match_topic_pattern("data.**", "data.pipeline.ingestion.batch")

    def test_match_mixed_pattern(self) -> None:
        """Test pattern with both wildcards and literal parts."""
        assert match_topic_pattern("data.*.ingestion", "data.pipeline.ingestion")
        assert match_topic_pattern("data.*.ingestion", "data.warehouse.ingestion")
        assert not match_topic_pattern("data.*.ingestion", "data.pipeline.transformation")
        assert not match_topic_pattern("data.*.ingestion", "data.pipeline.ingestion.batch")

    def test_get_matching_topics(self) -> None:
        """Test getting all matching topics from a set."""
        topics = {
            "data.pipeline",
            "data.warehouse",
            "data.pipeline.ingestion",
            "analytics.report",
        }

        assert get_matching_topics("data.*", topics) == {
            "data.pipeline",
            "data.warehouse",
        }

        assert get_matching_topics("data.**", topics) == {
            "data.pipeline",
            "data.warehouse",
            "data.pipeline.ingestion",
        }

        assert get_matching_topics("*.pipeline", topics) == {"data.pipeline"}

        assert get_matching_topics("**", topics) == topics

    def test_compile_topic_pattern(self) -> None:
        """Test that patterns are correctly compiled to regex."""
        pattern = compile_topic_pattern("data.*")
        assert pattern.match("data.pipeline")
        assert not pattern.match("data.pipeline.ingestion")


class TestBrokerDeclareTopic:
    """Test broker's declare_topic functionality."""

    @pytest.mark.asyncio
    async def test_declare_topic_creates_new(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test that declare_topic creates a new topic."""
        assert "test.topic" not in broker.get_topics()

        queue = await broker.declare_topic("test.topic")

        assert "test.topic" in broker.get_topics()
        assert queue is not None
        assert queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_declare_topic_idempotent(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test that declare_topic is idempotent."""
        # First declaration
        queue1 = await broker.declare_topic("test.topic")
        assert "test.topic" in broker.get_topics()

        # Second declaration should return the same queue
        queue2 = await broker.declare_topic("test.topic")
        assert queue2 is queue1  # Same object

        # Add an event to verify it's the same queue
        event = DummyEvent(message="Test")
        await queue1.put(event)
        assert queue2.qsize() == 1  # Should see the event in queue2

    @pytest.mark.asyncio
    async def test_declare_multiple_topics(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test declaring multiple topics."""
        topics = ["topic1", "topic2", "topic3"]

        for topic in topics:
            await broker.declare_topic(topic)

        assert broker.get_topics() == set(topics)


class TestBrokerPattern:
    """Test broker pattern publishing."""

    @pytest.mark.asyncio
    async def test_publish_pattern_single_level(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test publishing to pattern with single level wildcard."""
        # Declare topics
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.warehouse")
        await broker.declare_topic("data.pipeline.ingestion")

        event = DummyEvent(message="Test message")

        # Publish to pattern
        await broker.publish_pattern("data.*", event)

        # Check that event was published to matching topics only
        queue1 = await broker.get_queue("data.pipeline")
        queue2 = await broker.get_queue("data.warehouse")
        queue3 = await broker.get_queue("data.pipeline.ingestion")

        assert queue1.qsize() == 1
        assert queue2.qsize() == 1
        assert queue3.qsize() == 0  # Should not match

    @pytest.mark.asyncio
    async def test_publish_pattern_multi_level(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test publishing to pattern with multi level wildcard."""
        # Declare topics
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.warehouse")
        await broker.declare_topic("data.pipeline.ingestion")
        await broker.declare_topic("analytics.report")

        event = DummyEvent(message="Test message")

        # Publish to pattern
        await broker.publish_pattern("data.**", event)

        # Check that event was published to matching topics only
        queue1 = await broker.get_queue("data.pipeline")
        queue2 = await broker.get_queue("data.warehouse")
        queue3 = await broker.get_queue("data.pipeline.ingestion")
        queue4 = await broker.get_queue("analytics.report")

        assert queue1.qsize() == 1
        assert queue2.qsize() == 1
        assert queue3.qsize() == 1
        assert queue4.qsize() == 0  # Should not match

    @pytest.mark.asyncio
    async def test_publish_pattern_no_matches(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test publishing to pattern with no matching topics."""
        await broker.declare_topic("data.pipeline")

        event = DummyEvent(message="Test message")

        # Publish to pattern with no matches (should not raise error)
        await broker.publish_pattern("analytics.*", event)

        # No events should be published
        queue = await broker.get_queue("data.pipeline")
        assert queue.qsize() == 0


class TestPublisherPattern:
    """Test publisher pattern publishing."""

    @pytest.mark.asyncio
    async def test_publisher_publish_pattern(
        self,
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test publisher's publish_pattern method."""
        # Declare topics
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.warehouse")

        event = DummyEvent(message="Pattern test")

        # Publish using publisher
        await publisher.publish_pattern("data.*", event)

        # Verify events were published
        queue1 = await broker.get_queue("data.pipeline")
        queue2 = await broker.get_queue("data.warehouse")

        assert queue1.qsize() == 1
        assert queue2.qsize() == 1


class TestConsumerPattern:
    """Test consumer pattern consumption."""

    @pytest.mark.asyncio
    async def test_consume_pattern_single_level(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test consuming from pattern with single level wildcard."""

        # Declare topics for pattern matching
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.warehouse")
        await broker.declare_topic("data.pipeline.ingestion")

        async def publish_events() -> None:
            await asyncio.sleep(0.05)
            # Publish to multiple topics
            await publisher.publish(
                "data.pipeline", DummyEvent(topic="data.pipeline", message="Pipeline 1")
            )
            await publisher.publish(
                "data.pipeline", DummyEvent(topic="data.pipeline", message="Pipeline 2")
            )
            await publisher.publish(
                "data.warehouse", DummyEvent(topic="data.warehouse", message="Warehouse 1")
            )
            await publisher.publish(
                "data.pipeline.ingestion",
                DummyEvent(topic="data.pipeline.ingestion", message="Should not match"),
            )

            # Send CompletedEvent to all matching topics
            await publisher.publish("data.pipeline", CompletedEvent(topic="data.pipeline"))
            await publisher.publish("data.warehouse", CompletedEvent(topic="data.warehouse"))
            await publisher.publish(
                "data.pipeline.ingestion", CompletedEvent(topic="data.pipeline.ingestion")
            )

        async def consume_events() -> list[EventBase]:
            events = []
            async for event in consumer.consume_pattern("data.*"):
                events.append(event)
            return events

        # Start tasks
        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_events())

        await publish_task
        consumed_events = await consume_task

        # Should have consumed 3 events (2 from pipeline, 1 from warehouse)
        assert len(consumed_events) == 3
        messages = [event.message for event in consumed_events if isinstance(event, DummyEvent)]
        assert "Pipeline 1" in messages
        assert "Pipeline 2" in messages
        assert "Warehouse 1" in messages
        assert "Should not match" not in messages

    @pytest.mark.asyncio
    async def test_consume_pattern_multi_level(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test consuming from pattern with multi level wildcard."""

        # Declare topics
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.pipeline.ingestion")
        await broker.declare_topic("data.warehouse")
        await broker.declare_topic("analytics.report")

        async def publish_events() -> None:
            await asyncio.sleep(0.05)
            # Publish to multiple topics
            await publisher.publish(
                "data.pipeline", DummyEvent(topic="data.pipeline", message="Pipeline")
            )
            await publisher.publish(
                "data.pipeline.ingestion",
                DummyEvent(topic="data.pipeline.ingestion", message="Ingestion"),
            )
            await publisher.publish(
                "data.warehouse", DummyEvent(topic="data.warehouse", message="Warehouse")
            )
            await publisher.publish(
                "analytics.report", DummyEvent(topic="analytics.report", message="Analytics")
            )

            # Send CompletedEvent to all topics
            await publisher.publish("data.pipeline", CompletedEvent(topic="data.pipeline"))
            await publisher.publish(
                "data.pipeline.ingestion", CompletedEvent(topic="data.pipeline.ingestion")
            )
            await publisher.publish("data.warehouse", CompletedEvent(topic="data.warehouse"))
            await publisher.publish("analytics.report", CompletedEvent(topic="analytics.report"))

        async def consume_events() -> list[EventBase]:
            events = []
            async for event in consumer.consume_pattern("data.**"):
                events.append(event)
            return events

        # Start tasks
        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_events())

        await publish_task
        consumed_events = await consume_task

        # Should have consumed 3 events (all data.* topics)
        assert len(consumed_events) == 3
        messages = [event.message for event in consumed_events if isinstance(event, DummyEvent)]
        assert "Pipeline" in messages
        assert "Ingestion" in messages
        assert "Warehouse" in messages
        assert "Analytics" not in messages

    @pytest.mark.asyncio
    async def test_consume_pattern_mixed_event_types(
        self,
        consumer: InMemoryConsumer[EventBase],
        publisher: InMemoryPublisher[EventBase],
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test consuming different event types from pattern."""

        # Declare topics
        await broker.declare_topic("data.pipeline")
        await broker.declare_topic("data.warehouse")

        async def publish_events() -> None:
            await asyncio.sleep(0.05)
            # Publish different event types
            await publisher.publish(
                "data.pipeline", DummyEvent(topic="data.pipeline", message="Message 1")
            )
            await publisher.publish(
                "data.warehouse", AnotherDummyEvent(topic="data.warehouse", value=100)
            )
            await publisher.publish(
                "data.pipeline", DummyEvent(topic="data.pipeline", message="Message 2")
            )
            await publisher.publish(
                "data.warehouse", AnotherDummyEvent(topic="data.warehouse", value=200)
            )

            # Send CompletedEvent
            await publisher.publish("data.pipeline", CompletedEvent(topic="data.pipeline"))
            await publisher.publish("data.warehouse", CompletedEvent(topic="data.warehouse"))

        async def consume_events() -> tuple[list[DummyEvent], list[AnotherDummyEvent]]:
            dummy_events = []
            another_events = []
            async for event in consumer.consume_pattern("data.*"):
                if isinstance(event, AnotherDummyEvent):
                    another_events.append(event)
                elif isinstance(event, DummyEvent):
                    dummy_events.append(event)
            return dummy_events, another_events

        # Start tasks
        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_events())

        await publish_task
        dummy_events, another_events = await consume_task

        # Verify both event types were consumed
        assert len(dummy_events) == 2
        assert len(another_events) == 2
        assert {e.message for e in dummy_events} == {"Message 1", "Message 2"}
        assert {e.value for e in another_events} == {100, 200}
