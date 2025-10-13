"""Integration tests for the event system."""

import asyncio

import pytest

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from tests.conftest import AnotherDummyEvent, DummyEvent


@pytest.mark.integration
class DummyEventSystemIntegration:
    """Integration tests for the complete event system."""

    async def test_full_pub_sub_flow(
        self,
        broker: InMemoryBroker[EventBase],
        publisher: InMemoryPublisher[EventBase],
        consumer: InMemoryConsumer[EventBase],
        test_topic: str,
    ) -> None:
        """Test complete publish-subscribe flow."""

        async def publish_events() -> None:
            await asyncio.sleep(0.1)
            await publisher.publish(test_topic, DummyEvent(message="Event 1"))
            await publisher.publish(test_topic, DummyEvent(message="Event 2"))
            await publisher.publish(test_topic, AnotherDummyEvent(value=100))
            await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        async def consume_events() -> list[EventBase]:
            events = []
            async for event in consumer.consume(test_topic):
                events.append(event)
                # Simulate processing time
                await asyncio.sleep(0.05)
            return events

        # Run publisher and consumer concurrently
        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_events())

        await publish_task
        consumed_events = await consume_task

        # Verify all events were consumed
        assert len(consumed_events) == 3
        assert isinstance(consumed_events[0], DummyEvent)
        assert consumed_events[0].message == "Event 1"
        assert isinstance(consumed_events[1], DummyEvent)
        assert consumed_events[1].message == "Event 2"
        assert isinstance(consumed_events[2], AnotherDummyEvent)
        assert consumed_events[2].value == 100

    async def test_multiple_topics(
        self,
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test event system with multiple topics."""
        publisher = InMemoryPublisher(broker)
        consumer1 = InMemoryConsumer(broker)
        consumer2 = InMemoryConsumer(broker)

        topic1 = "topic1"
        topic2 = "topic2"

        async def publish_to_topic1() -> None:
            await asyncio.sleep(0.05)
            await publisher.publish(topic1, DummyEvent(message="Topic 1 Event"))
            await publisher.publish(topic1, CompletedEvent(topic=topic1))

        async def publish_to_topic2() -> None:
            await asyncio.sleep(0.05)
            await publisher.publish(topic2, AnotherDummyEvent(value=42))
            await publisher.publish(topic2, CompletedEvent(topic=topic2))

        async def consume_topic1() -> list[EventBase]:
            events = []
            async for event in consumer1.consume(topic1):
                events.append(event)
            return events

        async def consume_topic2() -> list[EventBase]:
            events = []
            async for event in consumer2.consume(topic2):
                events.append(event)
            return events

        # Run all tasks concurrently
        results = await asyncio.gather(
            publish_to_topic1(),
            publish_to_topic2(),
            consume_topic1(),
            consume_topic2(),
        )

        topic1_events = results[2]
        topic2_events = results[3]

        # Verify events are isolated by topic
        assert len(topic1_events) == 1
        assert isinstance(topic1_events[0], DummyEvent)
        assert topic1_events[0].message == "Topic 1 Event"

        assert len(topic2_events) == 1
        assert isinstance(topic2_events[0], AnotherDummyEvent)
        assert topic2_events[0].value == 42

    async def test_event_processing_pipeline(
        self,
        broker: InMemoryBroker[EventBase],
    ) -> None:
        """Test a pipeline of event processing."""
        publisher1 = InMemoryPublisher(broker)
        consumer1 = InMemoryConsumer(broker)
        publisher2 = InMemoryPublisher(broker)
        consumer2 = InMemoryConsumer(broker)

        topic1 = "input_topic"
        topic2 = "output_topic"

        async def process_and_republish() -> None:
            """Consume from topic1, process, and publish to topic2."""
            async for event in consumer1.consume(topic1):
                if isinstance(event, DummyEvent):
                    # Transform and republish
                    new_event = AnotherDummyEvent(value=len(event.message))
                    await publisher2.publish(topic2, new_event)
            # Signal completion on output topic
            await publisher2.publish(topic2, CompletedEvent(topic=topic2))

        async def publish_input() -> None:
            await asyncio.sleep(0.05)
            await publisher1.publish(topic1, DummyEvent(message="Hello"))
            await publisher1.publish(topic1, DummyEvent(message="World!"))
            await publisher1.publish(topic1, CompletedEvent(topic=topic1))

        async def consume_output() -> list[EventBase]:
            events = []
            async for event in consumer2.consume(topic2):
                events.append(event)
            return events

        # Run pipeline
        results = await asyncio.gather(publish_input(), process_and_republish(), consume_output())

        output_events = results[2]

        # Verify transformation
        assert len(output_events) == 2
        assert isinstance(output_events[0], AnotherDummyEvent)
        assert isinstance(output_events[1], AnotherDummyEvent)
        assert output_events[0].value == 5  # len("Hello")
        assert output_events[1].value == 6  # len("World!")

    async def test_high_throughput_events(
        self,
        broker: InMemoryBroker[EventBase],
        publisher: InMemoryPublisher[EventBase],
        consumer: InMemoryConsumer[EventBase],
        test_topic: str,
    ) -> None:
        """Test system with high volume of events."""
        num_events = 100

        async def publish_many_events() -> None:
            for i in range(num_events):
                await publisher.publish(test_topic, DummyEvent(message=f"Event {i}"))
            await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        async def consume_all_events() -> list[EventBase]:
            events = []
            async for event in consumer.consume(test_topic):
                events.append(event)
            return events

        publish_task = asyncio.create_task(publish_many_events())
        consume_task = asyncio.create_task(consume_all_events())

        await publish_task
        consumed_events = await consume_task

        # Verify all events were processed
        assert len(consumed_events) == num_events
        for i, event in enumerate(consumed_events):
            assert isinstance(event, DummyEvent)
            assert event.message == f"Event {i}"

    async def test_error_handling_in_consumer(
        self,
        broker: InMemoryBroker[EventBase],
        publisher: InMemoryPublisher[EventBase],
        test_topic: str,
    ) -> None:
        """Test that consumer can handle errors gracefully."""
        consumer = InMemoryConsumer(broker)

        async def publish_events() -> None:
            await asyncio.sleep(0.05)
            await publisher.publish(test_topic, DummyEvent(message="Valid event"))
            await publisher.publish(test_topic, CompletedEvent(topic=test_topic))

        async def consume_with_error_handling() -> tuple[list[EventBase], list[Exception]]:
            events = []
            errors = []
            try:
                async for event in consumer.consume(test_topic):
                    try:
                        # Simulate processing
                        events.append(event)
                    except Exception as e:
                        errors.append(e)
            except Exception as e:
                errors.append(e)
            return events, errors

        publish_task = asyncio.create_task(publish_events())
        consume_task = asyncio.create_task(consume_with_error_handling())

        await publish_task
        consumed_events, errors = await consume_task

        assert len(consumed_events) == 1
        assert len(errors) == 0
