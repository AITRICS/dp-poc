"""
Performance benchmark tests for the event system.

Run with:
    uv run pytest tests/test_benchmark.py -v
    uv run pytest tests/test_benchmark.py -v --benchmark-only
    uv run pytest tests/test_benchmark.py -v --benchmark-compare
"""

import asyncio
from typing import Any

import pytest

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from tests.conftest import AnotherDummyEvent, DummyEvent

# Mark all tests in this module as benchmark tests
pytestmark = pytest.mark.benchmark


class TestEventSystemBenchmark:
    """Benchmark tests for event system performance."""

    def test_benchmark_small_scale(self, benchmark: Any) -> None:
        """
        Small scale benchmark: 10 topics, 100 events each, 1 consumer.
        Good for quick performance checks.
        """

        async def run_small_benchmark() -> int:
            broker: InMemoryBroker[EventBase] = InMemoryBroker()
            publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)
            consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

            num_topics = 10
            events_per_topic = 100
            topics = [f"test.topic.{i:03d}" for i in range(num_topics)]

            # Declare topics
            for topic in topics:
                await broker.declare_topic(topic)

            # Consumer counter
            event_count = 0

            async def consume_events() -> int:
                nonlocal event_count
                async for _ in consumer.consume_pattern("test.**"):
                    event_count += 1
                return event_count

            # Start consumer
            consumer_task = asyncio.create_task(consume_events())

            # Publish events
            for topic in topics:
                for i in range(events_per_topic):
                    event: DummyEvent | AnotherDummyEvent | None = None
                    if i % 2 == 0:
                        event = DummyEvent(topic=topic, message=f"msg_{i}")
                    else:
                        event = AnotherDummyEvent(topic=topic, value=i)
                    await publisher.publish(topic, event)

            # Send completion events
            for topic in topics:
                await publisher.publish(topic, CompletedEvent(topic=topic))

            # Wait for consumer
            await consumer_task

            return event_count

        # Run benchmark
        result = benchmark.pedantic(
            lambda: asyncio.run(run_small_benchmark()),
            iterations=5,
            rounds=3,
        )

        # Verify
        expected = 10 * 100
        assert result == expected

    def test_benchmark_medium_scale(self, benchmark: Any) -> None:
        """
        Medium scale benchmark: 50 topics, 500 events each, 2 consumers.
        Tests concurrent consumption.
        """

        async def run_medium_benchmark() -> tuple[int, int]:
            broker: InMemoryBroker[EventBase] = InMemoryBroker()
            publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)

            num_topics = 50
            events_per_topic = 500
            topics = [f"medium.topic.{i:03d}.{i % 2}" for i in range(num_topics)]

            # Declare topics
            for topic in topics:
                await broker.declare_topic(topic)

            # Consumers
            consumer1 = InMemoryConsumer(broker)
            consumer2 = InMemoryConsumer(broker)
            counts = [0, 0]
            locks = [asyncio.Lock(), asyncio.Lock()]

            async def consume_with_pattern(consumer_id: int, pattern: str) -> None:
                consumer = consumer1 if consumer_id == 0 else consumer2
                async for _ in consumer.consume_pattern(pattern):
                    async with locks[consumer_id]:
                        counts[consumer_id] += 1

            # Start consumers
            task1 = asyncio.create_task(consume_with_pattern(0, "medium.topic.*.0"))
            task2 = asyncio.create_task(consume_with_pattern(1, "medium.topic.*.1"))

            # Publish events
            for topic in topics:
                for i in range(events_per_topic):
                    event: DummyEvent | AnotherDummyEvent | None = None
                    if i % 2 == 0:
                        event = DummyEvent(topic=topic, message=f"msg_{i}")
                    else:
                        event = AnotherDummyEvent(topic=topic, value=i)
                    await publisher.publish(topic, event)

            # Send completion events
            for topic in topics:
                await publisher.publish(topic, CompletedEvent(topic=topic))

            # Wait for consumers
            await asyncio.gather(task1, task2)

            return counts[0], counts[1]

        # Run benchmark
        result = benchmark.pedantic(
            lambda: asyncio.run(run_medium_benchmark()),
            iterations=3,
            rounds=2,
        )

        # Verify (25 topics each, 500 events per topic)
        assert result[0] == 25 * 500
        assert result[1] == 25 * 500

    @pytest.mark.slow
    def test_benchmark_large_scale(self, benchmark: Any) -> None:
        """
        Large scale benchmark: 100 topics, 1000 events each, 3 consumers.
        This is a comprehensive performance test.

        Run only this test with:
            uv run pytest tests/test_benchmark.py::TestEventSystemBenchmark::test_benchmark_large_scale -v
        """

        async def run_large_benchmark() -> tuple[int, int, int]:
            broker: InMemoryBroker[EventBase] = InMemoryBroker()
            publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)

            num_topics = 100
            events_per_topic = 1000
            topics = [f"benchmark.topic.{i:03d}.{i % 3}" for i in range(num_topics)]

            # Declare topics
            for topic in topics:
                await broker.declare_topic(topic)

            # Consumers
            consumers = [InMemoryConsumer(broker) for _ in range(3)]
            counts = [0, 0, 0]
            locks = [asyncio.Lock() for _ in range(3)]

            async def consume_with_pattern(consumer_id: int) -> None:
                pattern = f"benchmark.topic.*.{consumer_id}"
                async for _ in consumers[consumer_id].consume_pattern(pattern):
                    async with locks[consumer_id]:
                        counts[consumer_id] += 1

            # Start consumers
            consumer_tasks = [asyncio.create_task(consume_with_pattern(i)) for i in range(3)]

            # Publish events (batched for better performance)
            publish_tasks = []
            for topic_idx, topic in enumerate(topics):
                for event_idx in range(events_per_topic):
                    event: DummyEvent | AnotherDummyEvent | None = None
                    if event_idx % 2 == 0:
                        event = DummyEvent(topic=topic, message=f"msg_{event_idx}")
                    else:
                        event = AnotherDummyEvent(topic=topic, value=event_idx)
                    publish_tasks.append(publisher.publish(topic, event))

                # Batch publish every 10 topics
                if (topic_idx + 1) % 10 == 0:
                    await asyncio.gather(*publish_tasks)
                    publish_tasks = []

            # Publish remaining
            if publish_tasks:
                await asyncio.gather(*publish_tasks)

            # Send completion events
            for topic in topics:
                await publisher.publish(topic, CompletedEvent(topic=topic))

            # Wait for consumers
            await asyncio.gather(*consumer_tasks)

            return counts[0], counts[1], counts[2]

        # Run benchmark (only 1 iteration due to large scale)
        result = benchmark.pedantic(
            lambda: asyncio.run(run_large_benchmark()),
            iterations=1,
            rounds=1,
        )

        # Verify (topics are distributed by i % 3)
        # Consumer 0: indices 0,3,6,...,99 = 34 topics
        # Consumer 1: indices 1,4,7,...,97 = 33 topics
        # Consumer 2: indices 2,5,8,...,98 = 33 topics
        expected_counts = [34000, 33000, 33000]  # 34, 33, 33 topics * 1000 events each
        for i, count in enumerate(result):
            assert count == expected_counts[i], (
                f"Consumer {i} count mismatch: got {count}, expected {expected_counts[i]}"
            )

    def test_benchmark_publishing_only(self, benchmark: Any) -> None:
        """
        Benchmark pure publishing performance without consumers.
        Measures how fast events can be published to queues.
        """

        async def run_publish_benchmark() -> int:
            broker: InMemoryBroker[EventBase] = InMemoryBroker()
            publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)

            num_topics = 20
            events_per_topic = 500
            topics = [f"publish.topic.{i:03d}" for i in range(num_topics)]

            # Declare topics
            for topic in topics:
                await broker.declare_topic(topic)

            # Publish events
            total_published = 0
            for topic in topics:
                for i in range(events_per_topic):
                    event = DummyEvent(topic=topic, message=f"msg_{i}")
                    await publisher.publish(topic, event)
                    total_published += 1

            return total_published

        # Run benchmark
        result = benchmark.pedantic(
            lambda: asyncio.run(run_publish_benchmark()),
            iterations=5,
            rounds=3,
        )

        assert result == 20 * 500

    def test_benchmark_pattern_matching(self, benchmark: Any) -> None:
        """
        Benchmark pattern matching performance.
        Tests how efficiently patterns are matched against topic names.
        """

        async def run_pattern_benchmark() -> int:
            broker: InMemoryBroker[EventBase] = InMemoryBroker()
            publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)
            consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

            # Create hierarchical topics
            topics = []
            for category in ["data", "analytics", "monitoring"]:
                for subcategory in ["pipeline", "warehouse", "stream"]:
                    for idx in range(10):
                        topics.append(f"{category}.{subcategory}.{idx:03d}")

            # Declare all topics (90 total)
            for topic in topics:
                await broker.declare_topic(topic)

            event_count = 0

            async def consume_events() -> int:
                nonlocal event_count
                # This pattern should match all data.* topics (30 topics)
                async for _ in consumer.consume_pattern("data.**"):
                    event_count += 1
                return event_count

            # Start consumer
            consumer_task = asyncio.create_task(consume_events())

            # Publish 10 events to each topic
            events_per_topic = 10
            for topic in topics:
                for i in range(events_per_topic):
                    event = DummyEvent(topic=topic, message=f"msg_{i}")
                    await publisher.publish(topic, event)

            # Send completion events
            for topic in topics:
                await publisher.publish(topic, CompletedEvent(topic=topic))

            await consumer_task

            return event_count

        # Run benchmark
        result = benchmark.pedantic(
            lambda: asyncio.run(run_pattern_benchmark()),
            iterations=3,
            rounds=2,
        )

        # Should consume from 30 data.* topics, 10 events each
        assert result == 30 * 10


def test_benchmark_topic_declaration(benchmark: Any) -> None:
    """
    Benchmark topic declaration performance.
    Tests how fast topics can be created.
    """

    async def run_declaration_benchmark() -> int:
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        num_topics = 1000

        for i in range(num_topics):
            await broker.declare_topic(f"declare.test.{i:04d}")

        return len(broker.get_topics())

    # Run benchmark
    result = benchmark.pedantic(
        lambda: asyncio.run(run_declaration_benchmark()),
        iterations=5,
        rounds=3,
    )

    assert result == 1000


def test_benchmark_concurrent_publishers(benchmark: Any) -> None:
    """
    Benchmark multiple publishers publishing to the same topic concurrently.
    Tests thread-safety and concurrent access performance.
    """

    async def run_concurrent_publish_benchmark() -> int:
        broker: InMemoryBroker[EventBase] = InMemoryBroker()
        num_publishers = 5
        events_per_publisher = 200

        publishers = [InMemoryPublisher(broker) for _ in range(num_publishers)]

        await broker.declare_topic("concurrent.test")

        async def publish_events(publisher: InMemoryPublisher[EventBase], pub_id: int) -> None:
            for i in range(events_per_publisher):
                event = DummyEvent(topic="concurrent.test", message=f"pub{pub_id}_msg{i}")
                await publisher.publish("concurrent.test", event)

        # Publish concurrently
        await asyncio.gather(*[publish_events(pub, i) for i, pub in enumerate(publishers)])

        queue = await broker.get_queue("concurrent.test")
        return queue.qsize()

    # Run benchmark
    result = benchmark.pedantic(
        lambda: asyncio.run(run_concurrent_publish_benchmark()),
        iterations=5,
        rounds=3,
    )

    assert result == 5 * 200
