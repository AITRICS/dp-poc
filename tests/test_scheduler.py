"""Tests for Scheduler implementation."""

import asyncio
import contextlib

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.scheduler import Scheduler, create_cron_trigger, create_manual_trigger


# Test events
class SchedulerTestEvent(EventBase):
    """Test event for scheduler."""

    def __init__(self, topic: str, message: str, **kwargs: str) -> None:
        super().__init__(topic=topic, message=message, **kwargs)
        self.message = message


@pytest.fixture
async def event_system() -> tuple[
    InMemoryBroker[SchedulerTestEvent],
    InMemoryPublisher[SchedulerTestEvent],
    InMemoryConsumer[SchedulerTestEvent],
]:
    """Create event system components."""
    broker: InMemoryBroker[SchedulerTestEvent] = InMemoryBroker()
    publisher: InMemoryPublisher[SchedulerTestEvent] = InMemoryPublisher(broker)
    consumer: InMemoryConsumer[SchedulerTestEvent] = InMemoryConsumer(broker)
    return broker, publisher, consumer


@pytest.mark.asyncio
class TestSchedulerBasic:
    """Basic scheduler functionality tests."""

    async def test_scheduler_creation(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler creation."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        assert not scheduler.is_running()
        assert len(scheduler.get_registered_triggers()) == 0

    async def test_scheduler_start_stop(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler start and stop."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await scheduler.start()
        assert scheduler.is_running()

        await scheduler.stop()
        assert not scheduler.is_running()

    async def test_scheduler_register_trigger(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test registering triggers."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        trigger = create_manual_trigger(
            event_factory=lambda payload: SchedulerTestEvent(
                topic="test", message=payload.get("message", "")
            )
        )

        await scheduler.register_trigger("test_trigger", trigger, "test.topic")

        registered = scheduler.get_registered_triggers()
        assert len(registered) == 1
        assert "test_trigger" in registered
        # registered is dict[str, tuple[TriggerPort, str]]
        _, topic = registered["test_trigger"]
        assert topic == "test.topic"

    async def test_scheduler_unregister_trigger(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test unregistering triggers."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        trigger = create_manual_trigger(
            event_factory=lambda payload: SchedulerTestEvent(topic="test", message="")
        )

        await scheduler.register_trigger("test_trigger", trigger, "test.topic")
        assert len(scheduler.get_registered_triggers()) == 1

        await scheduler.unregister_trigger("test_trigger")
        assert len(scheduler.get_registered_triggers()) == 0


@pytest.mark.asyncio
class TestSchedulerExecution:
    """Test scheduler event execution."""

    async def test_scheduler_publishes_events(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that scheduler publishes events from triggers."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("test.topic")

        trigger = create_manual_trigger(
            event_factory=lambda payload: SchedulerTestEvent(
                topic="test.topic", message=payload.get("message", "")
            )
        )

        await scheduler.register_trigger("test_trigger", trigger, "test.topic")
        await scheduler.start()

        # Trigger an event
        await trigger.trigger({"message": "hello"})

        # Wait a bit for processing
        await asyncio.sleep(0.1)

        # Consume the published event
        async for event in consumer.consume("test.topic"):
            assert isinstance(event, SchedulerTestEvent)
            assert event.message == "hello"
            break

        await scheduler.stop()

    async def test_scheduler_multiple_triggers(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler with multiple triggers."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("topic1")
        await broker.declare_topic("topic2")

        trigger1 = create_manual_trigger(
            event_factory=lambda payload: SchedulerTestEvent(
                topic="topic1", message="from_trigger1"
            )
        )

        trigger2 = create_manual_trigger(
            event_factory=lambda payload: SchedulerTestEvent(
                topic="topic2", message="from_trigger2"
            )
        )

        await scheduler.register_trigger("trigger1", trigger1, "topic1")
        await scheduler.register_trigger("trigger2", trigger2, "topic2")
        await scheduler.start()

        assert len(scheduler.get_registered_triggers()) == 2

        # Trigger both
        await trigger1.trigger({})
        await trigger2.trigger({})

        # Wait for processing
        await asyncio.sleep(0.1)

        # Consume from topic1
        async for event in consumer.consume("topic1"):
            assert event.message == "from_trigger1"
            break

        # Consume from topic2
        async for event in consumer.consume("topic2"):
            assert event.message == "from_trigger2"
            break

        await scheduler.stop()

    @pytest.mark.slow
    async def test_scheduler_cron_trigger(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler with cron trigger."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("cron.topic")

        event_count = 0

        def event_factory() -> SchedulerTestEvent:
            nonlocal event_count
            event_count += 1
            return SchedulerTestEvent(topic="cron.topic", message=f"cron_{event_count}")

        cron_trigger = create_cron_trigger(
            cron_expression="* * * * * *",  # Every second
            event_factory=event_factory,
        )

        await scheduler.register_trigger("cron_trigger", cron_trigger, "cron.topic")
        await scheduler.start()

        # Collect events for ~3 seconds
        collected = []
        start_time = asyncio.get_event_loop().time()

        async for event in consumer.consume("cron.topic"):
            collected.append(event)
            if asyncio.get_event_loop().time() - start_time > 3:
                break

        await scheduler.stop()

        # Should have collected 2-4 events in 3 seconds
        assert 2 <= len(collected) <= 4
        for event in collected:
            assert isinstance(event, SchedulerTestEvent)
            assert event.message.startswith("cron_")


@pytest.mark.asyncio
class TestSchedulerLifecycle:
    """Test scheduler lifecycle management."""

    async def test_scheduler_start_with_triggers(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that starting scheduler starts all triggers."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        trigger1 = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="t1", message="")
        )
        trigger2 = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="t2", message="")
        )

        await scheduler.register_trigger("trigger1", trigger1, "topic1")
        await scheduler.register_trigger("trigger2", trigger2, "topic2")

        await scheduler.start()

        assert trigger1.is_running()
        assert trigger2.is_running()

        await scheduler.stop()

        assert not trigger1.is_running()
        assert not trigger2.is_running()

    async def test_scheduler_stop_cleans_up_tasks(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that stopping scheduler cleans up all tasks."""
        broker, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("test.topic")

        trigger = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="test.topic", message="")
        )

        await scheduler.register_trigger("trigger", trigger, "test.topic")
        await scheduler.start()

        # Trigger some events
        for i in range(5):
            await trigger.trigger({"message": f"msg_{i}"})

        await asyncio.sleep(0.1)

        # Stop should clean up
        await scheduler.stop()

        assert not scheduler.is_running()
        assert not trigger.is_running()

    async def test_scheduler_register_while_running(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test registering trigger while scheduler is running."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("topic1")
        await broker.declare_topic("topic2")

        trigger1 = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="topic1", message="first")
        )

        await scheduler.register_trigger("trigger1", trigger1, "topic1")
        await scheduler.start()

        # Register another trigger while running
        trigger2 = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="topic2", message="second")
        )
        await scheduler.register_trigger("trigger2", trigger2, "topic2")

        # Both should work
        await trigger1.trigger({})
        await trigger2.trigger({})

        await asyncio.sleep(0.1)

        # Check both events were published
        async for event in consumer.consume("topic1"):
            assert event.message == "first"
            break

        async for event in consumer.consume("topic2"):
            assert event.message == "second"
            break

        await scheduler.stop()

    async def test_scheduler_unregister_while_running(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test unregistering trigger while scheduler is running."""
        broker, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("test.topic")

        trigger = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(topic="test.topic", message="")
        )

        await scheduler.register_trigger("trigger", trigger, "test.topic")
        await scheduler.start()

        assert trigger.is_running()

        # Unregister while running
        await scheduler.unregister_trigger("trigger")

        assert not trigger.is_running()
        assert len(scheduler.get_registered_triggers()) == 0

        await scheduler.stop()


@pytest.mark.asyncio
class TestSchedulerErrorHandling:
    """Test scheduler error handling."""

    async def test_scheduler_handles_trigger_exception(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that scheduler handles exceptions from triggers gracefully."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("test.topic")

        call_count = 0

        def failing_factory(payload: dict[str, str]) -> SchedulerTestEvent:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Simulated error")
            return SchedulerTestEvent(topic="test.topic", message=payload.get("message", ""))

        trigger = create_manual_trigger(event_factory=failing_factory)

        await scheduler.register_trigger("trigger", trigger, "test.topic")
        await scheduler.start()

        # First trigger should fail (exception is logged but not raised)
        with contextlib.suppress(ValueError):  # Expected to fail
            await trigger.trigger({"message": "first"})

        # Second trigger should succeed
        await trigger.trigger({"message": "second"})

        await asyncio.sleep(0.1)

        # Should still receive the second event
        async for event in consumer.consume("test.topic"):
            if isinstance(event, SchedulerTestEvent):
                assert event.message == "second"
                break

        await scheduler.stop()

    async def test_scheduler_unregister_nonexistent(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test unregistering non-existent trigger."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        # Should raise KeyError
        with pytest.raises(KeyError, match="not found"):
            await scheduler.unregister_trigger("nonexistent")

    async def test_scheduler_double_start(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that double start is handled gracefully."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await scheduler.start()
        await scheduler.start()  # Should be idempotent

        assert scheduler.is_running()

        await scheduler.stop()

    async def test_scheduler_double_stop(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test that double stop is handled gracefully."""
        _, publisher, _ = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await scheduler.start()
        await scheduler.stop()
        await scheduler.stop()  # Should be idempotent

        assert not scheduler.is_running()


@pytest.mark.asyncio
class TestSchedulerConcurrency:
    """Test scheduler concurrency."""

    async def test_scheduler_concurrent_triggers(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler with concurrent triggers."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        # Create multiple topics
        for i in range(10):
            await broker.declare_topic(f"topic.{i}")

        # Create multiple triggers
        triggers = []
        for i in range(10):
            topic = f"topic.{i}"
            message = f"msg_{i}"
            trigger = create_manual_trigger(
                event_factory=lambda p, topic=topic, message=message: SchedulerTestEvent(  # type: ignore[misc]
                    topic=topic, message=message
                )
            )
            triggers.append(trigger)
            await scheduler.register_trigger(f"trigger_{i}", trigger, f"topic.{i}")

        await scheduler.start()

        # Trigger all simultaneously
        await asyncio.gather(*[trigger.trigger({}) for trigger in triggers])

        await asyncio.sleep(0.2)

        # Verify all events were published
        for idx in range(10):
            async for event in consumer.consume(f"topic.{idx}"):
                if isinstance(event, SchedulerTestEvent):
                    assert event.message == f"msg_{idx}"
                    break

        await scheduler.stop()

    async def test_scheduler_high_frequency_triggers(
        self,
        event_system: tuple[
            InMemoryBroker[SchedulerTestEvent],
            InMemoryPublisher[SchedulerTestEvent],
            InMemoryConsumer[SchedulerTestEvent],
        ],
    ) -> None:
        """Test scheduler with high-frequency triggers."""
        broker, publisher, consumer = event_system
        scheduler: Scheduler[SchedulerTestEvent] = Scheduler(publisher)

        await broker.declare_topic("high.frequency")

        trigger = create_manual_trigger(
            event_factory=lambda p: SchedulerTestEvent(
                topic="high.frequency", message=p.get("msg", "")
            )
        )

        await scheduler.register_trigger("trigger", trigger, "high.frequency")
        await scheduler.start()

        # Trigger 100 events rapidly
        for i in range(100):
            await trigger.trigger({"msg": f"msg_{i}"})

        await asyncio.sleep(0.5)

        # Consume and count events
        count = 0
        async for event in consumer.consume("high.frequency"):
            if isinstance(event, SchedulerTestEvent):
                count += 1
                if count >= 100:
                    break

        assert count == 100

        await scheduler.stop()
