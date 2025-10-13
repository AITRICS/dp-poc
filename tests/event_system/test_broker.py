"""Tests for InMemoryBroker."""

import asyncio

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.domain.queue_port import QueuePort
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_queue import InMemoryQueue
from tests.conftest import DummyEvent


@pytest.mark.unit
class TestInMemoryBroker:
    """Test suite for InMemoryBroker."""

    async def test_broker_initialization(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test that broker initializes correctly."""
        assert broker is not None
        assert isinstance(broker.queues, dict)

    async def test_get_queue_creates_new_queue(
        self, broker: InMemoryBroker[EventBase], test_topic: str
    ) -> None:
        """Test that get_queue creates a new queue if it doesn't exist."""
        queue = await broker.get_queue(test_topic)
        assert queue is not None
        assert isinstance(queue, QueuePort)
        assert isinstance(queue, InMemoryQueue)
        assert test_topic in broker.queues

    async def test_get_queue_returns_existing_queue(
        self, broker: InMemoryBroker[EventBase], test_topic: str
    ) -> None:
        """Test that get_queue returns the same queue for the same topic."""
        queue1 = await broker.get_queue(test_topic)
        queue2 = await broker.get_queue(test_topic)
        assert queue1 is queue2

    async def test_queue_factory(self) -> None:
        """Test that broker uses the provided queue factory."""
        custom_queue_created = False

        def custom_factory() -> QueuePort[EventBase]:
            nonlocal custom_queue_created
            custom_queue_created = True
            return InMemoryQueue[EventBase]()

        broker: InMemoryBroker[EventBase] = InMemoryBroker(queue_factory=custom_factory)
        await broker.get_queue("test_topic")
        assert custom_queue_created

    async def test_close_waits_for_all_queues(self, broker: InMemoryBroker[EventBase]) -> None:
        """Test that close waits for all queues to be processed."""
        topic1 = "topic1"
        topic2 = "topic2"

        queue1 = await broker.get_queue(topic1)
        queue2 = await broker.get_queue(topic2)
        event1 = DummyEvent(message="item1")
        event2 = DummyEvent(message="item2")
        # Add some items to the queues
        await queue1.put(event1)
        await queue2.put(event2)

        # Create a task that consumes items
        async def consume_and_mark_done() -> None:
            await asyncio.sleep(0.1)
            await queue1.get()
            queue1.task_done()
            await queue2.get()
            queue2.task_done()

        consume_task = asyncio.create_task(consume_and_mark_done())

        # Close should wait for all queues to be empty
        await broker.close()
        await consume_task

        # Verify all items were processed
        assert queue1.empty()
        assert queue2.empty()
