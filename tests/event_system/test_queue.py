"""Tests for InMemoryQueue."""

import asyncio

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_queue import InMemoryQueue
from tests.conftest import AnotherDummyEvent, DummyEvent


@pytest.mark.unit
class TestInMemoryQueue:
    """Test suite for InMemoryQueue."""

    async def test_queue_initialization(self) -> None:
        """Test that queue initializes correctly."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()
        assert queue is not None
        assert queue.empty()
        assert queue.qsize() == 0

    async def test_queue_with_maxsize(self) -> None:
        """Test that queue can be initialized with maxsize."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue(maxsize=5)
        assert queue is not None
        assert queue.qsize() == 0

    async def test_put_and_get_single_event(self) -> None:
        """Test putting and getting a single event."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()
        event = DummyEvent(message="Test message")

        await queue.put(event)
        assert queue.qsize() == 1
        assert not queue.empty()

        retrieved = await queue.get()
        assert isinstance(retrieved, DummyEvent)
        assert retrieved.message == "Test message"
        assert queue.empty()

    async def test_put_and_get_multiple_events(self) -> None:
        """Test putting and getting multiple events."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()

        event1 = DummyEvent(message="First")
        event2 = DummyEvent(message="Second")
        event3 = AnotherDummyEvent(value=42)

        await queue.put(event1)
        await queue.put(event2)
        await queue.put(event3)

        assert queue.qsize() == 3

        retrieved1 = await queue.get()
        retrieved2 = await queue.get()
        retrieved3 = await queue.get()

        assert isinstance(retrieved1, DummyEvent)
        assert retrieved1.message == "First"
        assert isinstance(retrieved2, DummyEvent)
        assert retrieved2.message == "Second"
        assert isinstance(retrieved3, AnotherDummyEvent)
        assert retrieved3.value == 42

    async def test_fifo_order(self) -> None:
        """Test that queue maintains FIFO order."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()

        for i in range(10):
            await queue.put(DummyEvent(message=f"Event {i}"))

        for i in range(10):
            event = await queue.get()
            assert isinstance(event, DummyEvent)
            assert event.message == f"Event {i}"

    async def test_empty_method(self) -> None:
        """Test empty() method."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()

        assert queue.empty()

        await queue.put(DummyEvent(message="Test"))
        assert not queue.empty()

        await queue.get()
        assert queue.empty()

    async def test_qsize_method(self) -> None:
        """Test qsize() method."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()

        assert queue.qsize() == 0

        for i in range(5):
            await queue.put(DummyEvent(message=f"Event {i}"))
            assert queue.qsize() == i + 1

        for i in range(5):
            await queue.get()
            assert queue.qsize() == 4 - i

    async def test_task_done_and_join(self) -> None:
        """Test task_done() and join() methods."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()

        # Add some items
        for i in range(3):
            await queue.put(DummyEvent(message=f"Event {i}"))

        async def worker() -> None:
            while not queue.empty():
                await queue.get()
                await asyncio.sleep(0.01)  # Simulate work
                queue.task_done()

        # Start worker
        worker_task = asyncio.create_task(worker())

        # Wait for all tasks to be done
        await queue.join()
        await worker_task

        assert queue.empty()

    async def test_concurrent_put_and_get(self) -> None:
        """Test concurrent put and get operations."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()
        num_items = 100

        async def producer() -> None:
            for i in range(num_items):
                await queue.put(DummyEvent(message=f"Event {i}"))
                await asyncio.sleep(0.001)

        async def consumer() -> list[EventBase]:
            events = []
            for _ in range(num_items):
                event = await queue.get()
                events.append(event)
                await asyncio.sleep(0.001)
            return events

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        await producer_task
        consumed_events = await consumer_task

        assert len(consumed_events) == num_items
        for i, event in enumerate(consumed_events):
            assert isinstance(event, DummyEvent)
            assert event.message == f"Event {i}"

    async def test_multiple_consumers(self) -> None:
        """Test multiple consumers consuming from the same queue."""
        queue: InMemoryQueue[EventBase] = InMemoryQueue()
        num_items = 20

        # Add items to queue
        for i in range(num_items):
            await queue.put(DummyEvent(message=f"Event {i}"))

        consumed_events1: list[EventBase] = []
        consumed_events2: list[EventBase] = []

        async def consumer1() -> None:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.1)
                    consumed_events1.append(event)
                except TimeoutError:
                    break

        async def consumer2() -> None:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.1)
                    consumed_events2.append(event)
                except TimeoutError:
                    break

        # Run consumers concurrently
        await asyncio.gather(consumer1(), consumer2())

        # Verify all items were consumed
        total_consumed = len(consumed_events1) + len(consumed_events2)
        assert total_consumed == num_items
        assert queue.empty()
