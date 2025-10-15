"""Tests for MultiprocessQueue."""

import multiprocessing as mp
import sys
from typing import Any

import pytest

from app.event_system.infrastructure.multiprocess_queue import MultiprocessQueue


# Module-level functions for multiprocess test (needed for pickling)
def _producer(q_underlying: Any) -> None:
    """Producer process."""
    for i in range(5):
        q_underlying.put(i)


def _consumer(q_underlying: Any, results: Any) -> None:
    """Consumer process."""
    for _ in range(5):
        results.append(q_underlying.get())


class TestMultiprocessQueueBasic:
    """Test basic queue operations."""

    def test_init(self) -> None:
        """Test queue initialization."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        assert queue.empty()
        # qsize() may return 0 on macOS (NotImplementedError handled)
        size = queue.qsize()
        assert isinstance(size, int)
        assert size >= 0

    def test_init_with_maxsize(self) -> None:
        """Test queue initialization with maxsize."""
        queue: MultiprocessQueue[int] = MultiprocessQueue(maxsize=10)
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_put_and_get_async(self) -> None:
        """Test async put and get operations."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()

        await queue.put(1)
        await queue.put(2)
        await queue.put(3)

        assert await queue.get() == 1
        assert await queue.get() == 2
        assert await queue.get() == 3

    def test_put_and_get_sync(self) -> None:
        """Test synchronous put and get operations."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()

        queue.put_sync(1)
        queue.put_sync(2)
        queue.put_sync(3)

        assert queue.get_sync() == 1
        assert queue.get_sync() == 2
        assert queue.get_sync() == 3

    @pytest.mark.asyncio
    async def test_put_get_with_different_types(self) -> None:
        """Test queue with different data types."""
        # String queue
        str_queue: MultiprocessQueue[str] = MultiprocessQueue()
        await str_queue.put("hello")
        await str_queue.put("world")
        assert await str_queue.get() == "hello"
        assert await str_queue.get() == "world"

        # Dict queue
        dict_queue: MultiprocessQueue[dict[str, Any]] = MultiprocessQueue()
        await dict_queue.put({"key": "value"})
        result = await dict_queue.get()
        assert result == {"key": "value"}

        # List queue
        list_queue: MultiprocessQueue[list[int]] = MultiprocessQueue()
        await list_queue.put([1, 2, 3])
        result_list = await list_queue.get()
        assert result_list == [1, 2, 3]


class TestMultiprocessQueueState:
    """Test queue state methods."""

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        """Test empty() method."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        assert queue.empty()

        await queue.put(1)
        assert not queue.empty()

        await queue.get()
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_qsize(self) -> None:
        """Test qsize() method (platform-dependent)."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()

        # On macOS, qsize() returns 0 (not supported)
        # On other platforms, it should work properly
        if sys.platform == "darwin":
            pytest.skip("qsize() not supported on macOS")

        assert queue.qsize() == 0

        await queue.put(1)
        assert queue.qsize() == 1

        await queue.put(2)
        assert queue.qsize() == 2

        await queue.get()
        assert queue.qsize() == 1

        await queue.get()
        assert queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_full(self) -> None:
        """Test full() method."""
        queue: MultiprocessQueue[int] = MultiprocessQueue(maxsize=2)
        assert not queue.full()

        await queue.put(1)
        assert not queue.full()

        await queue.put(2)
        assert queue.full()


class TestMultiprocessQueueCompat:
    """Test compatibility methods for QueuePort interface."""

    def test_task_done(self) -> None:
        """Test task_done() method (no-op for multiprocessing.Queue)."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        # Should not raise any exception
        queue.task_done()

    @pytest.mark.asyncio
    async def test_join(self) -> None:
        """Test join() method (no-op for multiprocessing.Queue)."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        # Should not block
        await queue.join()


class TestMultiprocessQueueNowait:
    """Test non-blocking operations."""

    def test_put_nowait(self) -> None:
        """Test put_nowait() method."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        queue.put_nowait(1)
        queue.put_nowait(2)

        assert queue.get_sync() == 1
        assert queue.get_sync() == 2

    def test_get_nowait(self) -> None:
        """Test get_nowait() method."""
        import time

        queue: MultiprocessQueue[int] = MultiprocessQueue()
        queue.put_sync(1)
        queue.put_sync(2)

        # Small delay to ensure items are available
        time.sleep(0.01)

        assert queue.get_nowait() == 1
        assert queue.get_nowait() == 2

    def test_get_nowait_empty_raises(self) -> None:
        """Test get_nowait() raises when queue is empty."""
        import queue as q

        mq: MultiprocessQueue[int] = MultiprocessQueue()
        with pytest.raises(q.Empty):
            mq.get_nowait()


class TestMultiprocessQueueProcessSafe:
    """Test process-safety of the queue."""

    def test_multiprocess_communication(self) -> None:
        """Test queue works across processes."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()

        # Use Manager for shared list
        with mp.Manager() as manager:
            results = manager.list()

            # Get underlying queue for passing to processes
            q_underlying = queue.get_underlying_queue()

            # Start producer
            p1 = mp.Process(target=_producer, args=(q_underlying,))
            # Start consumer
            p2 = mp.Process(target=_consumer, args=(q_underlying, results))

            p1.start()
            p2.start()

            p1.join(timeout=5)
            p2.join(timeout=5)

            # Check results
            assert list(results) == [0, 1, 2, 3, 4]


class TestMultiprocessQueueUnderlyingAccess:
    """Test access to underlying multiprocessing.Queue."""

    def test_get_underlying_queue(self) -> None:
        """Test get_underlying_queue() method."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        underlying = queue.get_underlying_queue()

        # Should be able to use underlying queue directly
        underlying.put(42)
        assert queue.get_sync() == 42

    def test_close_and_join_thread(self) -> None:
        """Test close() and join_thread() methods."""
        queue: MultiprocessQueue[int] = MultiprocessQueue()
        queue.put_sync(1)

        # Close should not raise
        queue.close()

        # Join thread should not raise
        queue.join_thread()


class TestMultiprocessQueueTypeCompatibility:
    """Test that MultiprocessQueue conforms to QueuePort interface."""

    @pytest.mark.asyncio
    async def test_conforms_to_queueport_interface(self) -> None:
        """Test that MultiprocessQueue implements QueuePort interface."""

        queue: MultiprocessQueue[int] = MultiprocessQueue()

        # Test all QueuePort methods are available
        await queue.put(1)
        value = await queue.get()
        assert value == 1

        size = queue.qsize()
        assert isinstance(size, int)
        assert size >= 0
        assert isinstance(queue.empty(), bool)
        assert isinstance(queue.full(), bool)

        queue.task_done()
        await queue.join()
