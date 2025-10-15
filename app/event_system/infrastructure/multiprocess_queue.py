"""Multiprocess-safe queue implementation using multiprocessing.Queue."""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from typing import TYPE_CHECKING, Generic, TypeVar

from app.event_system.domain.queue_port import QueuePort

if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue

T = TypeVar("T")


class MultiprocessQueue(QueuePort[T], Generic[T]):
    """
    A process-safe queue for inter-process communication.

    This implementation wraps multiprocessing.Queue and conforms to QueuePort interface.
    The async methods are wrappers around synchronous operations for interface compatibility.

    Note: This queue uses multiprocessing.Queue internally, which is synchronous.
    The async methods execute in the default executor to maintain compatibility with QueuePort.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """
        Initialize the multiprocess queue.

        Args:
            maxsize: Maximum size of the queue. 0 means unlimited.
        """
        self._queue: mp.Queue[T] = mp.Queue(maxsize=maxsize)
        self._maxsize = maxsize

    async def put(self, item: T) -> None:
        """
        Put an item into the queue (async interface for QueuePort).

        Args:
            item: The item to put into the queue (must be picklable).
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._queue.put, item)

    async def get(self) -> T:
        """
        Get an item from the queue (async interface for QueuePort).

        Returns:
            The next item from the queue.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._queue.get)

    def put_sync(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        """
        Put an item into the queue.

        Args:
            item: The item to put into the queue (must be picklable).
            block: If True, block if necessary until a free slot is available.
            timeout: If block is True, timeout after this many seconds.

        Raises:
            queue.Full: If queue is full and timeout expires.
        """
        self._queue.put(item, block=block, timeout=timeout)

    def get_sync(self, block: bool = True, timeout: float | None = None) -> T:
        """
        Get an item from the queue.

        Args:
            block: If True, block if necessary until an item is available.
            timeout: If block is True, timeout after this many seconds.

        Returns:
            The next item from the queue.

        Raises:
            queue.Empty: If queue is empty and timeout expires.
        """
        return self._queue.get(block=block, timeout=timeout)

    def put_nowait(self, item: T) -> None:
        """
        Put an item into the queue without blocking.

        Args:
            item: The item to put into the queue.

        Raises:
            queue.Full: If queue is full.
        """
        self._queue.put_nowait(item)

    def get_nowait(self) -> T:
        """
        Get an item from the queue without blocking.

        Returns:
            The next item from the queue.

        Raises:
            queue.Empty: If queue is empty.
        """
        return self._queue.get_nowait()

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.

        Note: On macOS, this may raise NotImplementedError and return 0.

        Returns:
            The approximate number of items in the queue, or 0 if not supported.
        """
        try:
            return self._queue.qsize()
        except NotImplementedError:
            # macOS doesn't support qsize() due to broken sem_getvalue()
            return 0

    def task_done(self) -> None:
        """
        Indicate that a formerly enqueued task is complete.

        Note: multiprocessing.Queue does not have task_done() method.
        This is a no-op for compatibility with QueuePort interface.
        """
        pass

    async def join(self) -> None:
        """
        Block until all items in the queue have been gotten and processed.

        Note: multiprocessing.Queue does not have join() method.
        This is a no-op for compatibility with QueuePort interface.
        """
        pass

    def empty(self) -> bool:
        """
        Return True if the queue is empty.

        Note: This is not reliable in multiprocess context (race conditions).

        Returns:
            True if the queue is empty, False otherwise.
        """
        return self._queue.empty()

    def full(self) -> bool:
        """
        Return True if the queue is full.

        Note: This is not reliable in multiprocess context (race conditions).

        Returns:
            True if there are maxsize items in the queue, False otherwise.
        """
        return self._queue.full()

    def close(self) -> None:
        """
        Indicate that no more data will be put on this queue.

        The background thread will quit once it has flushed all buffered data.
        This is called automatically when the queue is garbage collected.
        """
        self._queue.close()

    def join_thread(self) -> None:
        """
        Join the background thread.

        This can only be called after close() has been called. It blocks until
        the background thread exits, ensuring all data in the buffer has been flushed.
        """
        self._queue.join_thread()

    def cancel_join_thread(self) -> None:
        """
        Prevent join_thread() from blocking.

        In particular, this prevents the background thread from being joined automatically
        when the process exits. A better name for this method would be allow_exit_without_flush().
        """
        self._queue.cancel_join_thread()

    def get_underlying_queue(self) -> MPQueue[T]:
        """
        Get the underlying multiprocessing.Queue for advanced use.

        Returns:
            The underlying multiprocessing.Queue instance.
        """
        return self._queue
