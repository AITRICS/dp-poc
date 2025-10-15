"""In-memory queue implementation using asyncio.Queue."""

import asyncio
from copy import deepcopy
from typing import Generic, TypeVar

from app.event_system.domain.queue_port import QueuePort

T = TypeVar("T")


class InMemoryQueue(QueuePort[T], Generic[T]):
    """
    An in-memory, asyncio-based implementation of the QueuePort.
    It uses asyncio.Queue as the underlying queue implementation.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """
        Initialize the in-memory queue.

        Args:
            maxsize: Maximum size of the queue. 0 means unlimited.
        """
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self._maxsize = maxsize

    async def put(self, item: T) -> None:
        """
        Put an item into the queue.

        Args:
            item: The event to put into the queue.
        """
        # if self._queue.full():
        #     queue_size = self._queue.qsize()
        #     if queue_size >= self._maxsize:
        #         self._maxsize = 2 * self._maxsize
        #         queue = self._queue
        #         self._queue = asyncio.Queue(maxsize=self._maxsize)
        #         for _ in range(queue_size):
        #             self._queue.put_nowait(queue.get_nowait())
        await self._queue.put(deepcopy(item))

    async def get(self) -> T:
        """
        Get an item from the queue.

        Returns:
            The next event from the queue.
        """
        return await self._queue.get()

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.

        Returns:
            The number of items in the queue.
        """
        return self._queue.qsize()

    def empty(self) -> bool:
        """
        Return True if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise.
        """
        return self._queue.empty()

    def full(self) -> bool:
        """
        Return True if the queue is full.

        Returns:
            True if there are maxsize items in the queue, False otherwise.
        """
        return self._queue.full()

    def task_done(self) -> None:
        """
        Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.
        """
        self._queue.task_done()

    async def join(self) -> None:
        """
        Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the queue.
        The count goes down whenever a consumer calls task_done() to indicate that
        the item was retrieved and all work on it is complete. When the count of
        unfinished tasks drops to zero, join() unblocks.
        """
        await self._queue.join()
