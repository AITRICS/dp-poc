"""Multiprocess worker pool manager implementation."""

from __future__ import annotations

import asyncio
import contextlib
import multiprocessing as mp
import queue
from typing import TYPE_CHECKING, Any

from app.event_system.domain.events import EventBase, TaskResultEvent
from app.event_system.infrastructure.multiprocess_queue import MultiprocessQueue
from app.executor.domain.executable_task import (
    STOP_SENTINEL,
    ExecutableTask,
    _StopSentinel,
)
from app.executor.domain.task_result import TaskResult
from app.executor.domain.worker_pool_manager_port import WorkerPoolManagerPort
from app.executor.infrastructure.worker import worker_loop
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.task_registry import get_registry

if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue

    from app.event_system.domain.publisher_port import PublisherPort
    from app.io_manager.domain.io_manager_port import IOManagerPort
    from app.task_registry.infrastructure.task_registry import TaskRegistry


class MultiprocessPoolManager(WorkerPoolManagerPort):
    """Worker pool manager using multiprocessing.

    Manages a pool of worker processes that execute tasks in parallel.
    Uses multiprocessing queues for task distribution and result collection.

    Architecture:
    - Task Queue: Orchestrator submits tasks, workers pull from it
    - Result Queue: Workers push results, orchestrator polls from it
    - Worker Processes: Long-running processes executing tasks

    Example:
        ```python
        manager = MultiprocessPoolManager(num_workers=4)
        with manager:
            manager.submit_task(task)
            result_queue = manager.get_result_queue()
            result = result_queue.get()
        ```
    """

    def __init__(
        self,
        num_workers: int = 1,
        io_manager: IOManagerPort | None = None,
        registry: TaskRegistry | None = None,
        publisher: PublisherPort[EventBase] | None = None,
    ) -> None:
        """Initialize the multiprocess pool manager.

        Args:
            num_workers: Number of worker processes (default: 1)
            io_manager: I/O manager for task results (default: FilesystemIOManager)
            registry: Task registry for workers to look up tasks (default: global registry)
            publisher: Event publisher for sending TaskResultEvents (optional)

        Raises:
            ValueError: If num_workers < 1
        """
        if num_workers < 1:
            raise ValueError("num_workers must be at least 1")

        self.num_workers = num_workers
        self.io_manager = io_manager or FilesystemIOManager()
        self.registry = registry or get_registry()
        self.publisher = publisher

        # Create queues for IPC
        self.task_queue: MultiprocessQueue[ExecutableTask | _StopSentinel] = MultiprocessQueue()
        self.result_queue: MultiprocessQueue[TaskResult] = MultiprocessQueue()

        # Worker processes
        self.workers: list[mp.Process] = []

        # Result polling task
        self._result_polling_task: asyncio.Task[None] | None = None
        self._should_stop_polling = False

    def __enter__(self) -> MultiprocessPoolManager:
        """Start the worker pool (sync version).

        Returns:
            Self for context manager pattern
        """
        self._start_workers()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Stop the worker pool and cleanup resources (sync version).

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        # Stop result polling
        self._should_stop_polling = True
        if self._result_polling_task is not None:
            self._result_polling_task.cancel()

        self._stop_workers()
        self.task_queue.close()
        self.result_queue.close()

    async def __aenter__(self) -> MultiprocessPoolManager:
        """Start the worker pool (async version).

        Returns:
            Self for async context manager pattern
        """
        self._start_workers()
        # Start result polling if publisher is configured
        if self.publisher is not None:
            self._result_polling_task = asyncio.create_task(self._poll_results())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Stop the worker pool and cleanup resources (async version).

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        # Stop result polling
        self._should_stop_polling = True
        if self._result_polling_task is not None:
            self._result_polling_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._result_polling_task

        self._stop_workers()
        self.task_queue.close()
        self.result_queue.close()

    def submit_task(self, task: ExecutableTask) -> None:
        """Submit a task to the worker pool.

        Args:
            task: Executable task to be executed by a worker
        """
        self.task_queue.put_sync(task)

    def get_result_queue(self) -> MPQueue[TaskResult]:
        """Get the result queue for orchestrator to poll.

        Returns:
            Queue that workers put results into
        """
        return self.result_queue.get_underlying_queue()

    def _start_workers(self) -> None:
        """Start worker processes.

        Creates and starts num_workers worker processes, each running
        the worker_loop function.
        """
        # Get underlying queues for workers
        task_q = self.task_queue.get_underlying_queue()
        result_q = self.result_queue.get_underlying_queue()

        for worker_id in range(self.num_workers):
            process = mp.Process(
                target=worker_loop,
                args=(task_q, result_q, self.io_manager, self.registry, worker_id),
                name=f"Worker-{worker_id}",
            )
            process.start()
            self.workers.append(process)

    def _stop_workers(self) -> None:
        """Stop worker processes gracefully.

        Sends STOP_SENTINEL to each worker and waits for them to finish.
        """
        # Send stop sentinels
        for _ in range(self.num_workers):
            self.task_queue.put_sync(STOP_SENTINEL)

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5.0)
            if worker.is_alive():
                # Force terminate if still alive
                worker.terminate()
                worker.join()

        self.workers.clear()

    async def _poll_results(self) -> None:
        """Poll result queue and publish TaskResultEvents.

        This background task continuously polls the result queue and
        converts TaskResult objects to TaskResultEvent, then publishes them.
        """
        if self.publisher is None:
            return

        loop = asyncio.get_event_loop()
        result_q = self.result_queue.get_underlying_queue()

        while not self._should_stop_polling:
            try:
                # Non-blocking get with timeout
                result = await loop.run_in_executor(
                    None, lambda: result_q.get(block=True, timeout=0.1)
                )

                # Convert TaskResult to TaskResultEvent
                event = TaskResultEvent(
                    topic="task.result",
                    run_id=result.run_id,
                    task_name=result.task_name,
                    task_result_id=result.task_result_id,
                    status=result.status.name,
                    execution_time=result.execution_time,
                    retry_count=result.retry_count,
                    error_message=result.error_message,
                    error_type=result.error_type,
                    is_streaming=result.is_streaming,
                    stream_index=result.stream_index,
                    stream_complete=result.stream_complete,
                    metadata=result.metadata,
                )

                # Publish the event
                await self.publisher.publish("task.result", event)

            except queue.Empty:
                # No results available, continue polling
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                # Task cancelled, exit gracefully
                break
            except Exception as error:
                # Log error but continue polling
                import logging

                logger = logging.getLogger(__name__)
                logger.exception("Error polling results: %s", error)
                await asyncio.sleep(0.1)
