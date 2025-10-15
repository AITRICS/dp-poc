"""Multiprocess executor for parallel task execution."""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from typing import TYPE_CHECKING

from app.event_system.infrastructure.multiprocess_queue import MultiprocessQueue
from app.executor.domain.executable_task import (
    STOP_SENTINEL,
    ExecutableTask,
    _StopSentinel,
)
from app.executor.domain.execution_result import ExecutionResult
from app.executor.domain.task_result import TaskResult
from app.executor.infrastructure.orchestrator import Orchestrator
from app.executor.infrastructure.worker import worker_loop
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.task_registry import get_registry

if TYPE_CHECKING:
    from app.executor.domain.execution_context import ExecutionContext
    from app.io_manager.domain.io_manager_port import IOManagerPort
    from app.task_registry.infrastructure.task_registry import TaskRegistry


class MultiprocessExecutor:
    """Executor that runs tasks in parallel using multiple worker processes.

    This executor follows a Queue-based Pull Model where:
    - Worker processes continuously poll a task queue
    - The orchestrator (main process) submits tasks and processes results
    - Inter-process communication uses MultiprocessQueue
    - Task results are stored via I/O Manager (not sent through queues)

    Architecture:
    - Main Process: Orchestrator coordinates execution
    - Worker Pool: Long-running processes execute tasks
    - Task Queue: Workers pull tasks (blocking wait)
    - Result Queue: Workers push results back

    Example:
        ```python
        executor = MultiprocessExecutor(num_workers=4)
        result = await executor.run_async(execution_context)

        if result.is_successful:
            print(f"Completed {len(result.completed_tasks)} tasks")
        ```
    """

    def __init__(
        self,
        num_workers: int = 1,
        io_manager: IOManagerPort | None = None,
        registry: TaskRegistry | None = None,
    ) -> None:
        """Initialize the multiprocess executor.

        Args:
            num_workers: Number of worker processes (default: 1)
            io_manager: I/O manager for task results (default: FilesystemIOManager)
            registry: Task registry for workers to look up tasks (default: global registry)

        Raises:
            ValueError: If num_workers < 1
        """
        if num_workers < 1:
            raise ValueError("num_workers must be at least 1")

        self.num_workers = num_workers
        self.io_manager = io_manager or FilesystemIOManager()
        self.registry = registry or get_registry()

        # Create queues for IPC
        self.task_queue: MultiprocessQueue[ExecutableTask | _StopSentinel] = MultiprocessQueue()
        self.result_queue: MultiprocessQueue[TaskResult] = MultiprocessQueue()

        # Worker processes
        self.workers: list[mp.Process] = []

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

    def run(self, execution_context: ExecutionContext) -> ExecutionResult:
        """Execute a DAG synchronously.

        This is a synchronous wrapper around run_async() for convenience.

        Args:
            execution_context: Execution context with DAG and metadata

        Returns:
            ExecutionResult with final status and task results

        Example:
            ```python
            executor = MultiprocessExecutor(num_workers=4)
            result = executor.run(execution_context)
            ```
        """
        return asyncio.run(self.run_async(execution_context))

    async def run_async(
        self,
        execution_context: ExecutionContext,
    ) -> ExecutionResult:
        """Execute a DAG asynchronously.

        Args:
            execution_context: Execution context with DAG and metadata

        Returns:
            ExecutionResult with final status and task results

        Raises:
            Exception: If orchestration fails

        Example:
            ```python
            executor = MultiprocessExecutor(num_workers=4)
            result = await executor.run_async(execution_context)
            ```
        """
        try:
            # Start worker processes
            self._start_workers()

            # Create orchestrator and run
            orchestrator = Orchestrator(
                context=execution_context,
                task_queue=self.task_queue.get_underlying_queue(),
                result_queue=self.result_queue.get_underlying_queue(),
            )

            # Run orchestration
            result = await orchestrator.orchestrate()

            return result

        finally:
            # Always clean up workers
            self._stop_workers()

            # Close queues
            self.task_queue.close()
            self.result_queue.close()
