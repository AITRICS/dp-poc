"""Worker process implementation for task execution."""

from __future__ import annotations

import logging
import queue
from typing import TYPE_CHECKING

from app.executor.domain.executable_task import ExecutableTask, _StopSentinel
from app.executor.domain.task_result import TaskResult
from app.executor.infrastructure.task_executor import execute_task

if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue

    from app.io_manager.domain.io_manager_port import IOManagerPort
    from app.task_registry.infrastructure.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


def worker_loop(
    task_queue: MPQueue[ExecutableTask | _StopSentinel],
    result_queue: MPQueue[TaskResult],
    io_manager: IOManagerPort,
    registry: TaskRegistry,
    worker_id: int = 0,
) -> None:
    """Main loop for worker process.

    Continuously polls the task queue, executes tasks, and sends results back.
    Exits gracefully when STOP_SENTINEL is received.

    Args:
        task_queue: Queue to receive ExecutableTask objects from orchestrator
        result_queue: Queue to send TaskResult objects back to orchestrator
        io_manager: I/O manager for loading inputs and saving outputs
        registry: Task registry to look up task metadata by name
        worker_id: Identifier for this worker (for logging/debugging)
    """
    while True:
        try:
            # Block on queue with timeout for graceful shutdown check
            item = task_queue.get(timeout=1.0)

            # Check for stop sentinel
            if isinstance(item, _StopSentinel):
                break

            # Type narrowing: item is now ExecutableTask
            assert isinstance(item, ExecutableTask)

            # Look up task metadata from registry
            task_metadata = registry.get(item.task_name)
            if task_metadata is None:
                raise ValueError(f"Task '{item.task_name}' not found in registry")

            # Execute the task and get result(s)
            results = execute_task(item, task_metadata, io_manager)

            # Send all results back to orchestrator
            for result in results:
                result_queue.put(result)

        except queue.Empty:
            # Timeout occurred, loop back to check for stop sentinel
            continue

        except Exception as error:
            # Unexpected error in worker loop
            # Try to send error result if we have task info
            try:
                if "item" in locals() and hasattr(item, "run_id"):
                    error_result = TaskResult.failure(
                        run_id=item.run_id,
                        task_name=getattr(item, "task_name", "unknown"),
                        task_result_id=getattr(item, "task_result_id", "unknown"),
                        execution_time=0.0,
                        error=error,
                        retry_count=getattr(item, "retry_count", 0),
                    )
                    result_queue.put(error_result)
            except Exception:
                # If we can't even send error result, just log and continue
                logger.exception("Worker %s encountered fatal error: %s", worker_id, error)
                continue
