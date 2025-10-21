"""Worker pool manager port interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue

    from app.executor.domain.executable_task import ExecutableTask
    from app.executor.domain.task_result import TaskResult


class WorkerPoolManagerPort(ABC):
    """Abstract port for managing worker pool.

    This interface defines the contract for managing a pool of workers
    that execute tasks. It follows the context manager pattern for
    safe resource management.

    The manager is responsible for:
    - Starting and stopping worker processes
    - Providing task submission interface
    - Providing result queue for orchestrator to poll

    The orchestrator is responsible for:
    - Polling the result queue
    - Deciding which tasks to submit based on results
    """

    @abstractmethod
    def __enter__(self) -> WorkerPoolManagerPort:
        """Start the worker pool.

        Returns:
            Self for context manager pattern
        """
        raise NotImplementedError

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Stop the worker pool and cleanup resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        raise NotImplementedError

    @abstractmethod
    def submit_task(self, task: ExecutableTask) -> None:
        """Submit a task to the worker pool.

        Args:
            task: Executable task to be executed by a worker
        """
        raise NotImplementedError

    @abstractmethod
    def get_result_queue(self) -> MPQueue[TaskResult]:
        """Get the result queue for orchestrator to poll.

        Returns:
            Queue that workers put results into
        """
        raise NotImplementedError


