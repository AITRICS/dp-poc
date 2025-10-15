"""I/O Manager port interface for task result persistence."""

from abc import ABC, abstractmethod
from typing import Any


class IOManagerPort(ABC):
    """
    Abstract port for managing task execution results.

    Provides interface for saving, loading, and checking existence of task results.
    Results are identified by run_id, task_name, and task_result_id.
    """

    @abstractmethod
    def save(self, task_name: str, run_id: str, task_result_id: str, data: Any) -> str:
        """
        Save task execution result.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.
            data: Result data to save (must be serializable).

        Returns:
            Storage location (path, URL, key, etc.) where data was saved.

        Raises:
            IOError: If save operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self, task_name: str, run_id: str, task_result_id: str) -> Any:
        """
        Load task execution result.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.

        Returns:
            Loaded result data.

        Raises:
            FileNotFoundError: If result does not exist.
            IOError: If load operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, task_name: str, run_id: str, task_result_id: str) -> bool:
        """
        Check if task result exists.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.

        Returns:
            True if result exists, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, task_name: str, run_id: str, task_result_id: str) -> bool:
        """
        Delete task execution result.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.

        Returns:
            True if deletion succeeded, False if result didn't exist.

        Raises:
            IOError: If delete operation fails.
        """
        raise NotImplementedError

    @abstractmethod
    def list_results(self, run_id: str, task_name: str | None = None) -> list[str]:
        """
        List all result IDs for a given run (optionally filtered by task name).

        Args:
            run_id: Unique identifier for the execution run.
            task_name: Optional task name filter.

        Returns:
            List of task_result_ids.
        """
        raise NotImplementedError

    @abstractmethod
    def clear_run(self, run_id: str) -> int:
        """
        Delete all results for a given run.

        Args:
            run_id: Unique identifier for the execution run.

        Returns:
            Number of results deleted.
        """
        raise NotImplementedError
