"""Filesystem-based I/O Manager implementation."""

import logging
import pickle
import shutil
from pathlib import Path
from typing import Any

from app.io_manager.domain.io_manager_port import IOManagerPort

logger = logging.getLogger(__name__)


class FilesystemIOManager(IOManagerPort):
    """
    Filesystem-based implementation of IOManagerPort.

    Stores task results as pickle files in a directory structure:
    {base_path}/{run_id}/{task_name}/{task_result_id}.pkl
    """

    def __init__(self, base_path: str | Path = "./.dp-poc/runs") -> None:
        """
        Initialize filesystem I/O manager.

        Args:
            base_path: Base directory for storing results (default: ~/.dp-poc/runs).
        """
        self.base_path = Path(base_path).expanduser().resolve()
        logger.info(f"Initialized FilesystemIOManager with base_path={self.base_path}")

    def _get_result_path(self, task_name: str, run_id: str, task_result_id: str) -> Path:
        """
        Get the filesystem path for a task result.

        Args:
            task_name: Name of the task.
            run_id: Run identifier.
            task_result_id: Task result identifier.

        Returns:
            Path object for the result file.
        """
        return self.base_path / run_id / task_name / f"{task_result_id}.pkl"

    def _get_task_dir(self, task_name: str, run_id: str) -> Path:
        """
        Get the directory path for a task's results.

        Args:
            task_name: Name of the task.
            run_id: Run identifier.

        Returns:
            Path object for the task directory.
        """
        return self.base_path / run_id / task_name

    def _get_run_dir(self, run_id: str) -> Path:
        """
        Get the directory path for a run's results.

        Args:
            run_id: Run identifier.

        Returns:
            Path object for the run directory.
        """
        return self.base_path / run_id

    def save(self, task_name: str, run_id: str, task_result_id: str, data: Any) -> str:
        """
        Save task execution result to filesystem.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.
            data: Result data to save (must be picklable).

        Returns:
            Absolute path to the saved file.

        Raises:
            IOError: If save operation fails.
            TypeError: If data is not picklable.
        """
        result_path = self._get_result_path(task_name, run_id, task_result_id)

        try:
            # Create parent directories
            result_path.parent.mkdir(parents=True, exist_ok=True)

            # Save data using pickle
            with result_path.open("wb") as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

            logger.debug(
                f"Saved result: task={task_name}, run={run_id}, "
                f"result_id={task_result_id}, path={result_path}"
            )
            return str(result_path)

        except OSError as e:
            logger.error(f"Failed to save result to {result_path}: {e}")
            raise OSError(f"Failed to save result: {e}") from e
        except (pickle.PicklingError, TypeError) as e:
            logger.error(f"Failed to pickle data for {task_name}: {e}")
            raise TypeError(f"Data is not picklable: {e}") from e

    def load(self, task_name: str, run_id: str, task_result_id: str) -> Any:
        """
        Load task execution result from filesystem.

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
        result_path = self._get_result_path(task_name, run_id, task_result_id)

        if not result_path.exists():
            raise FileNotFoundError(
                f"Result not found: task={task_name}, run={run_id}, result_id={task_result_id}"
            )

        try:
            with result_path.open("rb") as f:
                data = pickle.load(f)

            logger.debug(
                f"Loaded result: task={task_name}, run={run_id}, result_id={task_result_id}"
            )
            return data

        except OSError as e:
            logger.error(f"Failed to load result from {result_path}: {e}")
            raise OSError(f"Failed to load result: {e}") from e
        except pickle.UnpicklingError as e:
            logger.error(f"Failed to unpickle data from {result_path}: {e}")
            raise OSError(f"Failed to unpickle data: {e}") from e

    def exists(self, task_name: str, run_id: str, task_result_id: str) -> bool:
        """
        Check if task result exists on filesystem.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.

        Returns:
            True if result file exists, False otherwise.
        """
        result_path = self._get_result_path(task_name, run_id, task_result_id)
        return result_path.exists() and result_path.is_file()

    def delete(self, task_name: str, run_id: str, task_result_id: str) -> bool:
        """
        Delete task execution result from filesystem.

        Args:
            task_name: Name of the task that produced the result.
            run_id: Unique identifier for the execution run.
            task_result_id: Unique identifier for this specific task result.

        Returns:
            True if deletion succeeded, False if result didn't exist.

        Raises:
            IOError: If delete operation fails.
        """
        result_path = self._get_result_path(task_name, run_id, task_result_id)

        if not result_path.exists():
            return False

        try:
            result_path.unlink()
            logger.debug(
                f"Deleted result: task={task_name}, run={run_id}, result_id={task_result_id}"
            )
            return True

        except OSError as e:
            logger.error(f"Failed to delete result at {result_path}: {e}")
            raise OSError(f"Failed to delete result: {e}") from e

    def list_results(self, run_id: str, task_name: str | None = None) -> list[str]:
        """
        List all result IDs for a given run (optionally filtered by task name).

        Args:
            run_id: Unique identifier for the execution run.
            task_name: Optional task name filter.

        Returns:
            List of task_result_ids.
        """
        results: list[str] = []

        if task_name:
            # List results for specific task
            task_dir = self._get_task_dir(task_name, run_id)
            if task_dir.exists() and task_dir.is_dir():
                for pkl_file in task_dir.glob("*.pkl"):
                    # Remove .pkl extension to get task_result_id
                    results.append(pkl_file.stem)
        else:
            # List results for all tasks in run
            run_dir = self._get_run_dir(run_id)
            if run_dir.exists() and run_dir.is_dir():
                for task_dir in run_dir.iterdir():
                    if task_dir.is_dir():
                        for pkl_file in task_dir.glob("*.pkl"):
                            results.append(pkl_file.stem)

        return sorted(results)

    def clear_run(self, run_id: str) -> int:
        """
        Delete all results for a given run.

        Args:
            run_id: Unique identifier for the execution run.

        Returns:
            Number of results deleted.
        """
        run_dir = self._get_run_dir(run_id)

        if not run_dir.exists():
            return 0

        # Count files before deletion
        count = sum(1 for _ in run_dir.rglob("*.pkl"))

        try:
            shutil.rmtree(run_dir)
            logger.info(f"Cleared run directory: {run_dir} ({count} results)")
            return count

        except OSError as e:
            logger.error(f"Failed to clear run directory {run_dir}: {e}")
            raise OSError(f"Failed to clear run: {e}") from e
