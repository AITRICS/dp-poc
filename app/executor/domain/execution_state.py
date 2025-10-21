"""Execution state for tracking DAG execution progress."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class ExecutionState:
    """State tracking for a single DAG execution.

    This class maintains only the runtime state for a DAG execution.
    The ExecutionPlan (DAG structure) is held by the Orchestrator, not here.

    Attributes:
        run_id: Unique identifier for this execution
        dag_id: DAG identifier
        completed_tasks: Set of task names that completed successfully
        failed_tasks: Set of task names that failed
        in_progress_tasks: Set of task names currently being executed
        task_result_ids: Map of task names to their latest result ID
        start_time: Timestamp when execution started
    """

    run_id: str
    dag_id: str
    completed_tasks: set[str] = field(default_factory=set)
    failed_tasks: set[str] = field(default_factory=set)
    in_progress_tasks: set[str] = field(default_factory=set)
    task_result_ids: dict[str, str] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    def get_execution_time(self) -> float:
        """Get total execution time so far.

        Returns:
            Time elapsed since start in seconds
        """
        return time.time() - self.start_time

    def mark_task_completed(self, task_name: str) -> None:
        """Mark a task as completed.

        Args:
            task_name: Name of the task that completed
        """
        self.in_progress_tasks.discard(task_name)
        self.completed_tasks.add(task_name)

    def mark_task_failed(self, task_name: str) -> None:
        """Mark a task as failed.

        Args:
            task_name: Name of the task that failed
        """
        self.in_progress_tasks.discard(task_name)
        self.failed_tasks.add(task_name)

    def mark_task_in_progress(self, task_name: str) -> None:
        """Mark a task as in progress.

        Args:
            task_name: Name of the task that started
        """
        self.in_progress_tasks.add(task_name)

    def set_task_result_id(self, task_name: str, result_id: str) -> None:
        """Set the result ID for a task.

        Args:
            task_name: Name of the task
            result_id: Result ID to set
        """
        self.task_result_ids[task_name] = result_id

