"""Execution result model for pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.executor.domain.task_result import TaskResult
from app.executor.domain.task_status import TaskStatus


@dataclass
class ExecutionResult:
    """Represents the result of a complete pipeline execution.

    This aggregates all task results and provides overall execution status.

    Attributes:
        run_id: Unique identifier for this pipeline run
        dag_id: Identifier of the DAG that was executed
        status: Overall pipeline execution status
        completed_tasks: List of task names that completed successfully
        failed_tasks: List of task names that failed
        total_execution_time: Total time taken for pipeline execution in seconds
        task_results: Dictionary mapping task names to their results
                     (list for streaming tasks, single item for normal tasks)
        metadata: Additional metadata about the execution
    """

    run_id: str
    dag_id: str
    status: TaskStatus
    completed_tasks: list[str] = field(default_factory=list)
    failed_tasks: list[str] = field(default_factory=list)
    total_execution_time: float = 0.0
    task_results: dict[str, list[TaskResult]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the execution result."""
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.dag_id:
            raise ValueError("dag_id must not be empty")
        if self.total_execution_time < 0:
            raise ValueError("total_execution_time must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary containing all execution result information
        """
        return {
            "run_id": self.run_id,
            "dag_id": self.dag_id,
            "status": self.status.name,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "total_execution_time": self.total_execution_time,
            "task_results": {
                task_name: [result.to_dict() for result in results]
                for task_name, results in self.task_results.items()
            },
            "metadata": self.metadata,
        }

    @property
    def is_successful(self) -> bool:
        """Check if the entire pipeline execution was successful.

        Returns:
            True if status is SUCCESS (considers fail-safe tasks)
        """
        return self.status == TaskStatus.SUCCESS

    @property
    def total_tasks(self) -> int:
        """Get total number of tasks executed.

        Returns:
            Total number of tasks (completed + failed)
        """
        return len(self.completed_tasks) + len(self.failed_tasks)

    def add_task_result(self, task_name: str, result: TaskResult) -> None:
        """Add a task result to the execution result.

        Args:
            task_name: Name of the task
            result: Task result to add
        """
        if task_name not in self.task_results:
            self.task_results[task_name] = []
        self.task_results[task_name].append(result)

    def get_task_results(self, task_name: str) -> list[TaskResult]:
        """Get all results for a specific task.

        Args:
            task_name: Name of the task

        Returns:
            List of task results (may be empty if task not found)
        """
        return self.task_results.get(task_name, [])
