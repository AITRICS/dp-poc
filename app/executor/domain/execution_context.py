"""Execution context for pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.io_manager.domain.io_manager_port import IOManagerPort
    from app.planner.domain.execution_plan import ExecutionPlan


@dataclass
class ExecutionContext:
    """Context for a pipeline execution run.

    This provides all the information and resources needed to execute
    a pipeline, including the execution plan, I/O manager, and run metadata.

    Attributes:
        run_id: Unique identifier for this pipeline run
        dag_id: Identifier of the DAG being executed
        execution_plan: The execution plan containing task order and dependencies
        io_manager: I/O manager for storing and loading task results
        metadata: Additional metadata about the run (tags, user info, etc.)
    """

    run_id: str
    dag_id: str
    execution_plan: ExecutionPlan
    io_manager: IOManagerPort
    metadata: dict[str, str] | None = None

    def __post_init__(self) -> None:
        """Validate the execution context."""
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.dag_id:
            raise ValueError("dag_id must not be empty")
        if self.metadata is None:
            self.metadata = {}

    def get_root_tasks(self) -> list[str]:
        """Get the root tasks (tasks with no dependencies) from the execution plan.

        Returns:
            List of task names that can be executed first
        """
        if not self.execution_plan.parallel_levels:
            return []
        return self.execution_plan.parallel_levels[0]

    def get_downstream_tasks(self, task_name: str) -> list[str]:
        """Get tasks that depend on the given task.

        Args:
            task_name: Name of the task to find downstream tasks for

        Returns:
            List of task names that depend on the given task
        """
        root_node = self.execution_plan.dag.get_node(task_name)
        if root_node is None:
            return []
        return list(root_node.downstream)

    def get_upstream_tasks(self, task_name: str) -> list[str]:
        """Get tasks that the given task depends on.

        Args:
            task_name: Name of the task to find upstream tasks for

        Returns:
            List of task names that the given task depends on
        """
        root_node = self.execution_plan.dag.get_node(task_name)
        if root_node is None:
            return []
        return list(root_node.upstream)

    def is_task_ready(self, task_name: str, completed_tasks: set[str]) -> bool:
        """Check if a task is ready to be executed.

        A task is ready if all its upstream dependencies have been completed.

        Args:
            task_name: Name of the task to check
            completed_tasks: Set of task names that have completed successfully

        Returns:
            True if the task can be executed now
        """
        upstream_tasks = self.get_upstream_tasks(task_name)
        return all(upstream in completed_tasks for upstream in upstream_tasks)

    def get_ready_tasks(self, completed_tasks: set[str]) -> list[str]:
        """Get all tasks that are ready to be executed.

        Args:
            completed_tasks: Set of task names that have completed successfully

        Returns:
            List of task names that can be executed now
        """
        all_tasks = set(self.execution_plan.execution_order)
        remaining_tasks = all_tasks - completed_tasks

        return [
            task_name
            for task_name in remaining_tasks
            if self.is_task_ready(task_name, completed_tasks)
        ]
