"""Executable task model for the executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.task_registry.domain.task_model import TaskMetadata  # noqa: TC001


@dataclass
class ExecutableTask:
    """Represents a task ready to be executed by a worker.

    This is created dynamically by the Orchestrator for each task execution.
    It contains all the information needed by a worker process to execute
    the task, including input locations and metadata.

    Attributes:
        run_id: Unique identifier for this pipeline run
        task_name: Name of the task to execute
        task_result_id: Unique identifier for this specific task execution result
                       (used for streaming outputs where multiple results per task)
        task_metadata: Metadata about the task (function, schema, config, etc.)
        inputs: Dictionary mapping parameter names to their input locations
                For data flow dependencies: {"param_name": ("upstream_task", "result_id")}
                For control flow dependencies: parameter not included
        retry_count: Current retry attempt number (0 for first attempt)
    """

    run_id: str
    task_name: str
    task_result_id: str
    task_metadata: TaskMetadata
    inputs: dict[str, tuple[str, str]] = field(default_factory=dict)
    retry_count: int = 0

    def __post_init__(self) -> None:
        """Validate the executable task."""
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.task_name:
            raise ValueError("task_name must not be empty")
        if not self.task_result_id:
            raise ValueError("task_result_id must not be empty")
        if self.retry_count < 0:
            raise ValueError("retry_count must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary containing all task information
        """
        return {
            "run_id": self.run_id,
            "task_name": self.task_name,
            "task_result_id": self.task_result_id,
            "task_metadata": self.task_metadata.to_dict(),
            "inputs": self.inputs,
            "retry_count": self.retry_count,
        }

    def should_retry_on_failure(self) -> bool:
        """Check if this task should be retried on failure.

        Returns:
            True if retry_count < max_retries
        """
        return self.retry_count < self.task_metadata.max_retries

    def create_retry_task(self) -> ExecutableTask:
        """Create a new executable task for retry with incremented retry count.

        Returns:
            New ExecutableTask with retry_count + 1
        """
        return ExecutableTask(
            run_id=self.run_id,
            task_name=self.task_name,
            task_result_id=self.task_result_id,
            task_metadata=self.task_metadata,
            inputs=self.inputs.copy(),
            retry_count=self.retry_count + 1,
        )
