"""Executable task model for the executor."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from uuid6 import uuid7


@dataclass
class ExecutableTask:
    """Represents a task ready to be executed by a worker.

    This is created dynamically by the Orchestrator for each task execution.
    It contains all the information needed by a worker process to execute
    the task, including input locations.

    The worker will look up the actual task metadata from its TaskRegistry
    using the task_name, avoiding the need to pickle functions across processes.

    Attributes:
        run_id: Unique identifier for this pipeline run
        task_name: Name of the task to execute (used to look up metadata in registry)
        task_result_id: Unique identifier for this specific task execution result
                       (used for streaming outputs where multiple results per task)
        inputs: Dictionary mapping parameter names to their input locations
                For data flow dependencies: {"param_name": ("upstream_task", "result_id")}
                For control flow dependencies: parameter not included
        retry_count: Current retry attempt number (0 for first attempt)
        max_retries: Maximum number of retries allowed (copied from TaskMetadata)
    """

    run_id: str
    task_name: str
    task_result_id: str
    inputs: dict[str, tuple[str, str]] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 0

    task_id: uuid.UUID = field(default_factory=uuid7)

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
            "inputs": self.inputs,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "task_id": str(self.task_id),
        }

    def should_retry_on_failure(self) -> bool:
        """Check if this task should be retried on failure.

        Returns:
            True if retry_count < max_retries
        """
        return self.retry_count < self.max_retries

    def create_retry_task(self) -> ExecutableTask:
        """Create a new executable task for retry with incremented retry count.

        Returns:
            New ExecutableTask with retry_count + 1
        """
        return ExecutableTask(
            run_id=self.run_id,
            task_name=self.task_name,
            task_result_id=self.task_result_id,
            inputs=self.inputs.copy(),
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
        )


class _StopSentinel:
    """Sentinel value to signal worker processes to stop gracefully."""

    def __repr__(self) -> str:
        """String representation."""
        return "STOP_SENTINEL"


# Singleton sentinel instance for graceful worker shutdown
STOP_SENTINEL = _StopSentinel()
