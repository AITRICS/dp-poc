"""Task execution result model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.executor.domain.task_status import TaskStatus


@dataclass
class TaskResult:
    """Represents the result of a task execution.

    This is a lightweight signal sent from workers back to the orchestrator
    via the result queue. It does NOT contain the actual output data, only
    metadata about the execution. The actual output is stored via IOManager.

    Attributes:
        run_id: Unique identifier for this pipeline run
        task_name: Name of the executed task
        task_result_id: Unique identifier for this specific execution result
        status: Execution status (SUCCESS, FAILED, etc.)
        execution_time: Time taken to execute in seconds
        retry_count: Number of retries attempted
        error_message: Error message if task failed (None if successful)
        error_type: Type of error that occurred (None if successful)
        is_streaming: Whether this result is part of a streaming output
        stream_index: Index of this result in stream (None if not streaming)
        stream_complete: Whether this is the last result in stream (None if not streaming)
    """

    run_id: str
    task_name: str
    task_result_id: str
    status: TaskStatus
    execution_time: float = 0.0
    retry_count: int = 0
    error_message: str | None = None
    error_type: str | None = None
    is_streaming: bool = False
    stream_index: int | None = None
    stream_complete: bool | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the task result."""
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.task_name:
            raise ValueError("task_name must not be empty")
        if not self.task_result_id:
            raise ValueError("task_result_id must not be empty")
        if self.execution_time < 0:
            raise ValueError("execution_time must be non-negative")
        if self.retry_count < 0:
            raise ValueError("retry_count must be non-negative")

        # Validate streaming fields consistency
        if self.is_streaming:
            if self.stream_index is None:
                raise ValueError("stream_index must be set for streaming results")
            if self.stream_complete is None:
                raise ValueError("stream_complete must be set for streaming results")
        else:
            if self.stream_index is not None:
                raise ValueError("stream_index should be None for non-streaming results")
            if self.stream_complete is not None:
                raise ValueError("stream_complete should be None for non-streaming results")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary containing all result information
        """
        return {
            "run_id": self.run_id,
            "task_name": self.task_name,
            "task_result_id": self.task_result_id,
            "status": self.status.name,
            "execution_time": self.execution_time,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "is_streaming": self.is_streaming,
            "stream_index": self.stream_index,
            "stream_complete": self.stream_complete,
            "metadata": self.metadata,
        }

    @classmethod
    def success(
        cls,
        run_id: str,
        task_name: str,
        task_result_id: str,
        execution_time: float,
        retry_count: int = 0,
        is_streaming: bool = False,
        stream_index: int | None = None,
        stream_complete: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Create a successful task result.

        Args:
            run_id: Run identifier
            task_name: Task name
            task_result_id: Result identifier
            execution_time: Execution time in seconds
            retry_count: Number of retries
            is_streaming: Whether this is a streaming result
            stream_index: Stream index (required if is_streaming)
            stream_complete: Whether stream is complete (required if is_streaming)
            metadata: Additional metadata

        Returns:
            TaskResult with SUCCESS status
        """
        return cls(
            run_id=run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            status=TaskStatus.SUCCESS,
            execution_time=execution_time,
            retry_count=retry_count,
            is_streaming=is_streaming,
            stream_index=stream_index,
            stream_complete=stream_complete,
            metadata=metadata or {},
        )

    @classmethod
    def failure(
        cls,
        run_id: str,
        task_name: str,
        task_result_id: str,
        execution_time: float,
        error: Exception,
        retry_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Create a failed task result.

        Args:
            run_id: Run identifier
            task_name: Task name
            task_result_id: Result identifier
            execution_time: Execution time in seconds
            error: Exception that caused the failure
            retry_count: Number of retries
            metadata: Additional metadata

        Returns:
            TaskResult with FAILED status
        """
        return cls(
            run_id=run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            status=TaskStatus.FAILED,
            execution_time=execution_time,
            retry_count=retry_count,
            error_message=str(error),
            error_type=type(error).__name__,
            metadata=metadata or {},
        )

    @classmethod
    def timeout(
        cls,
        run_id: str,
        task_name: str,
        task_result_id: str,
        execution_time: float,
        retry_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Create a timeout task result.

        Args:
            run_id: Run identifier
            task_name: Task name
            task_result_id: Result identifier
            execution_time: Execution time in seconds
            retry_count: Number of retries
            metadata: Additional metadata

        Returns:
            TaskResult with TIMEOUT status
        """
        return cls(
            run_id=run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            status=TaskStatus.TIMEOUT,
            execution_time=execution_time,
            retry_count=retry_count,
            error_message="Task execution exceeded timeout limit",
            error_type="TimeoutError",
            metadata=metadata or {},
        )

    @classmethod
    def cancelled(
        cls,
        run_id: str,
        task_name: str,
        task_result_id: str,
        execution_time: float,
        retry_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Create a cancelled task result.

        Args:
            run_id: Run identifier
            task_name: Task name
            task_result_id: Result identifier
            execution_time: Execution time in seconds
            retry_count: Number of retries
            metadata: Additional metadata

        Returns:
            TaskResult with CANCELLED status
        """
        return cls(
            run_id=run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            status=TaskStatus.CANCELLED,
            execution_time=execution_time,
            retry_count=retry_count,
            error_message="Task execution was cancelled",
            error_type="CancelledError",
            metadata=metadata or {},
        )
