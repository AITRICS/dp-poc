"""Task execution status enumeration."""

from enum import StrEnum, auto


class TaskStatus(StrEnum):
    """Represents the execution status of a task.

    Attributes:
        PENDING: Task is waiting to be executed
        RUNNING: Task is currently being executed
        SUCCESS: Task completed successfully
        FAILED: Task failed with an error
        TIMEOUT: Task exceeded its timeout limit
        CANCELLED: Task was cancelled before completion
    """

    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    TIMEOUT = auto()
    CANCELLED = auto()

    def is_terminal(self) -> bool:
        """Check if this status is terminal (execution has ended).

        Returns:
            True if status is SUCCESS, FAILED, TIMEOUT, or CANCELLED
        """
        return self in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.TIMEOUT,
            TaskStatus.CANCELLED,
        )

    def is_successful(self) -> bool:
        """Check if this status indicates successful execution.

        Returns:
            True only if status is SUCCESS
        """
        return self == TaskStatus.SUCCESS
