"""
Task registry interface.
Defines how tasks are registered and managed.
"""

from abc import ABC, abstractmethod

from app.task_registry.domain.task_model import TaskMetadata


class RegistryPort(ABC):
    """
    Interface for task registry.
    Manages task registration, lookup, and validation.
    """

    @abstractmethod
    def register(self, task: TaskMetadata) -> None:
        """
        Register a task.

        Args:
            task: Task metadata to register.

        Raises:
            ValueError: If task name already exists or validation fails.
        """
        ...

    @abstractmethod
    def get(self, name: str) -> TaskMetadata | None:
        """
        Get a task by name.

        Args:
            name: Task name.

        Returns:
            Task metadata if found, None otherwise.
        """
        ...

    @abstractmethod
    def get_all(self) -> dict[str, TaskMetadata]:
        """
        Get all registered tasks.

        Returns:
            Dictionary mapping task names to metadata.
        """
        ...

    @abstractmethod
    def validate_dependencies(self) -> list[str]:
        """
        Validate that all task dependencies exist.

        Returns:
            List of error messages. Empty list if all valid.
        """
        ...

    @abstractmethod
    def get_tasks_by_tag(self, tag: str) -> list[TaskMetadata]:
        """
        Get all tasks with a specific tag.

        Args:
            tag: Tag to filter by.

        Returns:
            List of tasks with the tag.
        """
        ...
