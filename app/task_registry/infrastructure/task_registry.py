"""
Task registry implementation.
Stores and manages task metadata.
"""

import logging
from typing import Any

from app.task_registry.domain.registry_port import RegistryPort
from app.task_registry.domain.task_model import TaskMetadata


class TaskRegistry(RegistryPort):
    """
    In-memory task registry.

    Example:
        registry = TaskRegistry()

        # Register a task
        task = TaskMetadata(
            name="my_task",
            func=my_function,
            dependencies=["other_task"]
        )
        registry.register(task)

        # Validate dependencies
        errors = registry.validate_dependencies()
        if errors:
            print("Validation errors:", errors)
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._tasks: dict[str, TaskMetadata] = {}
        self._tag_index: dict[str, set[str]] = {}  # tag -> set of task names

    def register(self, task: TaskMetadata) -> None:
        """
        Register a task.

        Args:
            task: Task metadata to register.

        Raises:
            ValueError: If task name already exists.
        """
        if task.name in self._tasks:
            raise ValueError(f"Task '{task.name}' is already registered")

        self._tasks[task.name] = task

        # Update tag index
        for tag in task.tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = set()
            self._tag_index[tag].add(task.name)

        logging.info(
            f"Registered task '{task.name}' (dependencies: {task.dependencies}, tags: {task.tags})"
        )

    def get(self, name: str) -> TaskMetadata | None:
        """
        Get a task by name.

        Args:
            name: Task name.

        Returns:
            Task metadata if found, None otherwise.
        """
        return self._tasks.get(name)

    def get_all(self) -> dict[str, TaskMetadata]:
        """
        Get all registered tasks.

        Returns:
            Dictionary mapping task names to metadata.
        """
        return self._tasks.copy()

    def validate_dependencies(self) -> list[str]:
        """
        Validate that all task dependencies exist.

        Returns:
            List of error messages. Empty list if all valid.
        """
        errors: list[str] = []

        for task_name, task in self._tasks.items():
            for dependency in task.dependencies:
                if dependency not in self._tasks:
                    errors.append(
                        f"Task '{task_name}' depends on '{dependency}' which is not registered"
                    )

        return errors

    def get_tasks_by_tag(self, tag: str) -> list[TaskMetadata]:
        """
        Get all tasks with a specific tag.

        Args:
            tag: Tag to filter by.

        Returns:
            List of tasks with the tag.
        """
        if tag not in self._tag_index:
            return []

        return [self._tasks[name] for name in self._tag_index[tag]]

    def unregister(self, name: str) -> bool:
        """
        Unregister a task.

        Args:
            name: Task name to unregister.

        Returns:
            True if task was unregistered, False if not found.
        """
        if name not in self._tasks:
            return False

        task = self._tasks.pop(name)

        # Update tag index
        for tag in task.tags:
            if tag in self._tag_index:
                self._tag_index[tag].discard(name)
                if not self._tag_index[tag]:
                    del self._tag_index[tag]

        logging.info(f"Unregistered task '{name}'")
        return True

    def get_stats(self) -> dict[str, Any]:
        """
        Get registry statistics.

        Returns:
            Dictionary with statistics.
        """
        return {
            "total_tasks": len(self._tasks),
            "total_tags": len(self._tag_index),
            "tasks_with_dependencies": sum(1 for task in self._tasks.values() if task.dependencies),
            "async_tasks": sum(1 for task in self._tasks.values() if task.is_async),
            "sync_tasks": sum(1 for task in self._tasks.values() if not task.is_async),
        }

    def clear(self) -> None:
        """Clear all registered tasks."""
        self._tasks.clear()
        self._tag_index.clear()
        logging.info("Cleared all tasks from registry")

    def __getstate__(self) -> dict[str, Any]:
        """Support for pickling.

        Returns:
            State dictionary for pickle
        """
        return {
            "_tasks": self._tasks,
            "_tag_index": self._tag_index,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Support for unpickling.

        Args:
            state: State dictionary from pickle
        """
        self._tasks = state["_tasks"]
        self._tag_index = state["_tag_index"]
