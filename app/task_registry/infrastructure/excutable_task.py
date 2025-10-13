"""
Executable task.
Executes both sync and async tasks uniformly.
"""

import asyncio
from typing import Any

from app.task_registry.domain.task_model import TaskMetadata


class ExcutableTask:
    """
    Executes tasks (both sync and async).

    Example:
        executor = ExcutableTask(task_metadata)
        result = await executor.execute(arg1, arg2, key=value)
    """

    def __init__(self, task_metadata: TaskMetadata) -> None:
        """
        Initialize task executor.

        Args:
            task: Task metadata containing the function to execute.
        """
        self.task = task_metadata

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the task.

        Handles both sync and async functions uniformly.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            The result of task execution.

        Raises:
            Exception: Any exception raised by the task function.
        """
        if self.task.is_async:
            # Execute async function directly
            return await self.task.func(*args, **kwargs)
        else:
            # Execute sync function in executor to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: self.task.func(*args, **kwargs))

    def get_name(self) -> str:
        """Get the task name."""
        return self.task.name

    def get_dependencies(self) -> list[str]:
        """Get the list of task dependencies."""
        return self.task.dependencies.copy()
