"""
Task decorator.
Decorates functions to register them as tasks.
"""

from collections.abc import Callable
from typing import Any, TypeVar

from app.task_registry.domain.task_model import TaskMetadata
from app.task_registry.infrastructure.task_registry import TaskRegistry
from app.task_registry.utils.schema_utils import (
    extract_function_schema,
    is_async_function,
)

F = TypeVar("F", bound=Callable[..., Any])

# Global registry instance
_global_registry = TaskRegistry()


def task(
    name: str | None = None,
    *,
    tags: list[str] | None = None,
    dependencies: list[str] | None = None,
    description: str | None = None,
    max_retries: int = 0,
    fail_safe: bool = False,
    stream_output: bool = False,
    timeout: int | None = None,
) -> Callable[[F], F]:
    """
    Decorator to register a function as a task.

    Input and output schemas are automatically extracted from function type hints.

    Args:
        name: Task name (defaults to function name).
        tags: List of tags for categorization.
        dependencies: List of task names this task depends on.
        description: Human-readable description (defaults to docstring).
        max_retries: Maximum number of retries on failure (default: 0).
        fail_safe: If True, continue execution even if this task fails (default: False).
        stream_output: If True, task returns Generator and triggers fan-out execution (default: False).
        timeout: Task execution timeout in seconds (int). None means no timeout (default: None).

    Returns:
        Decorated function (unchanged).

    Example:
        @task(name="extract_data", tags=["etl"])
        async def extract_data() -> dict[str, list[int]]:
            return {"data": [1, 2, 3]}

        @task(name="transform_data", dependencies=["extract_data"])
        def transform_data(data: dict[str, list[int]]) -> list[int]:
            return data["data"]

        @task(name="process_stream", stream_output=True)
        def process_stream() -> Generator[int, None, None]:
            for i in range(10):
                yield i
    """

    def decorator(func: F) -> F:
        task_name = name or func.__name__
        task_tags = tags or []
        task_dependencies = dependencies or []

        # Auto-extract schemas from type hints (fill missing with Any)
        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=True)

        # Create task metadata
        metadata = TaskMetadata(
            name=task_name,
            func=func,
            tags=task_tags,
            dependencies=task_dependencies,
            input_schema=input_schema,
            output_schema=output_schema,
            description=description or func.__doc__,
            is_async=is_async_function(func),
            max_retries=max_retries,
            fail_safe=fail_safe,
            stream_output=stream_output,
            timeout=timeout,
        )

        # Register task
        _global_registry.register(metadata)

        # Return original function unchanged
        return func

    return decorator


def get_registry() -> TaskRegistry:
    """
    Get the global task registry.

    Returns:
        The global TaskRegistry instance.
    """
    return _global_registry


def validate_all_tasks() -> list[str]:
    """
    Validate all registered tasks.

    Returns:
        List of validation errors. Empty if all valid.
    """
    return _global_registry.validate_dependencies()


def clear_registry() -> None:
    """Clear the global registry (useful for testing)."""
    _global_registry.clear()
