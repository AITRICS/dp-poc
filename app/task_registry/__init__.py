"""
Task Registry System.

Provides task registration, metadata management, and dependency validation.
"""

from app.task_registry.decorator import (
    clear_registry,
    get_registry,
    task,
    validate_all_tasks,
)
from app.task_registry.domain import RegistryPort, TaskMetadata
from app.task_registry.infrastructure import ExcutableTask, TaskRegistry
from app.task_registry.utils import extract_function_schema, is_async_function

__all__ = [
    # Decorator
    "task",
    "get_registry",
    "validate_all_tasks",
    "clear_registry",
    # Domain
    "TaskMetadata",
    "RegistryPort",
    # Infrastructure
    "TaskRegistry",
    "ExcutableTask",
    # Utils
    "is_async_function",
    "extract_function_schema",
]
