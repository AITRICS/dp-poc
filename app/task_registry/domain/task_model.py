"""
Task metadata model.
Stores information about registered tasks.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskMetadata:
    """
    Metadata for a registered task.

    Attributes:
        name: Unique task identifier.
        func: The callable function (sync or async).
        tags: List of tags for categorization.
        dependencies: List of task names this task depends on.
        input_schema: Expected input type/schema (for validation).
        output_schema: Expected output type/schema (for validation).
        description: Human-readable description.
        is_async: Whether the function is async.
    """

    name: str
    func: Callable[..., Any]
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    input_schema: type | dict[str, type] | None = None
    output_schema: type | None = None
    description: str | None = None
    is_async: bool = False

    def __post_init__(self) -> None:
        """Validate task metadata."""
        if not self.name:
            raise ValueError("Task name cannot be empty")

        if not callable(self.func):
            raise ValueError(f"Task '{self.name}' func must be callable")

        # Check for duplicate dependencies
        if len(self.dependencies) != len(set(self.dependencies)):
            raise ValueError(f"Task '{self.name}' has duplicate dependencies")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "tags": self.tags,
            "dependencies": self.dependencies,
            "input_schema": str(self.input_schema) if self.input_schema else None,
            "output_schema": str(self.output_schema) if self.output_schema else None,
            "description": self.description,
            "is_async": self.is_async,
        }
