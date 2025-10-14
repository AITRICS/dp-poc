"""
Node model for DAG.
Represents a single task in the execution graph.
"""

from dataclasses import dataclass, field
from typing import Any

from app.task_registry.domain.task_model import TaskMetadata


@dataclass
class Node:
    """
    A node in the DAG representing a task.

    Attributes:
        task: The TaskMetadata from TaskRegistry.
        node_id: Unique identifier (same as task name).
        upstream: Set of node IDs this node depends on (parents).
        downstream: Set of node IDs that depend on this node (children).
        level: Parallel execution level (0 for root nodes).
    """

    task: TaskMetadata
    node_id: str
    upstream: set[str] = field(default_factory=set)
    downstream: set[str] = field(default_factory=set)
    level: int = 0

    def __post_init__(self) -> None:
        """Validate node data."""
        if not self.node_id:
            raise ValueError("Node ID cannot be empty")
        if self.node_id != self.task.name:
            raise ValueError(f"Node ID '{self.node_id}' must match task name '{self.task.name}'")

    def is_root(self) -> bool:
        """Check if this is a root node (no upstream dependencies)."""
        return len(self.upstream) == 0

    def is_leaf(self) -> bool:
        """Check if this is a leaf node (no downstream dependents)."""
        return len(self.downstream) == 0

    def is_ready(self, completed: set[str]) -> bool:
        """
        Check if this node is ready to execute.

        Args:
            completed: Set of completed node IDs.

        Returns:
            True if all upstream nodes are completed, False otherwise.
        """
        return self.upstream.issubset(completed)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "node_id": self.node_id,
            "task_name": self.task.name,
            "upstream": list(self.upstream),
            "downstream": list(self.downstream),
            "level": self.level,
            "is_async": self.task.is_async,
        }
