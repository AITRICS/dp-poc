"""
Execution Plan model.
Represents an executable plan derived from a DAG.
"""

from dataclasses import dataclass, field
from typing import Any

from app.planner.domain.dag import DAG
from app.planner.domain.node import Node


@dataclass
class ExecutionPlan:
    """
    Execution plan for task execution.

    Contains the DAG, execution order, and parallel execution levels.

    Attributes:
        dag: The DAG representing task dependencies.
        execution_order: List of node IDs in topological order.
        parallel_levels: List of lists, where each inner list contains
                        node IDs that can be executed in parallel.
    """

    dag: DAG
    execution_order: list[str] = field(default_factory=list)
    parallel_levels: list[list[str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate execution plan."""
        if len(self.dag) == 0:
            raise ValueError("Cannot create execution plan from empty DAG")

        # If not already set, calculate execution order and parallel levels
        if not self.execution_order:
            self.execution_order = self.dag.topological_sort()

        if not self.parallel_levels:
            self.parallel_levels = self._calculate_parallel_levels()

    def _calculate_parallel_levels(self) -> list[list[str]]:
        """
        Calculate parallel execution levels.

        Groups nodes by their level. All nodes in the same level
        can be executed in parallel.

        Returns:
            List of lists containing node IDs grouped by level.
        """
        # Group nodes by level
        levels_map: dict[int, list[str]] = {}
        for node_id, node in self.dag.get_all_nodes().items():
            level = node.level
            if level not in levels_map:
                levels_map[level] = []
            levels_map[level].append(node_id)

        # Convert to sorted list
        max_level = max(levels_map.keys()) if levels_map else 0
        parallel_levels = [levels_map.get(i, []) for i in range(max_level + 1)]

        return parallel_levels

    def get_ready_nodes(self, completed: set[str]) -> list[Node]:
        """
        Get nodes that are ready to execute based on completed nodes.

        A node is ready if all its upstream dependencies are completed.

        Args:
            completed: Set of completed node IDs.

        Returns:
            List of nodes that are ready to execute.
        """
        ready_nodes: list[Node] = []

        for node_id, node in self.dag.get_all_nodes().items():
            # Skip if already completed
            if node_id in completed:
                continue

            # Check if ready
            if node.is_ready(completed):
                ready_nodes.append(node)

        return ready_nodes

    def get_level(self, node_id: str) -> int:
        """
        Get the execution level of a node.

        Args:
            node_id: Node identifier.

        Returns:
            Execution level (0-indexed).

        Raises:
            KeyError: If node does not exist.
        """
        node = self.dag.get_node(node_id)
        if node is None:
            raise KeyError(f"Node '{node_id}' not found in DAG")
        return node.level

    def get_node(self, node_id: str) -> Node | None:
        """
        Get a node by ID.

        Args:
            node_id: Node identifier.

        Returns:
            Node if found, None otherwise.
        """
        return self.dag.get_node(node_id)

    def get_total_levels(self) -> int:
        """
        Get the total number of execution levels.

        Returns:
            Number of levels.
        """
        return len(self.parallel_levels)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert execution plan to dictionary for serialization.

        Returns:
            Dictionary representation.
        """
        return {
            "execution_order": self.execution_order,
            "parallel_levels": self.parallel_levels,
            "total_levels": self.get_total_levels(),
            "total_nodes": len(self.dag),
            "dag": self.dag.to_dict(),
        }

    def __len__(self) -> int:
        """Return the number of nodes in the execution plan."""
        return len(self.dag)
