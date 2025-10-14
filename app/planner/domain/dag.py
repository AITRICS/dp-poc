"""
DAG (Directed Acyclic Graph) implementation.
Manages nodes and their dependencies, with cycle detection and topological sorting.
"""

import hashlib
import logging
from collections import deque
from typing import Any

from app.planner.domain.node import Node


class DAG:
    """
    Directed Acyclic Graph for task execution planning.

    Manages nodes and their dependencies, validates the graph structure,
    and provides topological sorting for execution order.

    The DAG has a unique ID based on either:
    - User-provided ID (if specified)
    - Content hash (auto-generated from DAG structure)
    """

    def __init__(self, dag_id: str | None = None) -> None:
        """
        Initialize a DAG.

        Args:
            dag_id: Optional user-provided DAG ID. If not provided,
                   a content hash will be generated automatically.
        """
        self._nodes: dict[str, Node] = {}
        self._validated = False
        self._user_provided_id = dag_id
        self._content_hash: str | None = None

    def add_node(self, node: Node) -> None:
        """
        Add a node to the DAG.

        Args:
            node: Node to add.

        Raises:
            ValueError: If node with same ID already exists.
        """
        if node.node_id in self._nodes:
            raise ValueError(f"Node '{node.node_id}' already exists in DAG")

        self._nodes[node.node_id] = node
        self._validated = False
        logging.debug(f"Added node '{node.node_id}' to DAG")

    def get_node(self, node_id: str) -> Node | None:
        """
        Get a node by ID.

        Args:
            node_id: Node identifier.

        Returns:
            Node if found, None otherwise.
        """
        return self._nodes.get(node_id)

    def get_all_nodes(self) -> dict[str, Node]:
        """
        Get all nodes in the DAG.

        Returns:
            Dictionary mapping node IDs to nodes.
        """
        return self._nodes.copy()

    def get_root_nodes(self) -> list[Node]:
        """
        Get all root nodes (nodes with no upstream dependencies).

        Returns:
            List of root nodes.
        """
        return [node for node in self._nodes.values() if node.is_root()]

    def get_leaf_nodes(self) -> list[Node]:
        """
        Get all leaf nodes (nodes with no downstream dependents).

        Returns:
            List of leaf nodes.
        """
        return [node for node in self._nodes.values() if node.is_leaf()]

    def validate(self) -> list[str]:
        """
        Validate the DAG structure.

        Checks for:
        - Cycle detection
        - Orphan nodes (dependencies that don't exist)
        - Self-references

        Returns:
            List of error messages. Empty if valid.
        """
        errors: list[str] = []

        # Check for self-references
        for node_id, node in self._nodes.items():
            if node_id in node.upstream:
                errors.append(f"Node '{node_id}' has self-reference")

        # Check for orphan nodes (upstream nodes that don't exist)
        for node_id, node in self._nodes.items():
            for upstream_id in node.upstream:
                if upstream_id not in self._nodes:
                    errors.append(
                        f"Node '{node_id}' depends on '{upstream_id}' which does not exist"
                    )

        # Check for cycles
        cycles = self._detect_cycles()
        for cycle in cycles:
            cycle_str = " -> ".join(cycle)
            errors.append(f"Cycle detected: {cycle_str}")

        if not errors:
            self._validated = True
            logging.info(f"DAG validated successfully with {len(self._nodes)} nodes")

        return errors

    def topological_sort(self) -> list[str]:
        """
        Perform topological sort using Kahn's algorithm.

        Returns:
            List of node IDs in topological order.

        Raises:
            ValueError: If DAG has not been validated or contains cycles.
        """
        if not self._validated:
            errors = self.validate()
            if errors:
                raise ValueError(f"Cannot sort invalid DAG: {errors}")

        # Calculate in-degree for each node
        in_degree: dict[str, int] = dict.fromkeys(self._nodes, 0)
        for node in self._nodes.values():
            for downstream_id in node.downstream:
                in_degree[downstream_id] += 1

        # Start with nodes that have no incoming edges
        queue: deque[str] = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result: list[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            # Reduce in-degree for downstream nodes
            node = self._nodes[node_id]
            for downstream_id in node.downstream:
                in_degree[downstream_id] -= 1
                if in_degree[downstream_id] == 0:
                    queue.append(downstream_id)

        # If not all nodes are processed, there's a cycle
        if len(result) != len(self._nodes):
            raise ValueError("Topological sort failed: cycle detected")

        return result

    def calculate_levels(self) -> None:
        """
        Calculate the execution level for each node.

        The level is the maximum distance from any root node.
        Root nodes are at level 0, their direct children at level 1, etc.

        This should be called after validation.
        """
        if not self._validated:
            errors = self.validate()
            if errors:
                raise ValueError(f"Cannot calculate levels for invalid DAG: {errors}")

        # Initialize all nodes to level 0
        for node in self._nodes.values():
            node.level = 0

        # Get topological order
        orders = self.topological_sort()

        # Calculate levels based on upstream nodes
        for node_id in orders:
            node = self._nodes[node_id]
            if node.upstream:
                # Level is max(upstream levels) + 1
                max_upstream_level = max(
                    self._nodes[upstream_id].level for upstream_id in node.upstream
                )
                node.level = max_upstream_level + 1

        logging.info(f"Calculated levels for {len(self._nodes)} nodes")

    def _detect_cycles(self) -> list[list[str]]:
        """
        Detect cycles using DFS with White-Gray-Black algorithm.

        Returns:
            List of cycles found (each cycle is a list of node IDs).
        """
        white: int = 0
        gray: int = 1
        black: int = 2

        color: dict[str, int] = dict.fromkeys(self._nodes, white)
        cycles: list[list[str]] = []

        def dfs(node_id: str, path: list[str]) -> None:
            """DFS helper function."""
            if color[node_id] == black:
                return

            if color[node_id] == gray:
                # Found a cycle
                cycle_start = path.index(node_id)
                cycle = path[cycle_start:] + [node_id]
                cycles.append(cycle)
                return

            color[node_id] = gray
            path.append(node_id)

            node = self._nodes[node_id]
            for downstream_id in node.downstream:
                if downstream_id in self._nodes:  # Skip if node doesn't exist
                    dfs(downstream_id, path[:])

            color[node_id] = black

        for node_id in self._nodes:
            if color[node_id] == white:
                dfs(node_id, [])

        return cycles

    def to_dict(self) -> dict[str, Any]:
        """
        Convert DAG to dictionary for serialization.

        Returns:
            Dictionary representation of the DAG.
        """
        return {
            "nodes": {node_id: node.to_dict() for node_id, node in self._nodes.items()},
            "node_count": len(self._nodes),
            "root_nodes": [node.node_id for node in self.get_root_nodes()],
            "leaf_nodes": [node.node_id for node in self.get_leaf_nodes()],
        }

    def __len__(self) -> int:
        """Return the number of nodes in the DAG."""
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        """Check if a node exists in the DAG."""
        return node_id in self._nodes

    @property
    def dag_id(self) -> str:
        """
        Get the DAG ID.

        Returns user-provided ID if available, otherwise generates
        a content hash based on the DAG structure.

        Returns:
            DAG identifier string.
        """
        if self._user_provided_id:
            return self._user_provided_id

        if not self._content_hash:
            self._content_hash = self._generate_content_hash()

        return self._content_hash

    def get_content_hash(self) -> str:
        """
        Get the content hash of the DAG.

        This is always calculated from the DAG structure,
        regardless of whether a user-provided ID exists.

        Returns:
            Content hash string (16 characters).
        """
        if not self._content_hash:
            self._content_hash = self._generate_content_hash()
        return self._content_hash

    def is_same_structure(self, other: "DAG") -> bool:
        """
        Check if this DAG has the same structure as another DAG.

        Two DAGs have the same structure if they have the same content hash.

        Args:
            other: Another DAG to compare with.

        Returns:
            True if structures are identical, False otherwise.
        """
        return self.get_content_hash() == other.get_content_hash()

    def _generate_content_hash(self) -> str:
        """
        Generate a deterministic content hash from the DAG structure.

        The hash is based on:
        - Sorted node names
        - Sorted dependency edges

        Same structure → Same hash

        Returns:
            First 16 characters of SHA256 hash.
        """
        # Get sorted node names
        node_names = sorted(self._nodes.keys())

        # Get sorted edges (upstream -> downstream)
        edges: list[str] = []
        for node_name in node_names:
            node = self._nodes[node_name]
            for upstream in sorted(node.upstream):
                edges.append(f"{upstream}->{node_name}")

        # Create deterministic string representation
        content = "|".join(node_names) + "||" + "|".join(sorted(edges))

        # Generate SHA256 hash and return first 16 characters
        hash_obj = hashlib.sha256(content.encode("utf-8"))
        return hash_obj.hexdigest()[:16]
