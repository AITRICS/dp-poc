"""
DAG (Directed Acyclic Graph) implementation.
Manages nodes and their dependencies.
"""

import logging
from typing import Any

from app.planner.domain.dag_hasher import DAGHasher
from app.planner.domain.node import Node


class DAG:
    """
    Directed Acyclic Graph for task execution planning.

    Manages nodes and their dependencies.

    The DAG has a unique ID based on either:
    - User-provided ID (if specified)
    - Content hash (auto-generated from DAG structure)

    Note: Validation, topological sorting, and level calculation
    are delegated to DAGValidator and DAGAnalyzer classes.
    """

    def __init__(self, dag_id: str | None = None) -> None:
        """
        Initialize a DAG.

        Args:
            dag_id: Optional user-provided DAG ID. If not provided,
                   a content hash will be generated automatically.
        """
        self._nodes: dict[str, Node] = {}
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
            self._content_hash = DAGHasher.generate_hash(self)

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
            self._content_hash = DAGHasher.generate_hash(self)
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
