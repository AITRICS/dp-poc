"""
DAG Hasher.
Generates content-based hash for DAG structure.
"""

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.planner.domain.dag import DAG


class DAGHasher:
    """
    DAG content hasher.

    Generates deterministic hash based on DAG structure
    (nodes and edges) for identification and comparison.
    """

    @staticmethod
    def generate_hash(dag: "DAG") -> str:
        """
        Generate a deterministic content hash from the DAG structure.

        The hash is based on:
        - Sorted node names
        - Sorted dependency edges

        Same structure → Same hash

        Args:
            dag: DAG to hash.

        Returns:
            First 16 characters of SHA256 hash.
        """
        nodes = dag.get_all_nodes()

        # Get sorted node names
        node_names = sorted(nodes.keys())

        # Get sorted edges (upstream -> downstream)
        edges: list[str] = []
        for node_name in node_names:
            node = nodes[node_name]
            for upstream in sorted(node.upstream):
                edges.append(f"{upstream}->{node_name}")

        # Create deterministic string representation
        content = "|".join(node_names) + "||" + "|".join(sorted(edges))

        # Generate SHA256 hash and return first 16 characters
        hash_obj = hashlib.sha256(content.encode("utf-8"))
        return hash_obj.hexdigest()[:16]
