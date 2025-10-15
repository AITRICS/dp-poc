"""
DAG Analyzer.
Performs topological sorting and level calculation for DAG execution planning.
"""

import logging
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.planner.domain.dag import DAG


class DAGAnalyzer:
    """
    DAG analysis for execution planning.

    Provides:
    - Topological sorting (execution order)
    - Level calculation (parallel execution levels)
    """

    @staticmethod
    def topological_sort(dag: "DAG") -> list[str]:
        """
        Perform topological sort using Kahn's algorithm.

        Args:
            dag: DAG to sort.

        Returns:
            List of node IDs in topological order.

        Raises:
            ValueError: If DAG contains cycles.
        """
        nodes = dag.get_all_nodes()

        # Calculate in-degree for each node
        in_degree: dict[str, int] = dict.fromkeys(nodes, 0)
        for node in nodes.values():
            for downstream_id in node.downstream:
                in_degree[downstream_id] += 1

        # Start with nodes that have no incoming edges
        queue: deque[str] = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result: list[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            # Reduce in-degree for downstream nodes
            node = nodes[node_id]
            for downstream_id in node.downstream:
                in_degree[downstream_id] -= 1
                if in_degree[downstream_id] == 0:
                    queue.append(downstream_id)

        # If not all nodes are processed, there's a cycle
        if len(result) != len(nodes):
            raise ValueError("Topological sort failed: cycle detected")

        return result

    @staticmethod
    def calculate_levels(dag: "DAG") -> None:
        """
        Calculate the execution level for each node.

        The level is the maximum distance from any root node.
        Root nodes are at level 0, their direct children at level 1, etc.

        This method modifies the nodes in place.

        Args:
            dag: DAG to calculate levels for.

        Raises:
            ValueError: If DAG contains cycles.
        """
        nodes = dag.get_all_nodes()

        # Initialize all nodes to level 0
        for node in nodes.values():
            node.level = 0

        # Get topological order
        orders = DAGAnalyzer.topological_sort(dag)

        # Calculate levels based on upstream nodes
        for node_id in orders:
            node = nodes[node_id]
            if node.upstream:
                # Level is max(upstream levels) + 1
                max_upstream_level = max(nodes[upstream_id].level for upstream_id in node.upstream)
                node.level = max_upstream_level + 1

        logging.info(f"Calculated levels for {len(nodes)} nodes")
