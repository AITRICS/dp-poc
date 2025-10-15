"""
DAG Validator.
Validates DAG structure for cycles, orphan nodes, and self-references.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.planner.domain.dag import DAG


class DAGValidator:
    """
    DAG structure validator.

    Checks for:
    - Cycle detection
    - Orphan nodes (dependencies that don't exist)
    - Self-references
    """

    @staticmethod
    def validate(dag: "DAG") -> list[str]:
        """
        Validate the DAG structure.

        Checks for:
        - Cycle detection
        - Orphan nodes (dependencies that don't exist)
        - Self-references

        Args:
            dag: DAG to validate.

        Returns:
            List of error messages. Empty if valid.
        """
        errors: list[str] = []

        # Check for self-references
        for node_id, node in dag.get_all_nodes().items():
            if node_id in node.upstream:
                errors.append(f"Node '{node_id}' has self-reference")

        # Check for orphan nodes (upstream nodes that don't exist)
        for node_id, node in dag.get_all_nodes().items():
            for upstream_id in node.upstream:
                if upstream_id not in dag:
                    errors.append(
                        f"Node '{node_id}' depends on '{upstream_id}' which does not exist"
                    )

        # Check for cycles
        cycles = DAGValidator._detect_cycles(dag)
        for cycle in cycles:
            cycle_str = " -> ".join(cycle)
            errors.append(f"Cycle detected: {cycle_str}")

        if not errors:
            logging.info(f"DAG validated successfully with {len(dag)} nodes")

        return errors

    @staticmethod
    def _detect_cycles(dag: "DAG") -> list[list[str]]:
        """
        Detect cycles using DFS with White-Gray-Black algorithm.

        Args:
            dag: DAG to check for cycles.

        Returns:
            List of cycles found (each cycle is a list of node IDs).
        """
        white: int = 0
        gray: int = 1
        black: int = 2

        nodes = dag.get_all_nodes()
        color: dict[str, int] = dict.fromkeys(nodes, white)
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

            node = nodes[node_id]
            for downstream_id in node.downstream:
                if downstream_id in nodes:  # Skip if node doesn't exist
                    dfs(downstream_id, path[:])

            color[node_id] = black

        for node_id in nodes:
            if color[node_id] == white:
                dfs(node_id, [])

        return cycles
