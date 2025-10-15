"""
Planner system for DAG-based task execution planning.

This module provides tools to analyze task dependencies, build DAGs,
and create execution plans with parallel execution capabilities.
"""

from app.planner.domain.dag import DAG
from app.planner.domain.dag_builder import DAGBuilder
from app.planner.domain.execution_plan import ExecutionPlan
from app.planner.domain.node import Node
from app.planner.domain.planner import Planner
from app.planner.domain.schema_validator import SchemaValidator

# Global planner instance (lazy initialization)
_global_planner: Planner | None = None


def get_planner() -> Planner:
    """
    Get the global planner instance.

    The planner is lazily initialized with a DAG builder from the global task registry.

    Returns:
        The global Planner instance.

    Example:
        from app.planner import get_planner

        planner = get_planner()
        plan = planner.create_execution_plan(tags=["etl"])
    """
    global _global_planner
    if _global_planner is None:
        from app.task_registry import get_registry

        dag_builder = DAGBuilder(get_registry())
        _global_planner = Planner(dag_builder)
    return _global_planner


def reset_planner() -> None:
    """
    Reset the global planner instance.

    Useful for testing or when the task registry has been cleared.
    """
    global _global_planner
    _global_planner = None


__all__ = [
    "Node",
    "DAG",
    "DAGBuilder",
    "ExecutionPlan",
    "Planner",
    "SchemaValidator",
    "get_planner",
    "reset_planner",
]
