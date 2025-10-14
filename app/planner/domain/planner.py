"""
Planner - Main interface for execution planning.
Orchestrates DAG building and execution plan creation.
"""

import logging

from app.planner.domain.dag_builder import DAGBuilder
from app.planner.domain.execution_plan import ExecutionPlan
from app.task_registry.domain.registry_port import RegistryPort


class Planner:
    """
    Main planner interface.

    Orchestrates the creation of execution plans from task registry.
    Provides both validation-only and full plan creation methods.
    """

    def __init__(self, registry: RegistryPort) -> None:
        """
        Initialize planner.

        Args:
            registry: Task registry to use for planning.
        """
        self.registry = registry
        self.dag_builder = DAGBuilder(registry)

    def create_execution_plan(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> ExecutionPlan:
        """
        Create an execution plan.

        Builds a DAG from the task registry, validates it, and creates
        an execution plan with topological ordering and parallel levels.

        Args:
            task_names: Specific task names to include. If None, include all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.

        Returns:
            ExecutionPlan ready for execution.

        Raises:
            ValueError: If DAG validation fails or no tasks match criteria.

        Example:
            planner = Planner(registry)
            plan = planner.create_execution_plan(tags=["etl"])
            print(f"Execution order: {plan.execution_order}")
            print(f"Parallel levels: {plan.parallel_levels}")
        """
        logging.info("Creating execution plan...")

        # Build DAG (includes validation)
        dag = self.dag_builder.build_dag(task_names=task_names, tags=tags)

        # Create execution plan
        plan = ExecutionPlan(dag=dag)

        logging.info(f"Execution plan created: {len(plan)} nodes, {plan.get_total_levels()} levels")

        return plan

    def validate_plan(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> list[str]:
        """
        Validate task dependencies without creating a full execution plan.

        Useful for checking DAG validity before execution.

        Args:
            task_names: Specific task names to validate. If None, validate all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.

        Returns:
            List of validation error messages. Empty list if valid.

        Example:
            planner = Planner(registry)
            errors = planner.validate_plan(tags=["etl"])
            if errors:
                print("Validation failed:")
                for error in errors:
                    print(f"  - {error}")
            else:
                print("Validation passed!")
        """
        try:
            _ = self.dag_builder.build_dag(task_names=task_names, tags=tags)
            # DAG builder already validates, so if we get here, it's valid
            return []
        except ValueError as e:
            # Parse error message to extract individual errors
            error_msg = str(e)
            if "DAG validation failed:" in error_msg:
                # Extract errors from the message
                errors_text = error_msg.split("DAG validation failed:\n", 1)[1]
                return errors_text.split("\n")
            return [error_msg]

    def get_task_count(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """
        Get the number of tasks that would be included in a plan.

        Args:
            task_names: Specific task names to count. If None, count all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.

        Returns:
            Number of tasks.
        """
        try:
            dag = self.dag_builder.build_dag(task_names=task_names, tags=tags)
            return len(dag)
        except ValueError:
            return 0
