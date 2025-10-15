"""
Planner - Main interface for execution planning.
Orchestrates DAG building and execution plan creation.
"""

import logging

from app.planner.domain.dag_builder import DAGBuilder
from app.planner.domain.dag_validator import DAGValidator
from app.planner.domain.execution_plan import ExecutionPlan
from app.planner.domain.schema_validator import SchemaValidator


class Planner:
    """
    Main planner interface.

    Orchestrates the creation of execution plans from DAG builder.
    Provides both validation-only and full plan creation methods.
    """

    def __init__(self, dag_builder: DAGBuilder) -> None:
        """
        Initialize planner.

        Args:
            dag_builder: DAG builder to use for planning.
        """
        self.dag_builder = dag_builder

    def create_execution_plan(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
        root_tasks: list[str] | None = None,
        include_upstream: bool = False,
        dag_id: str | None = None,
        validate_schemas: bool = False,
        strict_schemas: bool = False,
    ) -> ExecutionPlan:
        """
        Create an execution plan.

        Builds a DAG from the task registry, validates it, and creates
        an execution plan with topological ordering and parallel levels.

        Priority:
        1. If root_tasks provided, build DAG from those roots
        2. Otherwise, build DAG from task_names or tags

        Args:
            task_names: Specific task names to include. If None, include all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.
            root_tasks: Start from specific root tasks and include their downstream.
            include_upstream: If True (with root_tasks), include upstream dependencies.
            dag_id: Optional DAG ID. If not provided, content hash will be used.
            validate_schemas: If True, validate task schema compatibility.
            strict_schemas: If True, require all tasks to have schemas.

        Returns:
            ExecutionPlan ready for execution.

        Raises:
            ValueError: If DAG validation fails or no tasks match criteria.

        Example:
            # Using tags
            plan = planner.create_execution_plan(tags=["etl"])

            # Using root tasks
            plan = planner.create_execution_plan(root_tasks=["extract"])

            # With explicit DAG ID and schema validation
            plan = planner.create_execution_plan(
                root_tasks=["extract"],
                dag_id="daily_etl",
                validate_schemas=True
            )
        """
        logging.info("Creating execution plan...")

        # Build DAG
        if root_tasks:
            dag = self.dag_builder.build_dag_from_roots(
                root_tasks=root_tasks,
                include_upstream=include_upstream,
                dag_id=dag_id,
            )
        else:
            dag = self.dag_builder.build_dag(
                task_names=task_names,
                tags=tags,
                dag_id=dag_id,
            )

        # Validate structure
        errors = DAGValidator.validate(dag)
        if errors:
            raise ValueError("DAG validation failed:\n" + "\n".join(errors))

        # Validate schemas
        if validate_schemas:
            schema_errors = SchemaValidator.validate_dag_schemas(dag, strict=strict_schemas)
            if schema_errors:
                raise ValueError("Schema validation failed:\n" + "\n".join(schema_errors))

        # Create execution plan
        plan = ExecutionPlan(dag=dag)

        logging.info(f"Execution plan created: {len(plan)} nodes, {plan.get_total_levels()} levels")

        return plan

    def validate_plan(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
        root_tasks: list[str] | None = None,
        include_upstream: bool = False,
    ) -> list[str]:
        """
        Validate task dependencies without creating a full execution plan.

        Useful for checking DAG validity before execution.

        Args:
            task_names: Specific task names to validate. If None, validate all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.
            root_tasks: Start from specific root tasks.
            include_upstream: If True (with root_tasks), include upstream dependencies.

        Returns:
            List of validation error messages. Empty list if valid.

        Example:
            dag_builder = DAGBuilder(registry)
            planner = Planner(dag_builder)
            errors = planner.validate_plan(tags=["etl"])
            if errors:
                print("Validation failed:")
                for error in errors:
                    print(f"  - {error}")
            else:
                print("Validation passed!")
        """
        try:
            if root_tasks:
                _ = self.dag_builder.build_dag_from_roots(
                    root_tasks=root_tasks,
                    include_upstream=include_upstream,
                )
            else:
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
        root_tasks: list[str] | None = None,
        include_upstream: bool = False,
    ) -> int:
        """
        Get the number of tasks that would be included in a plan.

        Args:
            task_names: Specific task names to count. If None, count all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.
            root_tasks: Start from specific root tasks.
            include_upstream: If True (with root_tasks), include upstream dependencies.

        Returns:
            Number of tasks.
        """
        try:
            if root_tasks:
                dag = self.dag_builder.build_dag_from_roots(
                    root_tasks=root_tasks,
                    include_upstream=include_upstream,
                )
            else:
                dag = self.dag_builder.build_dag(task_names=task_names, tags=tags)
            return len(dag)
        except ValueError:
            return 0
