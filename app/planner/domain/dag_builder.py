"""
DAG Builder.
Constructs a DAG from TaskRegistry metadata.
"""

import logging

from app.planner.domain.dag import DAG
from app.planner.domain.dag_analyzer import DAGAnalyzer
from app.planner.domain.dag_validator import DAGValidator
from app.planner.domain.node import Node
from app.task_registry.domain.registry_port import RegistryPort
from app.task_registry.domain.task_model import TaskMetadata


class DAGBuilder:
    """
    Builds a DAG from TaskRegistry.

    Takes TaskMetadata from the registry and constructs a DAG
    with proper upstream/downstream relationships.
    """

    def __init__(self, registry: RegistryPort) -> None:
        """
        Initialize DAG builder.

        Args:
            registry: Task registry to build DAG from.
        """
        self.registry = registry

    def build_dag(
        self,
        task_names: list[str] | None = None,
        tags: list[str] | None = None,
        dag_id: str | None = None,
    ) -> DAG:
        """
        Build a DAG from the task registry.

        Args:
            task_names: Specific task names to include. If None, include all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.
            dag_id: Optional DAG ID. If not provided, content hash will be used.

        Returns:
            Constructed DAG.

        Raises:
            ValueError: If no tasks match the criteria or if task dependencies are invalid.
        """
        # Get tasks from registry
        tasks = self._get_tasks(task_names, tags)

        if not tasks:
            raise ValueError("No tasks found matching the criteria")

        logging.info(f"Building DAG with {len(tasks)} tasks")

        # Create DAG with optional user-provided ID
        dag = DAG(dag_id=dag_id)

        # First pass: Create nodes
        for task in tasks.values():
            node = Node(
                task=task,
                node_id=task.name,
                upstream=set(task.dependencies),
                downstream=set(),
            )
            dag.add_node(node)

        # Second pass: Build downstream relationships
        for node_id, node in dag.get_all_nodes().items():
            for upstream_id in node.upstream:
                upstream_node = dag.get_node(upstream_id)
                if upstream_node:
                    upstream_node.downstream.add(node_id)

        # Validate and calculate levels
        errors = DAGValidator.validate(dag)
        if errors:
            error_msg = "\n".join(errors)
            raise ValueError(f"DAG validation failed:\n{error_msg}")

        DAGAnalyzer.calculate_levels(dag)

        logging.info(
            f"Successfully built DAG with {len(dag)} nodes, "
            f"{len(dag.get_root_nodes())} roots, "
            f"{len(dag.get_leaf_nodes())} leaves"
        )

        return dag

    def _get_tasks(
        self,
        task_names: list[str] | None,
        tags: list[str] | None,
    ) -> dict[str, TaskMetadata]:
        """
        Get tasks from registry based on filters.

        Priority:
        1. If specific task_names provided, use only those tasks
        2. If tags provided, use tasks with those tags
        3. Otherwise, use all tasks

        Args:
            task_names: Specific task names to include.
            tags: Filter by tags.

        Returns:
            Dictionary of task name to TaskMetadata.
        """
        all_tasks = self.registry.get_all()

        # Priority 1: Specific task names
        if task_names is not None:
            tasks = {name: all_tasks[name] for name in task_names if name in all_tasks}
            missing = set(task_names) - set(tasks.keys())
            if missing:
                raise ValueError(f"Tasks not found in registry: {sorted(missing)}")
            return tasks

        # Priority 2: Tags filter
        if tags is not None:
            tasks = {}
            for tag in tags:
                tag_tasks = self.registry.get_tasks_by_tag(tag)
                for task in tag_tasks:
                    tasks[task.name] = task
            return tasks

        # No filters, return all tasks
        return all_tasks

    def build_dag_from_roots(
        self,
        root_tasks: list[str],
        include_upstream: bool = False,
        dag_id: str | None = None,
    ) -> DAG:
        """
        Build a DAG starting from specific root tasks.

        This method creates a DAG containing only the specified root tasks
        and their descendants (downstream tasks). Optionally includes upstream tasks.

        Args:
            root_tasks: List of task names to start from.
            include_upstream: If True, include upstream dependencies of root tasks.
            dag_id: Optional DAG ID. If not provided, content hash will be used.

        Returns:
            DAG containing only the connected component from root tasks.

        Raises:
            ValueError: If any root task doesn't exist or validation fails.

        Example:
            # Only include extract and its downstream
            dag = builder.build_dag_from_roots(root_tasks=["extract"])

            # Include extract's upstream dependencies as well
            dag = builder.build_dag_from_roots(
                root_tasks=["transform"],
                include_upstream=True
            )
        """
        all_tasks = self.registry.get_all()

        # Validate root tasks exist
        missing = set(root_tasks) - set(all_tasks.keys())
        if missing:
            raise ValueError(f"Root tasks not found in registry: {sorted(missing)}")

        # Collect tasks to include
        tasks_to_include: set[str] = set(root_tasks)

        # Add downstream tasks (recursively)
        def add_downstream(task_name: str) -> None:
            """Recursively add all downstream tasks."""
            task = all_tasks.get(task_name)
            if not task:
                return

            # Find tasks that depend on this task
            for other_name, other_task in all_tasks.items():
                if task_name in other_task.dependencies and other_name not in tasks_to_include:
                    tasks_to_include.add(other_name)
                    add_downstream(other_name)

        # Add upstream tasks (recursively) if requested
        def add_upstream(task_name: str) -> None:
            """Recursively add all upstream tasks."""
            task = all_tasks.get(task_name)
            if not task:
                return

            for dep in task.dependencies:
                if dep not in tasks_to_include:
                    tasks_to_include.add(dep)
                    add_upstream(dep)

        # Collect all relevant tasks
        for root_task in root_tasks:
            add_downstream(root_task)
            if include_upstream:
                add_upstream(root_task)

        # Build DAG with selected tasks
        tasks = {name: all_tasks[name] for name in tasks_to_include if name in all_tasks}

        if not tasks:
            raise ValueError("No tasks found in the connected component")

        logging.info(
            f"Building DAG from roots {root_tasks} "
            f"(include_upstream={include_upstream}): {len(tasks)} tasks"
        )

        # Create DAG with optional user-provided ID
        dag = DAG(dag_id=dag_id)

        # First pass: Create nodes
        for task in tasks.values():
            node = Node(
                task=task,
                node_id=task.name,
                upstream=set(task.dependencies) & tasks_to_include,  # Only include if in tasks
                downstream=set(),
            )
            dag.add_node(node)

        # Second pass: Build downstream relationships
        for node_id, node in dag.get_all_nodes().items():
            for upstream_id in node.upstream:
                upstream_node = dag.get_node(upstream_id)
                if upstream_node:
                    upstream_node.downstream.add(node_id)

        # Validate and calculate levels
        errors = DAGValidator.validate(dag)
        if errors:
            error_msg = "\n".join(errors)
            raise ValueError(f"DAG validation failed:\n{error_msg}")

        DAGAnalyzer.calculate_levels(dag)

        logging.info(
            f"Successfully built DAG with {len(dag)} nodes, "
            f"{len(dag.get_root_nodes())} roots, "
            f"{len(dag.get_leaf_nodes())} leaves"
        )

        return dag
