"""
DAG Builder.
Constructs a DAG from TaskRegistry metadata.
"""

import logging

from app.planner.domain.dag import DAG
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
    ) -> DAG:
        """
        Build a DAG from the task registry.

        Args:
            task_names: Specific task names to include. If None, include all tasks.
            tags: Filter tasks by tags. If None, no tag filtering.

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

        # Create DAG
        dag = DAG()

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
        errors = dag.validate()
        if errors:
            error_msg = "\n".join(errors)
            raise ValueError(f"DAG validation failed:\n{error_msg}")

        dag.calculate_levels()

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

        Args:
            task_names: Specific task names to include.
            tags: Filter by tags.

        Returns:
            Dictionary of task name to TaskMetadata.
        """
        all_tasks = self.registry.get_all()

        # If specific task names provided, filter by them
        if task_names is not None:
            tasks = {name: all_tasks[name] for name in task_names if name in all_tasks}
            missing = set(task_names) - set(tasks.keys())
            if missing:
                raise ValueError(f"Tasks not found in registry: {sorted(missing)}")
            return tasks

        # If tags provided, filter by them
        if tags is not None:
            tasks = {}
            for tag in tags:
                tag_tasks = self.registry.get_tasks_by_tag(tag)
                for task in tag_tasks:
                    tasks[task.name] = task
            return tasks

        # No filters, return all tasks
        return all_tasks
