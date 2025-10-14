"""
Planner Example

This example demonstrates how to use the Planner system to:
1. Build a DAG from registered tasks
2. Validate task dependencies
3. Create execution plans with parallel execution levels
4. Simulate task execution
"""

from app.planner import get_planner
from app.task_registry import clear_registry, get_registry, task


def main() -> None:
    """Main execution function."""
    print("=" * 60)
    print("Planner Example")
    print("=" * 60)

    # Clear registry for clean start
    clear_registry()

    # Define tasks with dependencies
    @task(name="extract_data", tags=["etl", "extraction"])
    def extract() -> dict[str, list[int]]:
        """Extract data from source."""
        print("  Extracting data...")
        return {"values": [1, 2, 3, 4, 5]}

    @task(
        name="transform_sum",
        tags=["etl", "transformation"],
        dependencies=["extract_data"],
    )
    def transform_sum(data: dict[str, list[int]]) -> float:
        """Calculate sum of values."""
        print("  Calculating sum...")
        return float(sum(data["values"]))

    @task(
        name="transform_avg",
        tags=["etl", "transformation"],
        dependencies=["extract_data"],
    )
    def transform_avg(data: dict[str, list[int]]) -> float:
        """Calculate average of values."""
        print("  Calculating average...")
        return sum(data["values"]) / len(data["values"])

    @task(
        name="load_results",
        tags=["etl", "loading"],
        dependencies=["transform_sum", "transform_avg"],
    )
    def load(sum_val: float, avg_val: float) -> None:
        """Load transformed data."""
        print(f"  Loading results: sum={sum_val}, avg={avg_val}")

    @task(name="report", tags=["reporting"])
    def report() -> None:
        """Generate a report (independent task)."""
        print("  Generating report...")

    print("\n1. Registered Tasks:")
    registry = get_registry()
    for task_meta in registry.get_all().values():
        print(f"   - {task_meta.name}")
        if task_meta.dependencies:
            print(f"     Dependencies: {task_meta.dependencies}")
        if task_meta.tags:
            print(f"     Tags: {task_meta.tags}")

    # Create planner
    planner = get_planner()

    # Validate dependencies
    print("\n2. Validating ETL Pipeline:")
    errors = planner.validate_plan(tags=["etl"])
    if errors:
        print(f"   Validation errors: {errors}")
        return
    else:
        print("   ✓ All dependencies are valid")

    # Create execution plan
    print("\n3. Creating Execution Plan:")
    plan = planner.create_execution_plan(tags=["etl"])

    print(f"   Total tasks: {len(plan)}")
    print(f"   Execution order: {plan.execution_order}")
    print(f"   Parallel levels: {plan.parallel_levels}")

    # Analyze parallel execution
    print("\n4. Parallel Execution Analysis:")
    for i, level_tasks in enumerate(plan.parallel_levels):
        print(f"\n   Level {i}:")
        print(f"   - Tasks: {level_tasks}")
        print(f"   - Can execute in parallel: {len(level_tasks)} task(s)")

        for task_id in level_tasks:
            node = plan.get_node(task_id)
            if node:
                print(f"     * {task_id}")
                if node.upstream:
                    print(f"       Depends on: {node.upstream}")
                if node.downstream:
                    print(f"       Enables: {node.downstream}")

    # Simulate execution
    print("\n5. Simulating Execution:")
    completed: set[str] = set()

    for level_num, level_tasks in enumerate(plan.parallel_levels):
        print(f"\n   Executing Level {level_num}:")

        # Check all tasks are ready
        for task_id in level_tasks:
            node = plan.get_node(task_id)
            if node and not node.is_ready(completed):
                print(f"   ERROR: {task_id} is not ready!")
                return

        # Execute (simulate)
        for task_id in level_tasks:
            node = plan.get_node(task_id)
            if node:
                print(f"   - Executing {task_id}...")

        # Mark as completed
        completed.update(level_tasks)
        print(f"   ✓ Level {level_num} completed")

    # Verify all tasks completed
    ready_nodes = plan.get_ready_nodes(completed)
    if ready_nodes:
        print(f"\n   WARNING: {len(ready_nodes)} tasks still ready")
    else:
        print("\n   ✓ All tasks completed successfully!")

    # Show DAG statistics
    print("\n6. DAG Statistics:")
    dag_dict = plan.dag.to_dict()
    print(f"   - Total nodes: {dag_dict['node_count']}")
    print(f"   - Root nodes: {dag_dict['root_nodes']}")
    print(f"   - Leaf nodes: {dag_dict['leaf_nodes']}")
    print(f"   - Execution levels: {plan.get_total_levels()}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
