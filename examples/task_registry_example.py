"""
Task Registry Example

This example demonstrates how to use the Task Registry system to:
1. Register tasks using the @task decorator
2. Define task dependencies
3. Execute tasks
4. Validate dependencies
"""

import asyncio

from app.task_registry import (
    ExcutableTask,
    get_registry,
    task,
    validate_all_tasks,
)


# Define tasks with dependencies
@task(name="extract_data", tags=["etl", "extraction"])
def extract() -> dict[str, list[int]]:
    """Extract data from source."""
    print("Extracting data...")
    return {"values": [1, 2, 3, 4, 5]}


@task(name="transform_data", tags=["etl", "transformation"], dependencies=["extract_data"])
def transform(data: dict[str, list[int]]) -> dict[str, float]:
    """Transform extracted data."""
    print("Transforming data...")
    values = data["values"]
    return {
        "sum": float(sum(values)),
        "count": float(len(values)),
        "avg": sum(values) / len(values),
    }


@task(name="load_data", tags=["etl", "loading"], dependencies=["transform_data"])
async def load(data: dict[str, float]) -> None:
    """Load transformed data to destination (async example)."""
    print("Loading data...")
    await asyncio.sleep(0.1)  # Simulate async I/O
    print(f"Loaded: {data}")


@task(name="generate_report", tags=["reporting"])
async def report() -> None:
    """Generate a report (independent task)."""
    print("Generating report...")
    await asyncio.sleep(0.1)
    print("Report generated!")


async def main() -> None:
    """Main execution function."""
    print("=" * 60)
    print("Task Registry Example")
    print("=" * 60)

    # Get the global registry
    registry = get_registry()

    # Display registered tasks
    print("\n1. Registered Tasks:")
    for task_meta in registry.get_all().values():
        print(f"   - {task_meta.name} (async: {task_meta.is_async})")
        if task_meta.dependencies:
            print(f"     Dependencies: {task_meta.dependencies}")
        if task_meta.tags:
            print(f"     Tags: {task_meta.tags}")

    # Validate dependencies
    print("\n2. Validating Dependencies:")
    errors = validate_all_tasks()
    if errors:
        print(f"   Validation errors: {errors}")
    else:
        print("   ✓ All dependencies are valid")

    # Get tasks by tag
    print("\n3. Tasks with 'etl' tag:")
    etl_tasks = registry.get_tasks_by_tag("etl")
    for task_meta in etl_tasks:
        print(f"   - {task_meta.name}")

    # Execute tasks manually (in correct order)
    print("\n4. Executing Tasks:")
    print("\n   Step 1: Extract")
    extract_task = registry.get("extract_data")
    if extract_task:
        executor = ExcutableTask(extract_task)
        extract_result = await executor.execute()
        print(f"   Result: {extract_result}")

    print("\n   Step 2: Transform")
    transform_task = registry.get("transform_data")
    if transform_task:
        executor = ExcutableTask(transform_task)
        transform_result = await executor.execute(extract_result)
        print(f"   Result: {transform_result}")

    print("\n   Step 3: Load")
    load_task = registry.get("load_data")
    if load_task:
        executor = ExcutableTask(load_task)
        await executor.execute(transform_result)
        print()

    print("   Step 4: Generate Report (independent)")
    report_task = registry.get("generate_report")
    if report_task:
        executor = ExcutableTask(report_task)
        await executor.execute()
        print()

    # Display registry statistics
    print("5. Registry Statistics:")
    stats = registry.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
