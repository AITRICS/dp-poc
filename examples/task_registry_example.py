"""
Task Registry Example

This example demonstrates how to use the Task Registry system to:
1. Register tasks using the @task decorator
2. Define task dependencies
3. Execute tasks
4. Validate dependencies
"""

import asyncio

from app.task_registry import get_registry, task, validate_all_tasks


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

    # Inspect task metadata
    print("\n4. Task Metadata Details:")
    extract_task = registry.get("extract_data")
    if extract_task:
        print(f"\n   Task: {extract_task.name}")
        print(f"   - Function: {extract_task.func.__name__}")
        print(f"   - Is Async: {extract_task.is_async}")
        print(f"   - Input Schema: {extract_task.input_schema}")
        print(f"   - Output Schema: {extract_task.output_schema}")
        print(f"   - Dependencies: {extract_task.dependencies}")

    transform_task = registry.get("transform_data")
    if transform_task:
        print(f"\n   Task: {transform_task.name}")
        print(f"   - Function: {transform_task.func.__name__}")
        print(f"   - Is Async: {transform_task.is_async}")
        print(f"   - Input Schema: {transform_task.input_schema}")
        print(f"   - Output Schema: {transform_task.output_schema}")
        print(f"   - Dependencies: {transform_task.dependencies}")

    # Display registry statistics
    print("\n5. Registry Statistics:")
    stats = registry.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
