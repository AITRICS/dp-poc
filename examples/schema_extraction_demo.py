"""
Schema Extraction Demo

This example demonstrates how the Task Registry automatically extracts
input and output schemas from function type hints.
"""

import asyncio
from typing import Any

from app.task_registry import get_registry, task


# Task with full type hints
@task(name="typed_task")
def add_numbers(a: int, b: int) -> int:
    """Add two numbers together."""
    return a + b


# Async task with type hints
@task(name="async_typed_task")
async def fetch_data(url: str, timeout: float) -> dict[str, Any]:
    """Fetch data from URL."""
    await asyncio.sleep(0.1)
    return {"url": url, "timeout": timeout, "status": "success"}


# Task with complex types
@task(name="complex_types_task")
def process_data(items: list[int], multiplier: float = 2.0) -> dict[str, float]:
    """Process a list of numbers."""
    total = sum(items) * multiplier
    avg = total / len(items) if items else 0
    return {"total": total, "average": avg}


# Task with partial type hints
@task(name="partial_hints_task")
def greet(name: str, age: int, city: str) -> str:
    """Generate greeting message."""
    return f"Hello {name}, {age} years old from {city}"


# Task with no type hints (schema will be filled with Any)
@task(name="no_hints_task")
def legacy_function(x, y):  # type: ignore[no-untyped-def]  # noqa: ANN201
    """Legacy function without type hints - will be auto-filled with Any."""
    return x + y


async def main() -> None:
    """Main execution function."""
    print("=" * 60)
    print("Schema Extraction Demo")
    print("=" * 60)

    registry = get_registry()

    print("\nAutomatically Extracted Schemas:\n")

    for task_meta in registry.get_all().values():
        print(f"Task: {task_meta.name}")
        print(f"  Async: {task_meta.is_async}")
        print(f"  Input Schema: {task_meta.input_schema}")
        print(f"  Output Schema: {task_meta.output_schema}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
