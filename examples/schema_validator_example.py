"""
Schema Validator Example

This example demonstrates how to use the Schema Validator to:
1. Validate type compatibility between connected tasks
2. Handle multiple inputs with named arguments
3. Use optional parameters with default values
4. Support generic types, Union types, and inheritance
"""

from typing import Any

from app.planner import get_planner
from app.task_registry import clear_registry, task


def main() -> None:
    """Main execution function."""
    print("=" * 60)
    print("Schema Validator Example")
    print("=" * 60)

    # Clear registry for clean start
    clear_registry()

    # Example 1: Basic schema validation
    print("\n1. Basic Schema Validation:")
    print("   Creating tasks with explicit types...")

    @task(name="extract_data")
    def extract() -> dict[str, list[int]]:
        """Extract data from source."""
        return {"values": [1, 2, 3, 4, 5]}

    @task(name="calculate_sum", dependencies=["extract_data"])
    def calc_sum(extract_data: dict[str, list[int]]) -> int:
        """Calculate sum - parameter name matches task name."""
        return sum(extract_data["values"])

    planner = get_planner()

    # With schema validation
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print(f"   ✓ Validation passed: {len(plan)} tasks")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 2: Multiple inputs (diamond pattern)
    print("\n2. Multiple Inputs (Named Arguments):")
    clear_registry()

    @task(name="extract")
    def extract2() -> dict[str, list[int]]:
        return {"values": [1, 2, 3, 4, 5]}

    @task(name="sum", dependencies=["extract"])
    def calc_sum2(extract: dict[str, list[int]]) -> int:
        """Parameter name 'extract' matches task name 'extract'."""
        return sum(extract["values"])

    @task(name="avg", dependencies=["extract"])
    def calc_avg(extract: dict[str, list[int]]) -> float:
        """Parameter name 'extract' matches task name 'extract'."""
        vals = extract["values"]
        return sum(vals) / len(vals)

    @task(name="report", dependencies=["sum", "avg"])
    def report(sum: int, avg: float) -> None:
        """Two parameters matching two upstream task names."""
        print(f"   Sum: {sum}, Avg: {avg}")

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print(f"   ✓ Diamond pattern validated: {len(plan)} tasks")
        print(f"   Execution order: {plan.execution_order}")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 3: Optional parameters (with defaults)
    print("\n3. Optional Parameters:")
    clear_registry()

    @task(name="process_data")
    def process_data() -> list[int]:
        return [1, 2, 3]

    @task(name="transform", dependencies=["process_data"])
    def transform(
        process_data: list[int],  # Required - from upstream
        multiplier: int = 2,  # Optional - has default
        debug: bool = False,  # Optional - has default
    ) -> list[int]:
        """Optional parameters are skipped in validation."""
        result = [x * multiplier for x in process_data]
        if debug:
            print(f"   Debug: transformed {process_data} → {result}")
        return result

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print(f"   ✓ Optional parameters handled: {len(plan)} tasks")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 4: Union types
    print("\n4. Union Types:")
    clear_registry()

    @task(name="get_value")
    def get_value() -> int:
        return 42

    @task(name="process", dependencies=["get_value"])
    def process_union(get_value: int | str) -> None:
        """Union type accepts int."""
        print(f"   Received: {get_value} (type: {type(get_value).__name__})")

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print("   ✓ Union type validated: int → int | str")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 5: Generic types
    print("\n5. Generic Types:")
    clear_registry()

    @task(name="create_list")
    def create_list() -> list[int]:
        return [1, 2, 3]

    @task(name="process_list", dependencies=["create_list"])
    def process_list(create_list: list[int]) -> int:
        """Generic types must match exactly."""
        return sum(create_list)

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print("   ✓ Generic type validated: list[int] → list[int]")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 6: Any type (wildcard)
    print("\n6. Any Type (Wildcard):")
    clear_registry()

    @task(name="dynamic_source")
    def dynamic_source() -> Any:  # No return type = Any
        return {"flexible": "data"}

    @task(name="flexible_consumer", dependencies=["dynamic_source"])
    def flexible_consumer(dynamic_source: Any) -> None:
        """Any type accepts anything."""
        print(f"   Received: {dynamic_source}")

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print("   ✓ Any type validated: Any → Any")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 7: Inheritance
    print("\n7. Inheritance (Covariant):")
    clear_registry()

    class Animal:
        pass

    class Dog(Animal):
        pass

    @task(name="get_dog")
    def get_dog() -> Dog:
        return Dog()

    @task(name="handle_animal", dependencies=["get_dog"])
    def handle_animal(get_dog: Animal) -> None:
        """Dog is compatible with Animal (subclass → parent)."""
        print(f"   Received animal: {type(get_dog).__name__}")

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print("   ✓ Inheritance validated: Dog → Animal")
    except ValueError as e:
        print(f"   ✗ Validation failed: {e}")

    # Example 8: Type mismatch (intentional error)
    print("\n8. Type Mismatch (Expected to Fail):")
    clear_registry()

    @task(name="get_number")
    def get_number() -> int:
        return 42

    @task(name="expect_string", dependencies=["get_number"])
    def expect_string(get_number: str) -> None:
        """Expects str but gets int - should fail."""
        print(get_number)

    planner = get_planner()
    try:
        plan = planner.create_execution_plan(validate_schemas=True)
        print("   ✗ Unexpected: validation should have failed!")
    except ValueError:
        print("   ✓ Expected error caught: Type mismatch detected")

    # Example 9: Strict mode
    print("\n9. Strict Mode (Requires All Schemas):")
    clear_registry()

    @task(name="no_type")
    def no_type() -> Any:  # Missing return type
        return 42

    @task(name="consumer", dependencies=["no_type"])
    def consumer(no_type: Any) -> None:  # Missing input type
        pass

    planner = get_planner()

    # Non-strict (default): passes with Any
    try:
        plan = planner.create_execution_plan(validate_schemas=True, strict_schemas=False)
        print("   ✓ Non-strict mode: accepted (Any types)")
    except ValueError:
        print("   ✗ Non-strict mode: unexpected failure")

    # Strict: fails with missing schemas
    try:
        plan = planner.create_execution_plan(validate_schemas=True, strict_schemas=True)
        print("   ✗ Strict mode: should have failed!")
    except ValueError:
        print("   ✓ Strict mode: missing schemas detected")

    print("\n" + "=" * 60)
    print("Schema validation provides:")
    print("  - Type safety between connected tasks")
    print("  - Named argument mapping (parameter = task name)")
    print("  - Optional parameter support (defaults ignored)")
    print("  - Advanced type checking (generics, unions, inheritance)")
    print("  - Opt-in validation (validate_schemas=True)")
    print("=" * 60)


if __name__ == "__main__":
    main()
