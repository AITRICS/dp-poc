"""Tests for SchemaValidator."""

from collections.abc import Generator
from typing import Any

import pytest

from app.planner import Planner
from app.task_registry import clear_registry, get_registry, task


@pytest.fixture(autouse=True)
def clean_registry() -> Generator[None, None, None]:
    """Clean registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


class TestNamedArguments:
    """Test named argument mapping for multiple inputs."""

    def test_valid_named_arguments(self) -> None:
        """Test valid named argument mapping."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B")
        def task_b() -> str:
            return "hello"

        @task(name="C", dependencies=["A", "B"])
        def task_c(A: int, B: str) -> None:  # Parameter names match task names
            pass

        planner = Planner(registry)
        # Should pass validation
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 3

    def test_missing_parameter(self) -> None:
        """Test missing parameter for upstream dependency."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="C", dependencies=["A"])
        def task_c(wrong_name: int) -> None:  # Wrong parameter name
            pass

        planner = Planner(registry)
        # Should fail: missing parameter 'A'
        with pytest.raises(ValueError, match="missing parameter 'A'"):
            planner.create_execution_plan(validate_schemas=True)

    def test_multiple_inputs_valid(self) -> None:
        """Test multiple inputs with correct parameter names."""
        registry = get_registry()

        @task(name="extract_db")
        def extract_db() -> dict[str, int]:
            return {"count": 100}

        @task(name="extract_api")
        def extract_api() -> dict[str, str]:
            return {"status": "ok"}

        @task(name="merge", dependencies=["extract_db", "extract_api"])
        def merge(extract_db: dict[str, int], extract_api: dict[str, str]) -> dict[str, Any]:
            return {"db": extract_db, "api": extract_api}

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 3


class TestOptionalParameters:
    """Test optional parameter handling (parameters with default values)."""

    def test_optional_parameters_skipped(self) -> None:
        """Test optional parameters are skipped in validation."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(
            A: int,  # Required
            config: str = "default",  # Optional (has default)
            debug: bool = False,  # Optional (has default)
        ) -> None:
            pass

        planner = Planner(registry)
        # Should pass: only 'A' is validated
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_optional_parameter_type_mismatch_ignored(self) -> None:
        """Test type mismatch in optional parameter is ignored."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(
            A: int,  # Required, correct type
            nonexistent: str = "default",  # Optional, not from upstream
        ) -> None:
            pass

        planner = Planner(registry)
        # Should pass: optional parameters don't need upstream
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2


class TestTypeCompatibility:
    """Test type compatibility checking."""

    def test_exact_match(self) -> None:
        """Test exact type match."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: int) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_any_type_accepts_all(self) -> None:
        """Test Any type accepts any input."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: Any) -> None:  # No annotation = Any
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_any_output_to_typed_input(self) -> None:
        """Test Any output is compatible with typed input."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> Any:  # No return type = Any
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: int) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_type_mismatch(self) -> None:
        """Test incompatible types are detected."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: str) -> None:  # Expects str, gets int
            pass

        planner = Planner(registry)
        with pytest.raises(ValueError, match="Type mismatch"):
            planner.create_execution_plan(validate_schemas=True)


class TestGenericTypes:
    """Test generic type checking (List, Dict, etc.)."""

    def test_list_exact_match(self) -> None:
        """Test list with matching element type."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> list[int]:
            return [1, 2, 3]

        @task(name="B", dependencies=["A"])
        def task_b(A: list[int]) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_list_element_mismatch(self) -> None:
        """Test list with mismatched element type."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> list[int]:
            return [1, 2, 3]

        @task(name="B", dependencies=["A"])
        def task_b(A: list[str]) -> None:  # Expects list[str], gets list[int]
            pass

        planner = Planner(registry)
        with pytest.raises(ValueError, match="Type mismatch"):
            planner.create_execution_plan(validate_schemas=True)

    def test_dict_exact_match(self) -> None:
        """Test dict with matching key/value types."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> dict[str, int]:
            return {"count": 42}

        @task(name="B", dependencies=["A"])
        def task_b(A: dict[str, int]) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_dict_any_value(self) -> None:
        """Test dict with Any value type."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> dict[str, int]:
            return {"count": 42}

        @task(name="B", dependencies=["A"])
        def task_b(A: dict[str, Any]) -> None:  # Any value type
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2


class TestUnionTypes:
    """Test Union and Optional type handling."""

    def test_union_accepts_member(self) -> None:
        """Test Union type accepts member type."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: int | str) -> None:  # Union accepts int
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_union_rejects_non_member(self) -> None:
        """Test Union type rejects non-member type."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> float:  # float is not int or str
            return 3.14

        @task(name="B", dependencies=["A"])
        def task_b(A: int | str) -> None:  # Union doesn't include float
            pass

        planner = Planner(registry)
        with pytest.raises(ValueError, match="Type mismatch"):
            planner.create_execution_plan(validate_schemas=True)

    def test_optional_type(self) -> None:
        """Test Optional type (Union[X, None])."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: int | None) -> None:  # Optional[int]
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2


class TestInheritance:
    """Test inheritance and subclass checking."""

    def test_subclass_to_parent(self) -> None:
        """Test subclass is compatible with parent class (covariant)."""
        registry = get_registry()

        class Animal:
            pass

        class Dog(Animal):
            pass

        @task(name="A")
        def task_a() -> Dog:
            return Dog()

        @task(name="B", dependencies=["A"])
        def task_b(A: Animal) -> None:  # Dog is Animal
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 2

    def test_parent_to_subclass_rejected(self) -> None:
        """Test parent is not compatible with subclass (contravariant)."""
        registry = get_registry()

        class Animal:
            pass

        class Dog(Animal):
            pass

        @task(name="A")
        def task_a() -> Animal:
            return Animal()

        @task(name="B", dependencies=["A"])
        def task_b(A: Dog) -> None:  # Animal is not necessarily Dog
            pass

        planner = Planner(registry)
        with pytest.raises(ValueError, match="Type mismatch"):
            planner.create_execution_plan(validate_schemas=True)


class TestStrictMode:
    """Test strict schema validation mode."""

    def test_strict_mode_requires_schemas(self) -> None:
        """Test strict mode requires all schemas."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> Any:  # No return type
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: Any) -> None:  # No input type
            pass

        planner = Planner(registry)

        # Non-strict: should pass (Any types)
        plan = planner.create_execution_plan(validate_schemas=True, strict_schemas=False)
        assert len(plan) == 2

        # Clear and re-register
        clear_registry()

        @task(name="A")
        def task_a2() -> Any:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b2(A: Any) -> None:
            pass

        # Strict: should fail (missing schemas)
        with pytest.raises(ValueError, match="missing"):
            planner.create_execution_plan(validate_schemas=True, strict_schemas=True)


class TestDisableValidation:
    """Test disabling schema validation."""

    def test_disable_validation(self) -> None:
        """Test schema validation can be disabled."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> int:
            return 42

        @task(name="B", dependencies=["A"])
        def task_b(A: str) -> None:  # Type mismatch
            pass

        planner = Planner(registry)

        # With validation: should fail
        with pytest.raises(ValueError, match="Type mismatch"):
            planner.create_execution_plan(validate_schemas=True)

        # Without validation: should pass
        plan = planner.create_execution_plan(validate_schemas=False)
        assert len(plan) == 2


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_diamond_pattern_with_schemas(self) -> None:
        """Test diamond pattern with schema validation."""
        registry = get_registry()

        @task(name="extract")
        def extract() -> dict[str, list[int]]:
            return {"values": [1, 2, 3, 4, 5]}

        @task(name="sum", dependencies=["extract"])
        def calc_sum(extract: dict[str, list[int]]) -> int:
            return sum(extract["values"])

        @task(name="avg", dependencies=["extract"])
        def calc_avg(extract: dict[str, list[int]]) -> float:
            vals = extract["values"]
            return sum(vals) / len(vals)

        @task(name="report", dependencies=["sum", "avg"])
        def report(sum: int, avg: float) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(validate_schemas=True)
        assert len(plan) == 4
        assert plan.execution_order == [
            "extract",
            "avg",
            "sum",
            "report",
        ] or plan.execution_order == ["extract", "sum", "avg", "report"]

    def test_etl_pipeline_with_schemas(self) -> None:
        """Test ETL pipeline with schema validation."""
        registry = get_registry()

        @task(name="extract", tags=["etl"])
        def extract() -> dict[str, Any]:
            return {"data": [1, 2, 3]}

        @task(name="transform", dependencies=["extract"], tags=["etl"])
        def transform(extract: dict[str, Any]) -> list[int]:
            return extract["data"]  # type: ignore[no-any-return]

        @task(name="load", dependencies=["transform"], tags=["etl"])
        def load(transform: list[int]) -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(tags=["etl"], validate_schemas=True)
        assert len(plan) == 3
        assert plan.execution_order == ["extract", "transform", "load"]
