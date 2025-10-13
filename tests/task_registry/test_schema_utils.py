"""Tests for schema extraction utilities."""

from typing import Any

from app.task_registry.utils.schema_utils import (
    extract_function_schema,
    is_async_function,
)


class TestIsAsyncFunction:
    """Test is_async_function utility."""

    def test_sync_function(self) -> None:
        """Test that sync functions are detected correctly."""

        def sync_func() -> None:
            pass

        assert is_async_function(sync_func) is False

    def test_async_function(self) -> None:
        """Test that async functions are detected correctly."""

        async def async_func() -> None:
            pass

        assert is_async_function(async_func) is True


class TestExtractFunctionSchema:
    """Test extract_function_schema utility."""

    def test_fully_typed_function(self) -> None:
        """Test extraction from fully typed function."""

        def func(a: int, b: str) -> list[int]:
            return [a] * len(b)

        input_schema, output_schema = extract_function_schema(func)

        assert input_schema is not None
        assert input_schema == {"a": int, "b": str}
        assert output_schema == list[int]

    def test_partially_typed_function_without_fill(self) -> None:
        """Test extraction from partially typed function without fill."""

        def func(a: int, b) -> str:  # type: ignore[no-untyped-def]
            return str(a + b)

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=False)

        assert input_schema is not None
        assert input_schema == {"a": int}  # Only typed parameter
        assert output_schema is str

    def test_partially_typed_function_with_fill(self) -> None:
        """Test extraction from partially typed function with fill."""

        def func(a: int, b) -> str:  # type: ignore[no-untyped-def]
            return str(a + b)

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=True)

        assert input_schema is not None
        assert input_schema == {"a": int, "b": Any}  # Missing parameter filled with Any
        assert output_schema is str

    def test_no_type_hints_without_fill(self) -> None:
        """Test extraction from function without type hints, no fill."""

        def func(a, b):  # type: ignore[no-untyped-def]
            return a + b

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=False)

        # No type hints -> returns None
        assert input_schema is None
        assert output_schema is None

    def test_no_type_hints_with_fill(self) -> None:
        """Test extraction from function without type hints, with fill."""

        def func(a, b):  # type: ignore[no-untyped-def]
            return a + b

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=True)

        # All parameters filled with Any
        assert input_schema is not None
        assert input_schema == {"a": Any, "b": Any}
        assert output_schema == Any

    def test_no_return_type_without_fill(self) -> None:
        """Test extraction when return type is missing, no fill."""

        def func(a: int, b: str):  # type: ignore[no-untyped-def]
            return a + len(b)

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=False)

        assert input_schema is not None
        assert input_schema == {"a": int, "b": str}
        assert output_schema is None

    def test_no_return_type_with_fill(self) -> None:
        """Test extraction when return type is missing, with fill."""

        def func(a: int, b: str):  # type: ignore[no-untyped-def]
            return a + len(b)

        input_schema, output_schema = extract_function_schema(func, fill_missing_with_any=True)

        assert input_schema is not None
        assert input_schema == {"a": int, "b": str}
        assert output_schema == Any  # Filled with Any

    def test_no_parameters(self) -> None:
        """Test extraction from function with no parameters."""

        def func() -> int:
            return 42

        input_schema, output_schema = extract_function_schema(func)

        assert input_schema is None  # Empty dict becomes None
        assert output_schema is int

    def test_complex_types(self) -> None:
        """Test extraction with complex types."""

        def func(items: list[dict[str, int]], multiplier: float = 2.0) -> dict[str, float]:
            return {"total": sum(d["value"] for d in items) * multiplier}

        input_schema, output_schema = extract_function_schema(func)

        assert input_schema is not None
        assert "items" in input_schema
        assert "multiplier" in input_schema
        assert output_schema == dict[str, float]
