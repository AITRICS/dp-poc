"""
Schema extraction utilities.
Extracts type hints from functions.
"""

import inspect
from typing import Any, get_type_hints


def is_async_function(func: Any) -> bool:
    """
    Check if a function is async.

    Args:
        func: Function to check.

    Returns:
        True if function is async, False otherwise.
    """
    return inspect.iscoroutinefunction(func)


def extract_function_schema(
    func: Any, fill_missing_with_any: bool = False
) -> tuple[dict[str, type] | None, type | None]:
    """
    Extract input and output schema from function type hints.

    Args:
        func: Function to extract schema from.
        fill_missing_with_any: If True, parameters without type hints are set to Any.

    Returns:
        Tuple of (input_schema, output_schema).
        - input_schema: Dict mapping parameter names to types
        - output_schema: Return type annotation

    Example:
        # Without fill_missing_with_any
        def func(a: int, b): ...
        # Returns: ({'a': int}, None)

        # With fill_missing_with_any=True
        def func(a: int, b): ...
        # Returns: ({'a': int, 'b': Any}, None)
    """
    try:
        type_hints = get_type_hints(func)
    except Exception:
        # If type hints cannot be extracted, return None
        return None, None

    # Extract input schema (parameters)
    input_schema: dict[str, type] = {}
    sig = inspect.signature(func)
    for param_name, _param in sig.parameters.items():
        if param_name in type_hints:
            input_schema[param_name] = type_hints[param_name]
        elif fill_missing_with_any:
            input_schema[param_name] = Any

    # Extract output schema (return type)
    output_schema = type_hints.get("return")
    if output_schema is None and fill_missing_with_any:
        output_schema = Any

    return input_schema if input_schema else None, output_schema
