"""
Schema Validator for DAG tasks.

Validates type compatibility between connected tasks in a DAG,
supporting advanced type checking including generics, unions, and inheritance.
"""

import inspect
import types
from typing import Any, Union, get_args, get_origin

from app.planner.domain.dag import DAG
from app.task_registry.domain.task_model import TaskMetadata


class SchemaValidator:
    """
    Advanced type compatibility validator for DAG tasks.

    Features:
    - Generic types (List[int], Dict[str, Any], etc.)
    - Union types (int | str, Optional[int])
    - Inheritance checking (subclass compatibility)
    - Any type handling (wildcard)
    - Named argument mapping (parameter name = upstream task name)
    - Optional parameters (with defaults) handling
    """

    @staticmethod
    def validate_dag_schemas(dag: DAG, strict: bool = False) -> list[str]:
        """
        Validate all task connections in DAG.

        Args:
            dag: DAG to validate
            strict: If True, require all tasks to have schemas

        Returns:
            List of validation errors (empty if valid)
        """
        errors: list[str] = []

        for node in dag.get_all_nodes().values():
            # Check each connection
            for upstream_id in node.upstream:
                upstream_node = dag.get_node(upstream_id)
                if not upstream_node:
                    continue

                # Validate this edge
                edge_errors = SchemaValidator.validate_edge(
                    upstream_node.task, node.task, strict=strict
                )
                errors.extend(edge_errors)

        return errors

    @staticmethod
    def validate_edge(
        upstream_task: TaskMetadata,
        downstream_task: TaskMetadata,
        strict: bool = False,
    ) -> list[str]:
        """
        Validate single connection between two tasks.

        Supports two types of dependencies:
        1. Control Flow Dependency: Only execution order matters (no data transfer)
           - Detected when: upstream has no output OR downstream has no matching parameter
        2. Data Flow Dependency: Data is passed and type compatibility is checked
           - Detected when: downstream has a parameter matching upstream task name

        Checks for Data Flow Dependencies:
        1. Parameter existence (upstream task name in downstream parameters)
        2. Optional vs required parameters (skip if has default)
        3. Output → Input type compatibility

        Args:
            upstream_task: Task producing output
            downstream_task: Task consuming input
            strict: If True, require all tasks to have schemas

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Get downstream function signature
        try:
            sig = inspect.signature(downstream_task.func)
        except (ValueError, TypeError):
            # Can't get signature (e.g., built-in function)
            return errors

        params = sig.parameters

        # Control Flow Dependency Detection:
        # If upstream has no output (None or NoneType), it's a control flow dependency
        # No data transfer validation needed
        if upstream_task.output_schema is None or upstream_task.output_schema is type(None):
            # Control flow only - just ensures execution order
            return errors

        # Control Flow Dependency Detection:
        # If downstream doesn't have a parameter matching upstream task name,
        # treat it as control flow dependency (execution order only)
        if upstream_task.name not in params:
            # Control flow only - downstream doesn't consume upstream's output
            return errors

        # Data Flow Dependency:
        # Downstream has a parameter matching upstream task name
        # Validate type compatibility
        param = params[upstream_task.name]

        # Skip validation if parameter has default value (optional data flow)
        if param.default is not inspect.Parameter.empty:
            # This parameter is optional, no need to validate
            return errors

        # Get types (use Any if not specified)
        output_type = upstream_task.output_schema or Any
        input_type = param.annotation if param.annotation != inspect.Parameter.empty else Any

        # Schema completeness check (strict mode)
        if strict:
            if upstream_task.output_schema is None:
                errors.append(f"Task '{upstream_task.name}' missing output_schema")
            if param.annotation == inspect.Parameter.empty:
                errors.append(
                    f"Task '{downstream_task.name}' parameter "
                    f"'{upstream_task.name}' missing type annotation"
                )

        # Type compatibility check for data flow
        if not SchemaValidator._is_type_compatible(output_type, input_type):
            errors.append(
                f"Type mismatch: '{upstream_task.name}' output "
                f"({SchemaValidator._type_repr(output_type)}) → "
                f"'{downstream_task.name}.{upstream_task.name}' input "
                f"({SchemaValidator._type_repr(input_type)})"
            )

        return errors

    @staticmethod
    def _is_type_compatible(output_type: type, input_type: type) -> bool:
        """
        Advanced type compatibility checking.

        Handles:
        - Any type (always compatible)
        - Exact matches
        - Subclass relationships
        - Generic types (List, Dict, etc.)
        - Union types (int | str)
        - Optional types

        Args:
            output_type: Type being produced
            input_type: Type being consumed

        Returns:
            True if compatible, False otherwise
        """
        # Any is compatible with everything
        if output_type is Any or input_type is Any:
            return True

        # Exact match
        if output_type == input_type:
            return True

        # Get origins for both types
        output_origin = get_origin(output_type)
        input_origin = get_origin(input_type)

        # Handle Union types in input (output can match any member)
        # Python 3.10+ uses types.UnionType for X | Y syntax
        # typing.Union is used for Union[X, Y] syntax
        if input_origin is Union or isinstance(input_type, types.UnionType):
            union_types = get_args(input_type)
            return any(SchemaValidator._is_type_compatible(output_type, t) for t in union_types)

        # Handle Union types in output
        if output_origin is Union or isinstance(output_type, types.UnionType):
            union_types = get_args(output_type)
            # At least one member of output union must be compatible with input
            return any(SchemaValidator._is_type_compatible(t, input_type) for t in union_types)

        # Handle Generic types (List, Dict, etc.)
        if output_origin is not None and input_origin is not None:
            # Origins must match (e.g., both list)
            if output_origin != input_origin:
                return False

            # Check generic arguments
            output_args = get_args(output_type)
            input_args = get_args(input_type)

            if len(output_args) != len(input_args):
                return False

            # All args must be compatible
            return all(
                SchemaValidator._is_type_compatible(out_arg, in_arg)
                for out_arg, in_arg in zip(output_args, input_args, strict=False)
            )

        # Handle inheritance (covariant)
        try:
            if (
                inspect.isclass(output_type)
                and inspect.isclass(input_type)
                and issubclass(output_type, input_type)
            ):
                return True
        except TypeError:
            # Not valid classes for issubclass
            pass

        return False

    @staticmethod
    def _check_union_compatibility(output_type: type, union_types: tuple[type, ...]) -> bool:
        """Check if output is compatible with any Union member."""
        return any(SchemaValidator._is_type_compatible(output_type, t) for t in union_types)

    @staticmethod
    def _type_repr(typ: type) -> str:
        """Get readable string representation of type."""
        if typ is Any:
            return "Any"
        if typ is inspect.Parameter.empty:
            return "Any (no annotation)"
        if hasattr(typ, "__name__"):
            return typ.__name__
        return str(typ)
