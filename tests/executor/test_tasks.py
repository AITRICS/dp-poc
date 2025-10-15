"""Module-level task functions for testing.

These functions are defined at module level so they can be pickled
and used in multiprocessing tests.
"""


def _add_one() -> int:
    """Add 1 + 1."""
    return 1 + 1


def _multiply_by_two(add_one: int) -> int:
    """Multiply input by 2."""
    return add_one * 2


def _task_a() -> int:
    """Return 10."""
    return 10


def _task_b() -> int:
    """Return 20."""
    return 20


def _task_c(task_a: int, task_b: int) -> int:
    """Add two inputs."""
    return task_a + task_b


def _failing_task() -> int:
    """Task that always fails."""
    raise ValueError("Intentional failure")


def _failsafe_task() -> int:
    """Task that fails but is failsafe."""
    raise ValueError("Failsafe failure")


def _simple_task() -> int:
    """Simple task that returns 42."""
    return 42


def _task_b_double(task_a: int) -> int:
    """Task B that doubles the input."""
    return task_a * 2
