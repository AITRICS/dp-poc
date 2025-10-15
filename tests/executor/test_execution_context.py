"""Tests for ExecutionContext model."""

from pathlib import Path

import pytest

from app.executor.domain.execution_context import ExecutionContext
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.planner.domain.execution_plan import ExecutionPlan
from app.planner.domain.planner import Planner
from app.task_registry.domain.task_model import TaskMetadata
from app.task_registry.infrastructure.task_registry import TaskRegistry


@pytest.fixture
def sample_registry() -> TaskRegistry:
    """Create a sample task registry for testing."""
    registry = TaskRegistry()

    def task_a() -> int:
        return 1

    def task_b(a: int) -> int:
        return a * 2

    def task_c(b: int) -> int:
        return b + 1

    registry.register(TaskMetadata(name="A", func=task_a, dependencies=[]))
    registry.register(TaskMetadata(name="B", func=task_b, dependencies=["A"]))
    registry.register(TaskMetadata(name="C", func=task_c, dependencies=["B"]))

    return registry


@pytest.fixture
def sample_execution_plan(sample_registry: TaskRegistry) -> ExecutionPlan:
    """Create a sample execution plan for testing."""
    planner = Planner(sample_registry)
    return planner.create_execution_plan()


@pytest.fixture
def sample_io_manager(tmp_path: Path) -> FilesystemIOManager:
    """Create a sample I/O manager for testing."""
    return FilesystemIOManager(base_path=tmp_path)


@pytest.fixture
def sample_execution_context(
    sample_execution_plan: ExecutionPlan,
    sample_io_manager: FilesystemIOManager,
) -> ExecutionContext:
    """Create a sample execution context for testing."""
    return ExecutionContext(
        run_id="run_001",
        dag_id="dag_123",
        execution_plan=sample_execution_plan,
        io_manager=sample_io_manager,
    )


class TestExecutionContext:
    """Test cases for ExecutionContext model."""

    def test_create_execution_context(
        self,
        sample_execution_context: ExecutionContext,
    ) -> None:
        """Test creating a basic execution context."""
        ctx = sample_execution_context

        assert ctx.run_id == "run_001"
        assert ctx.dag_id == "dag_123"
        assert ctx.execution_plan is not None
        assert ctx.io_manager is not None
        assert ctx.metadata == {}

    def test_create_context_with_metadata(
        self,
        sample_execution_plan: ExecutionPlan,
        sample_io_manager: FilesystemIOManager,
    ) -> None:
        """Test creating context with metadata."""
        metadata = {"user": "test_user", "environment": "dev"}
        ctx = ExecutionContext(
            run_id="run_001",
            dag_id="dag_123",
            execution_plan=sample_execution_plan,
            io_manager=sample_io_manager,
            metadata=metadata,
        )

        assert ctx.metadata == metadata

    def test_empty_run_id_raises_error(
        self,
        sample_execution_plan: ExecutionPlan,
        sample_io_manager: FilesystemIOManager,
    ) -> None:
        """Test that empty run_id raises ValueError."""
        with pytest.raises(ValueError, match="run_id must not be empty"):
            ExecutionContext(
                run_id="",
                dag_id="dag_123",
                execution_plan=sample_execution_plan,
                io_manager=sample_io_manager,
            )

    def test_empty_dag_id_raises_error(
        self,
        sample_execution_plan: ExecutionPlan,
        sample_io_manager: FilesystemIOManager,
    ) -> None:
        """Test that empty dag_id raises ValueError."""
        with pytest.raises(ValueError, match="dag_id must not be empty"):
            ExecutionContext(
                run_id="run_001",
                dag_id="",
                execution_plan=sample_execution_plan,
                io_manager=sample_io_manager,
            )

    def test_get_root_tasks(self, sample_execution_context: ExecutionContext) -> None:
        """Test getting root tasks from execution plan."""
        root_tasks = sample_execution_context.get_root_tasks()

        assert root_tasks == ["A"]

    def test_get_downstream_tasks(self, sample_execution_context: ExecutionContext) -> None:
        """Test getting downstream tasks."""
        downstream_a = sample_execution_context.get_downstream_tasks("A")
        downstream_b = sample_execution_context.get_downstream_tasks("B")
        downstream_c = sample_execution_context.get_downstream_tasks("C")

        assert downstream_a == ["B"]
        assert downstream_b == ["C"]
        assert downstream_c == []

    def test_get_upstream_tasks(self, sample_execution_context: ExecutionContext) -> None:
        """Test getting upstream tasks."""
        upstream_a = sample_execution_context.get_upstream_tasks("A")
        upstream_b = sample_execution_context.get_upstream_tasks("B")
        upstream_c = sample_execution_context.get_upstream_tasks("C")

        assert upstream_a == []
        assert upstream_b == ["A"]
        assert upstream_c == ["B"]

    def test_is_task_ready_root_task(
        self,
        sample_execution_context: ExecutionContext,
    ) -> None:
        """Test that root tasks are always ready."""
        completed_tasks: set[str] = set()
        assert sample_execution_context.is_task_ready("A", completed_tasks)

    def test_is_task_ready_with_dependencies(
        self,
        sample_execution_context: ExecutionContext,
    ) -> None:
        """Test task readiness with dependencies."""
        # B depends on A, so not ready when A is not completed
        completed_tasks: set[str] = set()
        assert not sample_execution_context.is_task_ready("B", completed_tasks)

        # B is ready when A is completed
        completed_tasks.add("A")
        assert sample_execution_context.is_task_ready("B", completed_tasks)

        # C depends on B, so not ready when B is not completed
        assert not sample_execution_context.is_task_ready("C", completed_tasks)

        # C is ready when B is also completed
        completed_tasks.add("B")
        assert sample_execution_context.is_task_ready("C", completed_tasks)

    def test_get_ready_tasks_initial(
        self,
        sample_execution_context: ExecutionContext,
    ) -> None:
        """Test getting ready tasks at the start."""
        completed_tasks: set[str] = set()
        ready_tasks = sample_execution_context.get_ready_tasks(completed_tasks)

        assert ready_tasks == ["A"]

    def test_get_ready_tasks_after_completion(
        self,
        sample_execution_context: ExecutionContext,
    ) -> None:
        """Test getting ready tasks after completing some tasks."""
        # After completing A, B should be ready
        completed_tasks = {"A"}
        ready_tasks = sample_execution_context.get_ready_tasks(completed_tasks)
        assert ready_tasks == ["B"]

        # After completing A and B, C should be ready
        completed_tasks = {"A", "B"}
        ready_tasks = sample_execution_context.get_ready_tasks(completed_tasks)
        assert ready_tasks == ["C"]

        # After completing all tasks, no tasks should be ready
        completed_tasks = {"A", "B", "C"}
        ready_tasks = sample_execution_context.get_ready_tasks(completed_tasks)
        assert ready_tasks == []

    def test_get_ready_tasks_with_parallel_execution(self, tmp_path: Path) -> None:
        """Test getting ready tasks when multiple tasks can run in parallel."""
        # Create a DAG with parallel branches: A -> B and A -> C
        registry = TaskRegistry()

        def task_a() -> int:
            return 1

        def task_b(a: int) -> int:
            return a * 2

        def task_c(a: int) -> int:
            return a * 3

        registry.register(TaskMetadata(name="A", func=task_a, dependencies=[]))
        registry.register(TaskMetadata(name="B", func=task_b, dependencies=["A"]))
        registry.register(TaskMetadata(name="C", func=task_c, dependencies=["A"]))

        planner = Planner(registry)
        execution_plan = planner.create_execution_plan()

        ctx = ExecutionContext(
            run_id="run_001",
            dag_id="dag_123",
            execution_plan=execution_plan,
            io_manager=FilesystemIOManager(base_path=tmp_path),
        )

        # After completing A, both B and C should be ready
        completed_tasks = {"A"}
        ready_tasks = ctx.get_ready_tasks(completed_tasks)
        assert set(ready_tasks) == {"B", "C"}
