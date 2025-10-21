"""Integration tests for Orchestrator and MultiprocessPoolManager."""

import asyncio
from collections.abc import Generator
from pathlib import Path

import pytest

from app.executor.domain.execution_context import ExecutionContext
from app.executor.domain.orchestrator import Orchestrator
from app.executor.infrastructure.multiprocess_pool_manager import MultiprocessPoolManager
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.planner import get_planner
from app.task_registry import get_registry, task


@pytest.fixture
def io_manager(tmp_path: Path) -> FilesystemIOManager:
    """Create a test I/O manager."""
    return FilesystemIOManager(base_path=tmp_path)


@pytest.fixture(autouse=True)
def clean_global_registry() -> Generator[None, None, None]:
    """Clean the global registry before each test."""
    registry = get_registry()
    registry.clear()
    yield
    registry.clear()


class TestMultiprocessPoolManager:
    """Test suite for Orchestrator and MultiprocessPoolManager integration."""

    def test_pool_manager_initialization(self, io_manager: FilesystemIOManager) -> None:
        """Test pool manager initialization."""
        pool_manager = MultiprocessPoolManager(num_workers=2, io_manager=io_manager)

        assert pool_manager.num_workers == 2
        assert pool_manager.io_manager is io_manager
        assert len(pool_manager.workers) == 0  # Not started yet

    def test_pool_manager_invalid_workers_raises_error(self) -> None:
        """Test that invalid num_workers raises error."""
        with pytest.raises(ValueError, match="num_workers must be at least 1"):
            MultiprocessPoolManager(num_workers=0)

    def test_execute_simple_dag(self, io_manager: FilesystemIOManager) -> None:
        """Test executing a simple DAG."""

        @task(name="add_one", dependencies=[])
        def add_one() -> int:
            return 1 + 1

        @task(name="multiply_by_two", dependencies=["add_one"])
        def multiply_by_two(add_one: int) -> int:
            return add_one * 2

        # Create execution plan
        from app.planner.domain.dag_builder import DAGBuilder
        from app.planner.domain.execution_plan import ExecutionPlan

        registry = get_registry()
        dag_builder = DAGBuilder(registry)

        dag = dag_builder.build_dag(task_names=["add_one", "multiply_by_two"])
        execution_plan = ExecutionPlan(dag=dag)

        # Create execution context
        context = ExecutionContext(
            run_id="test_run_123",
            dag_id=dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        # Execute
        pool_manager = MultiprocessPoolManager(num_workers=1, io_manager=io_manager)
        with pool_manager:
            orchestrator = Orchestrator(context=context, worker_pool_manager=pool_manager)
            result = asyncio.run(orchestrator.orchestrate())

        # Verify results
        assert result.is_successful
        assert len(result.completed_tasks) == 2
        assert len(result.failed_tasks) == 0
        assert "add_one" in result.completed_tasks
        assert "multiply_by_two" in result.completed_tasks

    def test_execute_parallel_tasks(self, io_manager: FilesystemIOManager) -> None:
        """Test executing parallel independent tasks."""

        @task(name="task_a", dependencies=[])
        def task_a() -> int:
            return 10

        @task(name="task_b", dependencies=[])
        def task_b() -> int:
            return 20

        @task(name="task_c", dependencies=["task_a", "task_b"])
        def task_c(task_a: int, task_b: int) -> int:
            return task_a + task_b

        # Create execution plan
        from app.planner.domain.dag_builder import DAGBuilder

        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        planner = get_planner()
        planner.dag_builder = dag_builder

        execution_plan = planner.create_execution_plan(task_names=["task_a", "task_b", "task_c"])

        context = ExecutionContext(
            run_id="test_run_parallel",
            dag_id=execution_plan.dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        # Execute with multiple workers
        pool_manager = MultiprocessPoolManager(num_workers=2, io_manager=io_manager)
        with pool_manager:
            orchestrator = Orchestrator(context=context, worker_pool_manager=pool_manager)
            result = asyncio.run(orchestrator.orchestrate())

        assert result.is_successful
        assert len(result.completed_tasks) == 3

        # Verify final result
        task_c_results = result.get_task_results("task_c")
        assert len(task_c_results) == 1
        saved_value = io_manager.load(
            "task_c", "test_run_parallel", task_c_results[0].task_result_id
        )
        assert saved_value == 30

    def test_execute_with_failure(self, io_manager: FilesystemIOManager) -> None:
        """Test executing DAG with task failure."""

        @task(name="success_task", dependencies=[])
        def success_task() -> int:
            return 42

        @task(name="failing_task", dependencies=["success_task"])
        def failing_task(success_task: int) -> None:
            raise ValueError("This task fails")

        from app.planner.domain.dag_builder import DAGBuilder

        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        planner = get_planner()
        planner.dag_builder = dag_builder

        execution_plan = planner.create_execution_plan(task_names=["success_task", "failing_task"])

        context = ExecutionContext(
            run_id="test_run_failure",
            dag_id=execution_plan.dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        pool_manager = MultiprocessPoolManager(num_workers=1, io_manager=io_manager)
        with pool_manager:
            orchestrator = Orchestrator(context=context, worker_pool_manager=pool_manager)
            result = asyncio.run(orchestrator.orchestrate())

        # Execution should fail
        assert not result.is_successful
        assert "success_task" in result.completed_tasks
        assert "failing_task" in result.failed_tasks

    def test_execute_with_fail_safe(self, io_manager: FilesystemIOManager) -> None:
        """Test executing DAG with fail-safe task."""

        @task(name="task_1", dependencies=[])
        def task_1() -> int:
            return 10

        @task(name="task_2_failsafe", dependencies=["task_1"], fail_safe=True)
        def task_2_failsafe(task_1: int) -> None:
            raise ValueError("This task fails but is fail-safe")

        @task(name="task_3", dependencies=["task_2_failsafe"])
        def task_3() -> int:
            return 20

        from app.planner.domain.dag_builder import DAGBuilder

        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        planner = get_planner()
        planner.dag_builder = dag_builder

        execution_plan = planner.create_execution_plan(
            task_names=["task_1", "task_2_failsafe", "task_3"]
        )

        context = ExecutionContext(
            run_id="test_run_failsafe",
            dag_id=execution_plan.dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        pool_manager = MultiprocessPoolManager(num_workers=1, io_manager=io_manager)
        with pool_manager:
            orchestrator = Orchestrator(context=context, worker_pool_manager=pool_manager)
            result = asyncio.run(orchestrator.orchestrate())

        # Should succeed despite task_2 failure because it's failsafe
        assert result.is_successful
        assert "task_1" in result.completed_tasks
        assert "task_2_failsafe" in result.completed_tasks  # Failsafe task is marked as completed
        assert "task_2_failsafe" in result.failed_tasks  # But also in failed_tasks
        assert "task_3" in result.completed_tasks
