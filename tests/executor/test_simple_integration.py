"""Simple integration tests for the executor."""

from collections.abc import Generator
from pathlib import Path

import pytest

from app.executor.domain.execution_context import ExecutionContext
from app.executor.infrastructure.multiprocess_executor import MultiprocessExecutor
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.planner.domain.dag_builder import DAGBuilder
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


class TestSimpleIntegration:
    """Simple integration tests for the executor."""

    def test_execute_single_task(self, io_manager: FilesystemIOManager) -> None:
        """Test executing a single task."""

        # Register task using decorator to add to global registry
        @task(name="simple_task", dependencies=[])
        def simple_task() -> int:
            return 42

        # Create DAG and execution plan
        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        dag = dag_builder.build_dag(task_names=["simple_task"])

        from app.planner.domain.execution_plan import ExecutionPlan

        execution_plan = ExecutionPlan(dag=dag)

        # Create execution context
        context = ExecutionContext(
            run_id="test_run_single",
            dag_id=dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        # Execute (will use global registry by default)
        executor = MultiprocessExecutor(num_workers=1, io_manager=io_manager)
        result = executor.run(context)

        # Verify
        assert result.is_successful
        assert len(result.completed_tasks) == 1
        assert "simple_task" in result.completed_tasks

    def test_execute_two_tasks_sequential(self, io_manager: FilesystemIOManager) -> None:
        """Test executing two tasks sequentially."""

        @task(name="task_a", dependencies=[])
        def task_a() -> int:
            return 10

        @task(name="task_b", dependencies=["task_a"])
        def task_b(task_a: int) -> int:
            return task_a * 2

        # Create DAG and execution plan
        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        dag = dag_builder.build_dag(task_names=["task_a", "task_b"])

        from app.planner.domain.execution_plan import ExecutionPlan

        execution_plan = ExecutionPlan(dag=dag)

        # Create execution context
        context = ExecutionContext(
            run_id="test_run_seq",
            dag_id=dag.dag_id,
            execution_plan=execution_plan,
            io_manager=io_manager,
        )

        # Execute
        executor = MultiprocessExecutor(num_workers=1, io_manager=io_manager)
        result = executor.run(context)

        # Verify
        assert result.is_successful
        assert len(result.completed_tasks) == 2
        assert "task_a" in result.completed_tasks
        assert "task_b" in result.completed_tasks

        # Verify final result
        task_b_results = result.get_task_results("task_b")
        assert len(task_b_results) == 1
        final_value = io_manager.load("task_b", "test_run_seq", task_b_results[0].task_result_id)
        assert final_value == 20
