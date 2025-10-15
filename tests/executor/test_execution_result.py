"""Tests for ExecutionResult model."""

import pytest

from app.executor.domain.execution_result import ExecutionResult
from app.executor.domain.task_result import TaskResult
from app.executor.domain.task_status import TaskStatus


class TestExecutionResult:
    """Test suite for ExecutionResult model."""

    def test_create_execution_result(self) -> None:
        """Test creating a valid execution result."""
        result = ExecutionResult(
            run_id="run_123",
            dag_id="dag_456",
            status=TaskStatus.SUCCESS,
            completed_tasks=["task_a", "task_b"],
            failed_tasks=[],
            total_execution_time=10.5,
        )

        assert result.run_id == "run_123"
        assert result.dag_id == "dag_456"
        assert result.status == TaskStatus.SUCCESS
        assert result.completed_tasks == ["task_a", "task_b"]
        assert result.failed_tasks == []
        assert result.total_execution_time == 10.5
        assert result.is_successful
        assert result.total_tasks == 2

    def test_empty_run_id_raises_error(self) -> None:
        """Test that empty run_id raises ValueError."""
        with pytest.raises(ValueError, match="run_id must not be empty"):
            ExecutionResult(
                run_id="",
                dag_id="dag_456",
                status=TaskStatus.SUCCESS,
            )

    def test_empty_dag_id_raises_error(self) -> None:
        """Test that empty dag_id raises ValueError."""
        with pytest.raises(ValueError, match="dag_id must not be empty"):
            ExecutionResult(
                run_id="run_123",
                dag_id="",
                status=TaskStatus.SUCCESS,
            )

    def test_negative_execution_time_raises_error(self) -> None:
        """Test that negative execution time raises ValueError."""
        with pytest.raises(ValueError, match="total_execution_time must be non-negative"):
            ExecutionResult(
                run_id="run_123",
                dag_id="dag_456",
                status=TaskStatus.SUCCESS,
                total_execution_time=-1.0,
            )

    def test_failed_execution(self) -> None:
        """Test execution result with failures."""
        result = ExecutionResult(
            run_id="run_123",
            dag_id="dag_456",
            status=TaskStatus.FAILED,
            completed_tasks=["task_a"],
            failed_tasks=["task_b"],
            total_execution_time=5.0,
        )

        assert not result.is_successful
        assert result.status == TaskStatus.FAILED
        assert result.total_tasks == 2

    def test_add_task_result(self) -> None:
        """Test adding task results."""
        result = ExecutionResult(
            run_id="run_123",
            dag_id="dag_456",
            status=TaskStatus.SUCCESS,
        )

        task_result = TaskResult.success(
            run_id="run_123",
            task_name="task_a",
            task_result_id="result_1",
            execution_time=2.5,
        )

        result.add_task_result("task_a", task_result)
        assert "task_a" in result.task_results
        assert len(result.task_results["task_a"]) == 1

        # Add another result for same task
        task_result2 = TaskResult.success(
            run_id="run_123",
            task_name="task_a",
            task_result_id="result_2",
            execution_time=3.0,
        )
        result.add_task_result("task_a", task_result2)
        assert len(result.task_results["task_a"]) == 2

    def test_get_task_results(self) -> None:
        """Test retrieving task results."""
        result = ExecutionResult(
            run_id="run_123",
            dag_id="dag_456",
            status=TaskStatus.SUCCESS,
        )

        # Get non-existent task
        results = result.get_task_results("task_a")
        assert results == []

        # Add and retrieve
        task_result = TaskResult.success(
            run_id="run_123",
            task_name="task_a",
            task_result_id="result_1",
            execution_time=2.5,
        )
        result.add_task_result("task_a", task_result)

        results = result.get_task_results("task_a")
        assert len(results) == 1
        assert results[0] == task_result

    def test_to_dict(self) -> None:
        """Test converting execution result to dictionary."""
        result = ExecutionResult(
            run_id="run_123",
            dag_id="dag_456",
            status=TaskStatus.SUCCESS,
            completed_tasks=["task_a"],
            failed_tasks=[],
            total_execution_time=5.0,
            metadata={"user": "admin"},
        )

        task_result = TaskResult.success(
            run_id="run_123",
            task_name="task_a",
            task_result_id="result_1",
            execution_time=2.5,
        )
        result.add_task_result("task_a", task_result)

        result_dict = result.to_dict()

        assert result_dict["run_id"] == "run_123"
        assert result_dict["dag_id"] == "dag_456"
        assert result_dict["status"] == "SUCCESS"
        assert result_dict["completed_tasks"] == ["task_a"]
        assert result_dict["failed_tasks"] == []
        assert result_dict["total_execution_time"] == 5.0
        assert result_dict["metadata"] == {"user": "admin"}
        assert "task_a" in result_dict["task_results"]
        assert len(result_dict["task_results"]["task_a"]) == 1
