"""Tests for ExecutableTask model."""

import pytest

from app.executor.domain.executable_task import ExecutableTask


class TestExecutableTask:
    """Test cases for ExecutableTask model."""

    def test_create_executable_task(self) -> None:
        """Test creating a basic executable task."""
        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
        )

        assert task.run_id == "run_001"
        assert task.task_name == "sample_task"
        assert task.task_result_id == "result_001"
        assert task.max_retries == 3
        assert task.inputs == {}
        assert task.retry_count == 0

    def test_create_task_with_inputs(self) -> None:
        """Test creating a task with input dependencies."""
        inputs = {
            "x": ("upstream_task", "result_001"),
            "y": ("another_task", "result_002"),
        }

        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            inputs=inputs,
        )

        assert task.inputs == inputs

    def test_create_task_with_retry_count(self) -> None:
        """Test creating a task with retry count."""
        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            retry_count=2,
        )

        assert task.retry_count == 2

    def test_empty_run_id_raises_error(self) -> None:
        """Test that empty run_id raises ValueError."""
        with pytest.raises(ValueError, match="run_id must not be empty"):
            ExecutableTask(
                run_id="",
                task_name="sample_task",
                task_result_id="result_001",
                max_retries=3,
            )

    def test_empty_task_name_raises_error(self) -> None:
        """Test that empty task_name raises ValueError."""
        with pytest.raises(ValueError, match="task_name must not be empty"):
            ExecutableTask(
                run_id="run_001",
                task_name="",
                task_result_id="result_001",
                max_retries=3,
            )

    def test_empty_task_result_id_raises_error(self) -> None:
        """Test that empty task_result_id raises ValueError."""
        with pytest.raises(ValueError, match="task_result_id must not be empty"):
            ExecutableTask(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="",
                max_retries=3,
            )

    def test_negative_retry_count_raises_error(self) -> None:
        """Test that negative retry_count raises ValueError."""
        with pytest.raises(ValueError, match="retry_count must be non-negative"):
            ExecutableTask(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                max_retries=3,
                retry_count=-1,
            )

    def test_to_dict(self) -> None:
        """Test converting task to dictionary."""
        inputs = {"x": ("upstream_task", "result_001")}
        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            inputs=inputs,
            retry_count=1,
        )

        result = task.to_dict()

        assert result["run_id"] == "run_001"
        assert result["task_name"] == "sample_task"
        assert result["task_result_id"] == "result_001"
        assert result["inputs"] == inputs
        assert result["retry_count"] == 1
        assert result["max_retries"] == 3

    def test_should_retry_on_failure_true(self) -> None:
        """Test should_retry_on_failure returns True when retries available."""
        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            retry_count=1,
        )

        # max_retries is 3, retry_count is 1, so should retry
        assert task.should_retry_on_failure()

    def test_should_retry_on_failure_false(self) -> None:
        """Test should_retry_on_failure returns False when no retries left."""
        task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            retry_count=3,
        )

        # max_retries is 3, retry_count is 3, so should not retry
        assert not task.should_retry_on_failure()

    def test_create_retry_task(self) -> None:
        """Test creating a retry task."""
        inputs = {"x": ("upstream_task", "result_001")}
        original_task = ExecutableTask(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            max_retries=3,
            inputs=inputs,
            retry_count=1,
        )

        retry_task = original_task.create_retry_task()

        assert retry_task.run_id == original_task.run_id
        assert retry_task.task_name == original_task.task_name
        assert retry_task.task_result_id == original_task.task_result_id
        assert retry_task.max_retries == original_task.max_retries
        assert retry_task.inputs == original_task.inputs
        assert retry_task.retry_count == original_task.retry_count + 1

        # Ensure inputs are copied, not shared
        retry_task.inputs["y"] = ("new_task", "result_002")
        assert "y" not in original_task.inputs
