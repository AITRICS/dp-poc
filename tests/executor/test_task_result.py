"""Tests for TaskResult model."""

import pytest

from app.executor.domain.task_result import TaskResult
from app.executor.domain.task_status import TaskStatus


class TestTaskResult:
    """Test cases for TaskResult model."""

    def test_create_basic_result(self) -> None:
        """Test creating a basic task result."""
        result = TaskResult(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            status=TaskStatus.SUCCESS,
            execution_time=1.5,
        )

        assert result.run_id == "run_001"
        assert result.task_name == "sample_task"
        assert result.task_result_id == "result_001"
        assert result.status == TaskStatus.SUCCESS
        assert result.execution_time == 1.5
        assert result.retry_count == 0
        assert result.error_message is None
        assert result.error_type is None
        assert not result.is_streaming

    def test_create_failed_result(self) -> None:
        """Test creating a failed task result."""
        result = TaskResult(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            status=TaskStatus.FAILED,
            execution_time=0.5,
            error_message="Something went wrong",
            error_type="ValueError",
        )

        assert result.status == TaskStatus.FAILED
        assert result.error_message == "Something went wrong"
        assert result.error_type == "ValueError"

    def test_create_streaming_result(self) -> None:
        """Test creating a streaming task result."""
        result = TaskResult(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            status=TaskStatus.SUCCESS,
            execution_time=1.0,
            is_streaming=True,
            stream_index=0,
            stream_complete=False,
        )

        assert result.is_streaming
        assert result.stream_index == 0
        assert result.stream_complete is False

    def test_empty_run_id_raises_error(self) -> None:
        """Test that empty run_id raises ValueError."""
        with pytest.raises(ValueError, match="run_id must not be empty"):
            TaskResult(
                run_id="",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
            )

    def test_empty_task_name_raises_error(self) -> None:
        """Test that empty task_name raises ValueError."""
        with pytest.raises(ValueError, match="task_name must not be empty"):
            TaskResult(
                run_id="run_001",
                task_name="",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
            )

    def test_empty_task_result_id_raises_error(self) -> None:
        """Test that empty task_result_id raises ValueError."""
        with pytest.raises(ValueError, match="task_result_id must not be empty"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="",
                status=TaskStatus.SUCCESS,
            )

    def test_negative_execution_time_raises_error(self) -> None:
        """Test that negative execution_time raises ValueError."""
        with pytest.raises(ValueError, match="execution_time must be non-negative"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                execution_time=-1.0,
            )

    def test_negative_retry_count_raises_error(self) -> None:
        """Test that negative retry_count raises ValueError."""
        with pytest.raises(ValueError, match="retry_count must be non-negative"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                retry_count=-1,
            )

    def test_streaming_without_index_raises_error(self) -> None:
        """Test that streaming result without stream_index raises ValueError."""
        with pytest.raises(ValueError, match="stream_index must be set"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                is_streaming=True,
                stream_complete=False,
            )

    def test_streaming_without_complete_flag_raises_error(self) -> None:
        """Test that streaming result without stream_complete raises ValueError."""
        with pytest.raises(ValueError, match="stream_complete must be set"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                is_streaming=True,
                stream_index=0,
            )

    def test_non_streaming_with_index_raises_error(self) -> None:
        """Test that non-streaming result with stream_index raises ValueError."""
        with pytest.raises(ValueError, match="stream_index should be None"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                is_streaming=False,
                stream_index=0,
            )

    def test_non_streaming_with_complete_flag_raises_error(self) -> None:
        """Test that non-streaming result with stream_complete raises ValueError."""
        with pytest.raises(ValueError, match="stream_complete should be None"):
            TaskResult(
                run_id="run_001",
                task_name="sample_task",
                task_result_id="result_001",
                status=TaskStatus.SUCCESS,
                is_streaming=False,
                stream_complete=False,
            )

    def test_to_dict(self) -> None:
        """Test converting result to dictionary."""
        result = TaskResult(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            status=TaskStatus.SUCCESS,
            execution_time=1.5,
            retry_count=1,
            metadata={"key": "value"},
        )

        result_dict = result.to_dict()

        assert result_dict["run_id"] == "run_001"
        assert result_dict["task_name"] == "sample_task"
        assert result_dict["task_result_id"] == "result_001"
        assert result_dict["status"] == "SUCCESS"
        assert result_dict["execution_time"] == 1.5
        assert result_dict["retry_count"] == 1
        assert result_dict["metadata"] == {"key": "value"}

    def test_success_factory(self) -> None:
        """Test TaskResult.success factory method."""
        result = TaskResult.success(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            execution_time=1.5,
            retry_count=1,
            metadata={"key": "value"},
        )

        assert result.status == TaskStatus.SUCCESS
        assert result.execution_time == 1.5
        assert result.retry_count == 1
        assert result.error_message is None
        assert result.error_type is None
        assert result.metadata == {"key": "value"}

    def test_success_factory_streaming(self) -> None:
        """Test TaskResult.success factory with streaming parameters."""
        result = TaskResult.success(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            execution_time=1.0,
            is_streaming=True,
            stream_index=5,
            stream_complete=True,
        )

        assert result.is_streaming
        assert result.stream_index == 5
        assert result.stream_complete is True

    def test_failure_factory(self) -> None:
        """Test TaskResult.failure factory method."""
        error = ValueError("Test error")

        result = TaskResult.failure(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            execution_time=0.5,
            error=error,
            retry_count=2,
        )

        assert result.status == TaskStatus.FAILED
        assert result.execution_time == 0.5
        assert result.retry_count == 2
        assert result.error_message == "Test error"
        assert result.error_type == "ValueError"

    def test_timeout_factory(self) -> None:
        """Test TaskResult.timeout factory method."""
        result = TaskResult.timeout(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            execution_time=60.0,
            retry_count=1,
        )

        assert result.status == TaskStatus.TIMEOUT
        assert result.execution_time == 60.0
        assert result.retry_count == 1
        assert result.error_message is not None
        assert "timeout" in result.error_message.lower()
        assert result.error_type == "TimeoutError"

    def test_cancelled_factory(self) -> None:
        """Test TaskResult.cancelled factory method."""
        result = TaskResult.cancelled(
            run_id="run_001",
            task_name="sample_task",
            task_result_id="result_001",
            execution_time=5.0,
        )

        assert result.status == TaskStatus.CANCELLED
        assert result.execution_time == 5.0
        assert result.error_message is not None
        assert "cancelled" in result.error_message.lower()
        assert result.error_type == "CancelledError"
