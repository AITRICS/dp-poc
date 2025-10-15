"""Tests for task executor helpers."""

import asyncio
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest

from app.executor.domain.executable_task import ExecutableTask
from app.executor.domain.task_status import TaskStatus
from app.executor.infrastructure.task_executor import (
    execute_task,
    execute_with_timeout_async,
    execute_with_timeout_sync,
    handle_streaming_results,
    load_task_inputs,
)
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.task_registry.domain.task_model import TaskMetadata


@pytest.fixture
def io_manager(tmp_path: Path) -> FilesystemIOManager:
    """Create a test I/O manager."""
    return FilesystemIOManager(base_path=tmp_path)


@pytest.fixture
def sample_metadata() -> TaskMetadata:
    """Create sample task metadata."""

    def sample_func(a: int, b: int) -> int:
        return a + b

    return TaskMetadata(
        name="test_task",
        func=sample_func,
        is_async=False,
        timeout=None,
        stream_output=False,
    )


@pytest.fixture
def sample_task() -> ExecutableTask:
    """Create a sample executable task."""
    return ExecutableTask(
        run_id="run_123",
        task_name="test_task",
        task_result_id="result_123",
        max_retries=0,
        inputs={"a": ("upstream_a", "result_a"), "b": ("upstream_b", "result_b")},
    )


class TestLoadTaskInputs:
    """Tests for load_task_inputs function."""

    def test_load_inputs_with_dependencies(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test loading inputs from I/O manager."""
        # Save upstream task results
        io_manager.save("upstream_a", "run_123", "result_a", 10)
        io_manager.save("upstream_b", "run_123", "result_b", 20)

        # Load inputs
        inputs = load_task_inputs(sample_task, io_manager)

        assert inputs == {"a": 10, "b": 20}

    def test_load_inputs_no_dependencies(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test loading inputs when no dependencies."""
        sample_task.inputs = {}

        inputs = load_task_inputs(sample_task, io_manager)

        assert inputs == {}

    def test_load_inputs_missing_raises_error(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test that loading missing input raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_task_inputs(sample_task, io_manager)


class TestExecuteWithTimeout:
    """Tests for timeout execution functions."""

    def test_sync_function_no_timeout(self) -> None:
        """Test executing sync function without timeout."""

        def add(a: int, b: int) -> int:
            return a + b

        result = execute_with_timeout_sync(add, {"a": 2, "b": 3}, None)
        assert result == 5

    def test_sync_function_with_timeout_succeeds(self) -> None:
        """Test executing sync function with timeout that succeeds."""
        import time

        def quick_func() -> str:
            time.sleep(0.1)
            return "done"

        result = execute_with_timeout_sync(quick_func, {}, timeout=2)
        assert result == "done"

    def test_sync_function_with_timeout_fails(self) -> None:
        """Test executing sync function that exceeds timeout."""
        import time

        def slow_func() -> str:
            time.sleep(2)
            return "done"

        with pytest.raises(asyncio.TimeoutError):
            execute_with_timeout_sync(slow_func, {}, timeout=1)

    async def test_async_function_no_timeout(self) -> None:
        """Test executing async function without timeout."""

        async def add_async(a: int, b: int) -> int:
            return a + b

        result = await execute_with_timeout_async(add_async, {"a": 2, "b": 3}, None)
        assert result == 5

    async def test_async_function_with_timeout_succeeds(self) -> None:
        """Test executing async function with timeout that succeeds."""

        async def quick_func() -> str:
            await asyncio.sleep(0.1)
            return "done"

        result = await execute_with_timeout_async(quick_func, {}, timeout=2)
        assert result == "done"

    async def test_async_function_with_timeout_fails(self) -> None:
        """Test executing async function that exceeds timeout."""

        async def slow_func() -> str:
            await asyncio.sleep(2)
            return "done"

        with pytest.raises(asyncio.TimeoutError):
            await execute_with_timeout_async(slow_func, {}, timeout=1)


class TestHandleStreamingResults:
    """Tests for streaming result handling."""

    def test_sync_generator(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test handling sync generator."""
        import time

        def gen() -> Generator[int, None, None]:
            yield 1
            yield 2
            yield 3

        start_time = time.time()
        results = handle_streaming_results(sample_task, gen(), io_manager, start_time)

        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.status == TaskStatus.SUCCESS
            assert result.is_streaming
            assert result.stream_index == i
            if i == 2:
                assert result.stream_complete
            else:
                assert not result.stream_complete

        # Check that values were saved
        assert io_manager.exists(
            sample_task.task_name, sample_task.run_id, results[0].task_result_id
        )
        assert (
            io_manager.load(sample_task.task_name, sample_task.run_id, results[0].task_result_id)
            == 1
        )

    def test_async_generator(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test handling async generator."""
        import time

        async def gen() -> AsyncGenerator[int, None]:
            yield 1
            yield 2
            yield 3

        start_time = time.time()
        results = handle_streaming_results(sample_task, gen(), io_manager, start_time)

        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.status == TaskStatus.SUCCESS
            assert result.is_streaming
            assert result.stream_index == i
            if i == 2:
                assert result.stream_complete
            else:
                assert not result.stream_complete

    def test_generator_with_error(
        self, sample_task: ExecutableTask, io_manager: FilesystemIOManager
    ) -> None:
        """Test handling generator that raises error."""
        import time

        def gen() -> Generator[int, None, None]:
            yield 1
            raise ValueError("Test error")

        start_time = time.time()
        results = handle_streaming_results(sample_task, gen(), io_manager, start_time)

        # Should have one success and one failure
        assert len(results) == 2
        assert results[0].status == TaskStatus.SUCCESS
        assert results[1].status == TaskStatus.FAILED
        assert results[1].error_message is not None
        assert "Test error" in results[1].error_message


class TestExecuteTask:
    """Tests for execute_task function."""

    def test_execute_simple_task(self, io_manager: FilesystemIOManager) -> None:
        """Test executing a simple non-streaming task."""

        def add(a: int, b: int) -> int:
            return a + b

        metadata = TaskMetadata(
            name="add_task",
            func=add,
            is_async=False,
            timeout=None,
            stream_output=False,
        )

        # Save upstream inputs
        io_manager.save("upstream_a", "run_123", "result_a", 10)
        io_manager.save("upstream_b", "run_123", "result_b", 20)

        task = ExecutableTask(
            run_id="run_123",
            task_name="add_task",
            task_result_id="result_123",
            max_retries=metadata.max_retries,
            inputs={"a": ("upstream_a", "result_a"), "b": ("upstream_b", "result_b")},
        )

        results = execute_task(task, metadata, io_manager)

        assert len(results) == 1
        assert results[0].status == TaskStatus.SUCCESS
        assert results[0].task_name == "add_task"
        assert not results[0].is_streaming

        # Check result was saved
        saved_value = io_manager.load("add_task", "run_123", "result_123")
        assert saved_value == 30

    def test_execute_async_task(self, io_manager: FilesystemIOManager) -> None:
        """Test executing an async task."""

        async def add_async(a: int, b: int) -> int:
            await asyncio.sleep(0.1)
            return a + b

        metadata = TaskMetadata(
            name="add_async_task",
            func=add_async,
            is_async=True,
            timeout=None,
            stream_output=False,
        )

        # Save upstream inputs
        io_manager.save("upstream_a", "run_123", "result_a", 10)
        io_manager.save("upstream_b", "run_123", "result_b", 20)

        task = ExecutableTask(
            run_id="run_123",
            task_name="add_async_task",
            task_result_id="result_123",
            max_retries=metadata.max_retries,
            inputs={"a": ("upstream_a", "result_a"), "b": ("upstream_b", "result_b")},
        )

        results = execute_task(task, metadata, io_manager)

        assert len(results) == 1
        assert results[0].status == TaskStatus.SUCCESS
        saved_value = io_manager.load("add_async_task", "run_123", "result_123")
        assert saved_value == 30

    def test_execute_streaming_task(self, io_manager: FilesystemIOManager) -> None:
        """Test executing a streaming task."""

        def gen() -> Generator[int, None, None]:
            for i in range(3):
                yield i * 2

        metadata = TaskMetadata(
            name="stream_task",
            func=gen,
            is_async=False,
            timeout=None,
            stream_output=True,
        )

        task = ExecutableTask(
            run_id="run_123",
            task_name="stream_task",
            task_result_id="result_123",
            max_retries=metadata.max_retries,
            inputs={},
        )

        results = execute_task(task, metadata, io_manager)

        assert len(results) == 3
        for i, result in enumerate(results):
            assert result.is_streaming
            assert result.stream_index == i

        assert results[-1].stream_complete

    def test_execute_task_with_timeout(self, io_manager: FilesystemIOManager) -> None:
        """Test executing task that times out."""
        import time

        def slow_func() -> str:
            time.sleep(2)
            return "done"

        metadata = TaskMetadata(
            name="slow_task",
            func=slow_func,
            is_async=False,
            timeout=1,  # 1 second timeout
            stream_output=False,
        )

        task = ExecutableTask(
            run_id="run_123",
            task_name="slow_task",
            task_result_id="result_123",
            max_retries=metadata.max_retries,
            inputs={},
        )

        results = execute_task(task, metadata, io_manager)

        assert len(results) == 1
        assert results[0].status == TaskStatus.TIMEOUT
        assert results[0].error_message is not None
        assert "timeout" in results[0].error_message.lower()

    def test_execute_task_with_error(self, io_manager: FilesystemIOManager) -> None:
        """Test executing task that raises error."""

        def error_func() -> None:
            raise ValueError("Test error")

        metadata = TaskMetadata(
            name="error_task",
            func=error_func,
            is_async=False,
            timeout=None,
            stream_output=False,
        )

        task = ExecutableTask(
            run_id="run_123",
            task_name="error_task",
            task_result_id="result_123",
            max_retries=metadata.max_retries,
            inputs={},
        )

        results = execute_task(task, metadata, io_manager)

        assert len(results) == 1
        assert results[0].status == TaskStatus.FAILED
        assert results[0].error_message is not None
        assert "Test error" in results[0].error_message
        assert results[0].error_type == "ValueError"
