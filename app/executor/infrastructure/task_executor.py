"""Task execution logic for worker processes."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncGenerator, Generator
from typing import Any

from uuid6 import uuid7

from app.executor.domain.executable_task import ExecutableTask
from app.executor.domain.task_result import TaskResult
from app.io_manager.domain.io_manager_port import IOManagerPort


def load_task_inputs(
    task: ExecutableTask,
    io_manager: IOManagerPort,
) -> dict[str, Any]:
    """Load task inputs from I/O Manager.

    Args:
        task: Executable task with input specifications
        io_manager: I/O manager to load data from

    Returns:
        Dictionary of parameter names to their loaded values

    Raises:
        FileNotFoundError: If input data not found
        IOError: If loading fails
    """
    inputs: dict[str, Any] = {}

    for param_name, (upstream_task, result_id) in task.inputs.items():
        # Load from I/O Manager using the upstream task's result
        value = io_manager.load(
            task_name=upstream_task,
            run_id=task.run_id,
            task_result_id=result_id,
        )
        inputs[param_name] = value

    return inputs


async def execute_with_timeout_async(
    func: Any,
    kwargs: dict[str, Any],
    timeout: int | None,
) -> Any:
    """Execute an async function with optional timeout.

    Args:
        func: Async function to execute
        kwargs: Keyword arguments for the function
        timeout: Timeout in seconds (None for no timeout)

    Returns:
        Function result

    Raises:
        asyncio.TimeoutError: If execution exceeds timeout
        Exception: Any exception raised by the function
    """
    if timeout is None:
        return await func(**kwargs)

    return await asyncio.wait_for(func(**kwargs), timeout=timeout)


def execute_with_timeout_sync(
    func: Any,
    kwargs: dict[str, Any],
    timeout: int | None,
) -> Any:
    """Execute a sync function with optional timeout.

    Note: For sync functions, timeout is enforced by running in an executor
    with asyncio.wait_for. This is not a hard timeout and may not interrupt
    blocking operations.

    Args:
        func: Sync function to execute
        kwargs: Keyword arguments for the function
        timeout: Timeout in seconds (None for no timeout)

    Returns:
        Function result

    Raises:
        asyncio.TimeoutError: If execution exceeds timeout
        Exception: Any exception raised by the function
    """

    async def _run_sync() -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(**kwargs))

    if timeout is None:
        # Run without timeout
        return asyncio.run(_run_sync())

    # Run with timeout
    return asyncio.run(asyncio.wait_for(_run_sync(), timeout=timeout))


def handle_streaming_results(
    task: ExecutableTask,
    generator: Generator[Any, None, None] | AsyncGenerator[Any, None],
    io_manager: IOManagerPort,
    start_time: float,
) -> list[TaskResult]:
    """Process streaming results from a generator.

    Each yielded value is saved to I/O Manager with a unique task_result_id
    and a TaskResult is created for it.

    Args:
        task: Executable task
        generator: Generator or AsyncGenerator yielding values
        io_manager: I/O manager to save results
        start_time: Execution start time for timing

    Returns:
        List of TaskResult objects (one per yielded value)
    """
    results: list[TaskResult] = []
    stream_index = 0

    try:
        # Handle both sync and async generators
        if inspect.isasyncgen(generator):
            # Async generator
            async def _process_async() -> list[TaskResult]:
                nonlocal stream_index
                results_inner: list[TaskResult] = []
                async for value in generator:
                    # Save each value with unique result ID
                    result_id = str(uuid7())
                    io_manager.save(
                        task_name=task.task_name,
                        run_id=task.run_id,
                        task_result_id=result_id,
                        data=value,
                    )

                    # Create success result for this stream item
                    execution_time = time.time() - start_time
                    results_inner.append(
                        TaskResult.success(
                            run_id=task.run_id,
                            task_name=task.task_name,
                            task_result_id=result_id,
                            execution_time=execution_time,
                            retry_count=task.retry_count,
                            is_streaming=True,
                            stream_index=stream_index,
                            stream_complete=False,  # Not complete yet
                        )
                    )
                    stream_index += 1

                # Mark the last result as complete
                if results_inner:
                    last_result = results_inner[-1]
                    results_inner[-1] = TaskResult.success(
                        run_id=last_result.run_id,
                        task_name=last_result.task_name,
                        task_result_id=last_result.task_result_id,
                        execution_time=last_result.execution_time,
                        retry_count=last_result.retry_count,
                        is_streaming=True,
                        stream_index=last_result.stream_index,
                        stream_complete=True,  # Mark as complete
                    )

                return results_inner

            # Check if we're already in an event loop
            try:
                _ = asyncio.get_running_loop()
                # We're in an event loop, need to create a task
                # This shouldn't happen in worker process, but handle it safely
                raise RuntimeError("Cannot handle async generator in running event loop")
            except RuntimeError:
                # No running loop, safe to use asyncio.run()
                results = asyncio.run(_process_async())

        else:
            # Sync generator
            for value in generator:  # type: ignore
                # Save each value with unique result ID
                result_id = str(uuid7())
                io_manager.save(
                    task_name=task.task_name,
                    run_id=task.run_id,
                    task_result_id=result_id,
                    data=value,
                )

                # Create success result for this stream item
                execution_time = time.time() - start_time
                results.append(
                    TaskResult.success(
                        run_id=task.run_id,
                        task_name=task.task_name,
                        task_result_id=result_id,
                        execution_time=execution_time,
                        retry_count=task.retry_count,
                        is_streaming=True,
                        stream_index=stream_index,
                        stream_complete=False,  # Not complete yet
                    )
                )
                stream_index += 1

            # Mark the last result as complete
            if results:
                last_result = results[-1]
                results[-1] = TaskResult.success(
                    run_id=last_result.run_id,
                    task_name=last_result.task_name,
                    task_result_id=last_result.task_result_id,
                    execution_time=last_result.execution_time,
                    retry_count=last_result.retry_count,
                    is_streaming=True,
                    stream_index=last_result.stream_index,
                    stream_complete=True,  # Mark as complete
                )

    except Exception as error:
        # If streaming fails, create failure result
        execution_time = time.time() - start_time
        results.append(
            TaskResult.failure(
                run_id=task.run_id,
                task_name=task.task_name,
                task_result_id=task.task_result_id,
                execution_time=execution_time,
                error=error,
                retry_count=task.retry_count,
            )
        )

    return results


def execute_task(
    task: ExecutableTask,
    task_metadata: Any,  # TaskMetadata from registry
    io_manager: IOManagerPort,
) -> list[TaskResult]:
    """Execute a task and return result(s).

    This is the main entry point for task execution in worker processes.
    Handles loading inputs, executing the function (sync/async), handling
    streaming/non-streaming outputs, timeouts, and errors.

    Args:
        task: Executable task to run
        task_metadata: Task metadata from registry (function, config, etc.)
        io_manager: I/O manager for loading inputs and saving outputs

    Returns:
        List of TaskResult objects (multiple for streaming, single for normal)
    """
    start_time = time.time()

    try:
        # Load inputs from I/O Manager
        inputs = load_task_inputs(task, io_manager)

        # Get function and metadata
        func = task_metadata.func
        timeout = task_metadata.timeout
        is_async = task_metadata.is_async
        is_streaming = task_metadata.stream_output

        # Execute function with timeout
        if is_async:
            result = execute_with_timeout_async(func, inputs, timeout)
            if inspect.iscoroutine(result):
                result = asyncio.run(result)
        else:
            result = execute_with_timeout_sync(func, inputs, timeout)

        # Handle streaming vs non-streaming
        if is_streaming:
            # Result should be a generator
            if not (inspect.isgenerator(result) or inspect.isasyncgen(result)):
                raise TypeError(
                    f"Task '{task.task_name}' has stream_output=True but did not return a generator"
                )
            return handle_streaming_results(task, result, io_manager, start_time)

        else:
            # Save single result
            io_manager.save(
                task_name=task.task_name,
                run_id=task.run_id,
                task_result_id=task.task_result_id,
                data=result,
            )

            execution_time = time.time() - start_time
            return [
                TaskResult.success(
                    run_id=task.run_id,
                    task_name=task.task_name,
                    task_result_id=task.task_result_id,
                    execution_time=execution_time,
                    retry_count=task.retry_count,
                )
            ]

    except TimeoutError:
        # Timeout occurred
        execution_time = time.time() - start_time
        return [
            TaskResult.timeout(
                run_id=task.run_id,
                task_name=task.task_name,
                task_result_id=task.task_result_id,
                execution_time=execution_time,
                retry_count=task.retry_count,
            )
        ]

    except Exception as error:
        # Any other error
        execution_time = time.time() - start_time
        return [
            TaskResult.failure(
                run_id=task.run_id,
                task_name=task.task_name,
                task_result_id=task.task_result_id,
                execution_time=execution_time,
                error=error,
                retry_count=task.retry_count,
            )
        ]
