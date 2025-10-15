"""Orchestrator for coordinating DAG execution."""

from __future__ import annotations

import asyncio
import queue
import time
from typing import TYPE_CHECKING, Any

from uuid6 import uuid7

from app.executor.domain.executable_task import ExecutableTask
from app.executor.domain.execution_context import ExecutionContext
from app.executor.domain.execution_result import ExecutionResult
from app.executor.domain.task_result import TaskResult
from app.executor.domain.task_status import TaskStatus

if TYPE_CHECKING:
    from multiprocessing.queues import Queue as MPQueue


class Orchestrator:
    """Coordinates DAG execution in the main process.

    Responsibilities:
    - Submit root tasks to task queue
    - Poll result queue for completed tasks
    - Track execution state (completed, failed, streaming)
    - Identify ready downstream tasks and submit them
    - Handle retries and fail-safe tasks
    - Return final execution result
    """

    def __init__(
        self,
        context: ExecutionContext,
        task_queue: MPQueue[Any],
        result_queue: MPQueue[TaskResult],
    ) -> None:
        """Initialize orchestrator.

        Args:
            context: Execution context with DAG and metadata
            task_queue: Queue to submit tasks to workers
            result_queue: Queue to receive results from workers
        """
        self.context = context
        self.task_queue = task_queue
        self.result_queue = result_queue

        # Track execution state
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.in_progress_tasks: set[str] = set()
        self.streaming_tasks: dict[str, list[TaskResult]] = {}

        # Collect all results
        self.all_task_results: dict[str, list[TaskResult]] = {}

        # Track task result IDs for streaming
        self.task_result_ids: dict[str, str] = {}

    def _create_executable_task(
        self,
        task_name: str,
        task_result_id: str | None = None,
        retry_count: int = 0,
    ) -> ExecutableTask:
        """Create an executable task from task name.

        Args:
            task_name: Name of the task to create
            task_result_id: Result ID (generated if not provided)
            retry_count: Retry attempt number

        Returns:
            ExecutableTask ready to be submitted

        Raises:
            ValueError: If task not found in DAG
        """
        # Get task metadata from DAG
        node = self.context.execution_plan.dag.get_node(task_name)
        if node is None:
            raise ValueError(f"Task '{task_name}' not found in DAG")

        # Generate result ID if not provided
        if task_result_id is None:
            task_result_id = str(uuid7())

        # Build inputs dict from completed upstream tasks
        inputs: dict[str, tuple[str, str]] = {}
        upstream_tasks = self.context.get_upstream_tasks(task_name)

        for upstream_task in upstream_tasks:
            # Check if upstream task has output that should be passed
            # Use the stored result ID for the upstream task
            if upstream_task in self.task_result_ids:
                upstream_result_id = self.task_result_ids[upstream_task]

                # Check if this is a data flow dependency by inspecting schema
                # If the task's input schema expects this parameter, add it
                if node.task.input_schema:
                    # Named argument mapping: param name = upstream task name
                    if isinstance(node.task.input_schema, dict):
                        if upstream_task in node.task.input_schema:
                            inputs[upstream_task] = (upstream_task, upstream_result_id)
                    # For single type schema, use all upstream outputs
                    else:
                        inputs[upstream_task] = (upstream_task, upstream_result_id)

        return ExecutableTask(
            run_id=self.context.run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            inputs=inputs,
            retry_count=retry_count,
            max_retries=node.task.max_retries,
        )

    def _submit_task(self, task: ExecutableTask) -> None:
        """Submit a task to the task queue.

        Args:
            task: Executable task to submit
        """
        self.in_progress_tasks.add(task.task_name)
        self.task_queue.put(task)

    def _get_ready_tasks(self) -> list[str]:
        """Get tasks that are ready to execute.

        A task is ready if:
        - All upstream dependencies are completed
        - Not already completed or in progress
        - Not failed (unless this is a retry)

        Returns:
            List of ready task names
        """
        ready = []
        all_tasks = set(self.context.execution_plan.execution_order)

        for task_name in all_tasks:
            # Skip if already completed, in progress, or failed
            if (
                task_name in self.completed_tasks
                or task_name in self.in_progress_tasks
                or task_name in self.failed_tasks
            ):
                continue

            # Check if all upstream dependencies are completed
            if self.context.is_task_ready(task_name, self.completed_tasks):
                ready.append(task_name)

        return ready

    def _handle_task_result(self, result: TaskResult) -> bool:
        """Handle a task result from a worker.

        Args:
            result: Task result to process

        Returns:
            True if execution should continue, False if should stop
        """
        task_name = result.task_name

        # Store result
        if task_name not in self.all_task_results:
            self.all_task_results[task_name] = []
        self.all_task_results[task_name].append(result)

        # Handle streaming results
        if result.is_streaming:
            if task_name not in self.streaming_tasks:
                self.streaming_tasks[task_name] = []
            self.streaming_tasks[task_name].append(result)

            # Store the first result ID for this streaming task
            if task_name not in self.task_result_ids:
                self.task_result_ids[task_name] = result.task_result_id

            # If not complete, wait for more results
            if not result.stream_complete:
                return True

            # Stream is complete, proceed to mark task as done

        # Store result ID for non-streaming or completed streaming tasks
        if task_name not in self.task_result_ids:
            self.task_result_ids[task_name] = result.task_result_id

        # Remove from in-progress
        self.in_progress_tasks.discard(task_name)

        # Handle based on status
        if result.status == TaskStatus.SUCCESS:
            # Mark as completed
            self.completed_tasks.add(task_name)

            # Submit ready downstream tasks
            ready_tasks = self._get_ready_tasks()
            for ready_task_name in ready_tasks:
                task = self._create_executable_task(ready_task_name)
                self._submit_task(task)

            return True

        elif result.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT):
            # Get original task for retry logic
            node = self.context.execution_plan.dag.get_node(task_name)
            if node is None:
                return False

            # Check if should retry
            if result.retry_count < node.task.max_retries:
                # Retry the task
                retry_task = self._create_executable_task(
                    task_name,
                    result.task_result_id,  # Keep same result ID
                    result.retry_count + 1,
                )
                self._submit_task(retry_task)
                return True

            # No more retries, check fail_safe
            if node.task.fail_safe:
                # Treat as completed despite failure
                self.completed_tasks.add(task_name)
                self.failed_tasks.add(task_name)

                # Continue with downstream tasks
                ready_tasks = self._get_ready_tasks()
                for ready_task_name in ready_tasks:
                    task = self._create_executable_task(ready_task_name)
                    self._submit_task(task)

                return True

            # Fatal failure - stop execution
            self.failed_tasks.add(task_name)
            return False

        else:
            # CANCELLED or other status - stop execution
            self.failed_tasks.add(task_name)
            return False

    async def orchestrate(self) -> ExecutionResult:
        """Run the orchestration loop.

        Returns:
            ExecutionResult with final status and results
        """
        start_time = time.time()

        # Submit root tasks
        root_tasks = self.context.get_root_tasks()
        for task_name in root_tasks:
            task = self._create_executable_task(task_name)
            self._submit_task(task)

        # Poll result queue until done
        loop = asyncio.get_event_loop()
        while True:
            try:
                # Non-blocking check for results
                try:
                    result = await loop.run_in_executor(
                        None, lambda: self.result_queue.get(block=False)
                    )
                except queue.Empty:
                    # No results available, check if we're done
                    if not self.in_progress_tasks:
                        # No tasks in progress, we're done
                        break

                    # Wait a bit before checking again
                    await asyncio.sleep(0.1)
                    continue

                # Handle the result
                should_continue = self._handle_task_result(result)
                if not should_continue:
                    # Fatal error occurred, stop execution
                    break

                # Check if we're done (all tasks completed or failed)
                all_tasks = set(self.context.execution_plan.execution_order)
                processed_tasks = self.completed_tasks | self.failed_tasks
                if processed_tasks == all_tasks:
                    # All tasks processed
                    break

            except Exception as error:
                # Unexpected error in orchestration loop
                print(f"Orchestrator error: {error}")
                break

        # Calculate final status
        total_execution_time = time.time() - start_time

        # Check if any failed task is not fail-safe
        has_non_failsafe_failure = False
        if self.failed_tasks:
            for task_name in self.failed_tasks:
                node = self.context.execution_plan.dag.get_node(task_name)
                if node is not None and not node.task.fail_safe:
                    has_non_failsafe_failure = True
                    break

        final_status = TaskStatus.FAILED if has_non_failsafe_failure else TaskStatus.SUCCESS

        # Build execution result
        return ExecutionResult(
            run_id=self.context.run_id,
            dag_id=self.context.dag_id,
            status=final_status,
            completed_tasks=list(self.completed_tasks),
            failed_tasks=list(self.failed_tasks),
            total_execution_time=total_execution_time,
            task_results=self.all_task_results,
            metadata=self.context.metadata or {},
        )
