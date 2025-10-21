"""Event-driven orchestrator for coordinating DAG execution."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from uuid6 import uuid7

from app.event_system.domain.events import (
    DAGExecutionEvent,
    EventBase,
    ExecutionResultEvent,
    TaskResultEvent,
    TaskSubmitEvent,
)
from app.executor.domain.executable_task import ExecutableTask
from app.executor.domain.execution_state import ExecutionState
from app.executor.domain.task_result import TaskResult
from app.executor.domain.task_status import TaskStatus

if TYPE_CHECKING:
    from app.event_system.domain.consumer_port import ConsumerPort
    from app.event_system.domain.publisher_port import PublisherPort
    from app.executor.domain.execution_state_repository_port import (
        ExecutionStateRepositoryPort,
    )
    from app.executor.domain.worker_pool_manager_port import WorkerPoolManagerPort
    from app.io_manager.domain.io_manager_port import IOManagerPort
    from app.planner.domain.execution_plan import ExecutionPlan

logger = logging.getLogger(__name__)


class Orchestrator:
    """Event-driven orchestrator for coordinating DAG execution.

    This orchestrator holds ExecutionPlan(s) as a map and processes events
    to coordinate task execution. All execution state is managed through
    state_repository, not in memory.

    Architecture:
    - Orchestrator: Holds ExecutionPlans (the "map"), processes events
    - state_repository: Single source of truth for all execution state
    - Events: Drive all state changes

    Responsibilities:
    - Process DAGExecutionEvent to start new DAG executions
    - Process TaskResultEvent from workers to track task completion
    - Process TaskSubmitEvent to submit tasks to worker pool
    - Handle retries and fail-safe tasks
    - Publish ExecutionResultEvent when DAG execution completes
    - Persist all state changes to repository

    Event Flow:
    1. DAGExecutionEvent → Determine root tasks → Publish TaskSubmitEvent(s)
    2. TaskSubmitEvent → Submit task to worker pool
    3. TaskResultEvent → Determine next tasks → Publish TaskSubmitEvent(s)
    """

    def __init__(
        self,
        execution_plans: dict[str, ExecutionPlan],
        worker_pool_manager: WorkerPoolManagerPort,
        consumer: ConsumerPort[EventBase],
        publisher: PublisherPort[EventBase],
        state_repository: ExecutionStateRepositoryPort,
        io_manager: IOManagerPort,
    ) -> None:
        """Initialize event-driven orchestrator.

        Args:
            execution_plans: Map of dag_id to ExecutionPlan
            worker_pool_manager: Worker pool manager for task execution
            consumer: Event consumer for receiving events
            publisher: Event publisher for sending events
            state_repository: State repository for persisting execution state (required)
            io_manager: I/O manager for storing and loading task results
        """
        self.execution_plans = execution_plans
        self.worker_pool_manager = worker_pool_manager
        self.consumer = consumer
        self.publisher = publisher
        self.state_repository = state_repository
        self.io_manager = io_manager

    def _get_root_tasks(self, execution_plan: ExecutionPlan) -> list[str]:
        """Get root tasks from execution plan.

        Args:
            execution_plan: Execution plan to get root tasks from

        Returns:
            List of root task names
        """
        if not execution_plan.parallel_levels:
            return []
        return execution_plan.parallel_levels[0]

    def _get_ready_tasks(
        self, execution_plan: ExecutionPlan, state: ExecutionState
    ) -> list[str]:
        """Get tasks that are ready to execute.

        A task is ready if:
        - All upstream dependencies are completed
        - Not already completed or in progress
        - Not failed

        Args:
            execution_plan: Execution plan containing DAG structure
            state: Current execution state

        Returns:
            List of ready task names
        """
        ready = []
        all_tasks = set(execution_plan.execution_order)

        for task_name in all_tasks:
            # Skip if already completed, in progress, or failed
            if (
                task_name in state.completed_tasks
                or task_name in state.in_progress_tasks
                or task_name in state.failed_tasks
            ):
                continue

            # Check if all upstream dependencies are completed
            node = execution_plan.dag.get_node(task_name)
            if node is None:
                continue

            upstream_tasks = list(node.upstream)
            if all(upstream in state.completed_tasks for upstream in upstream_tasks):
                ready.append(task_name)

        return ready

    def _is_execution_complete(
        self, execution_plan: ExecutionPlan, state: ExecutionState
    ) -> bool:
        """Check if execution is complete.

        Args:
            execution_plan: Execution plan containing DAG structure
            state: Current execution state

        Returns:
            True if all tasks have been processed
        """
        all_tasks = set(execution_plan.execution_order)
        processed_tasks = state.completed_tasks | state.failed_tasks
        return processed_tasks == all_tasks

    def _create_executable_task(
        self,
        run_id: str,
        dag_id: str,
        task_name: str,
        state: ExecutionState,
        task_result_id: str | None = None,
        retry_count: int = 0,
    ) -> ExecutableTask:
        """Create an executable task from task name.

        Args:
            run_id: Run ID for the execution
            dag_id: DAG ID for the execution
            task_name: Name of the task to create
            state: Current execution state
            task_result_id: Result ID (generated if not provided)
            retry_count: Retry attempt number

        Returns:
            ExecutableTask ready to be submitted

        Raises:
            ValueError: If dag_id not found or task not found in DAG
        """
        # Get execution plan
        execution_plan = self.execution_plans.get(dag_id)
        if execution_plan is None:
            raise ValueError(f"ExecutionPlan not found for dag_id '{dag_id}'")

        # Get task metadata from DAG
        node = execution_plan.dag.get_node(task_name)
        if node is None:
            raise ValueError(f"Task '{task_name}' not found in DAG")

        # Generate result ID if not provided
        if task_result_id is None:
            task_result_id = str(uuid7())

        # Build inputs dict from completed upstream tasks
        inputs: dict[str, tuple[str, str]] = {}
        upstream_tasks = list(node.upstream)

        for upstream_task in upstream_tasks:
            # Check if upstream task has output that should be passed
            # Use the stored result ID for the upstream task
            if upstream_task in state.task_result_ids:
                upstream_result_id = state.task_result_ids[upstream_task]

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
            run_id=run_id,
            task_name=task_name,
            task_result_id=task_result_id,
            inputs=inputs,
            retry_count=retry_count,
            max_retries=node.task.max_retries,
        )

    async def _publish_task_submit_event(self, task: ExecutableTask) -> None:
        """Publish a TaskSubmitEvent for a task.

        Args:
            task: Executable task to submit
        """
        event = TaskSubmitEvent(
            topic="task.submit",
            run_id=task.run_id,
            task_name=task.task_name,
            task_result_id=task.task_result_id,
            inputs=task.inputs,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
        )

        await self.publisher.publish("task.submit", event)

    def _event_to_task_result(self, event: TaskResultEvent) -> TaskResult:
        """Convert TaskResultEvent to TaskResult.

        Args:
            event: Task result event

        Returns:
            TaskResult object
        """
        status_str = event.get_status()
        status = TaskStatus[status_str]

        return TaskResult(
            run_id=event.get_run_id(),
            task_name=event.get_task_name(),
            task_result_id=event.get_task_result_id(),
            status=status,
            execution_time=event.get_execution_time(),
            retry_count=event.get_retry_count(),
            error_message=event.get_error_message(),
            error_type=event.get_error_type(),
            is_streaming=event.get_is_streaming(),
            stream_index=event.get_stream_index(),
            stream_complete=event.get_stream_complete(),
            metadata=event.get_metadata() or {},
        )

    async def _handle_task_result(
        self,
        execution_plan: ExecutionPlan,
        state: ExecutionState,
        result: TaskResult,
    ) -> bool:
        """Handle a task result from a worker.

        Args:
            execution_plan: Execution plan containing DAG structure
            state: Current execution state
            result: Task result to process

        Returns:
            True if execution should continue, False if should stop
        """
        task_name = result.task_name

        # Handle streaming results
        if result.is_streaming:
            # Store the first result ID for this streaming task
            if task_name not in state.task_result_ids:
                state.set_task_result_id(task_name, result.task_result_id)

            # If not complete, wait for more results
            if not result.stream_complete:
                return True

            # Stream is complete, proceed to mark task as done

        # Store result ID for non-streaming or completed streaming tasks
        if task_name not in state.task_result_ids:
            state.set_task_result_id(task_name, result.task_result_id)

        # Handle based on status
        if result.status == TaskStatus.SUCCESS:
            # Mark as completed
            state.mark_task_completed(task_name)

            # Publish TaskSubmitEvent for ready downstream tasks
            ready_tasks = self._get_ready_tasks(execution_plan, state)
            for ready_task_name in ready_tasks:
                task = self._create_executable_task(
                    state.run_id, state.dag_id, ready_task_name, state
                )
                await self._publish_task_submit_event(task)

            return True

        elif result.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT):
            # Get original task for retry logic
            node = execution_plan.dag.get_node(task_name)
            if node is None:
                return False

            # Check if should retry
            if result.retry_count < node.task.max_retries:
                # Retry the task
                retry_task = self._create_executable_task(
                    state.run_id,
                    state.dag_id,
                    task_name,
                    state,
                    result.task_result_id,  # Keep same result ID
                    result.retry_count + 1,
                )
                await self._publish_task_submit_event(retry_task)
                return True

            # No more retries, check fail_safe
            if node.task.fail_safe:
                # Treat as completed despite failure
                state.mark_task_completed(task_name)
                state.failed_tasks.add(task_name)

                # Continue with downstream tasks
                ready_tasks = self._get_ready_tasks(execution_plan, state)
                for ready_task_name in ready_tasks:
                    task = self._create_executable_task(
                        state.run_id, state.dag_id, ready_task_name, state
                    )
                    await self._publish_task_submit_event(task)

                return True

            # Fatal failure - stop execution
            state.mark_task_failed(task_name)
            return False

        else:
            # CANCELLED or other status - stop execution
            state.mark_task_failed(task_name)
            return False

    async def _handle_dag_execution_event(self, event: DAGExecutionEvent) -> None:
        """Handle DAG execution start event.

        Args:
            event: DAG execution event
        """
        try:
            dag_id = event.get_dag_id()
            if dag_id is None:
                logger.error("DAGExecutionEvent missing dag_id")
                return

            # Get execution plan (already held by orchestrator)
            execution_plan = self.execution_plans.get(dag_id)
            if execution_plan is None:
                logger.error("ExecutionPlan not found for dag_id: %s", dag_id)
                return

            # Generate run_id
            run_id = str(uuid7())

            # Create execution state
            state = ExecutionState(
                run_id=run_id,
                dag_id=dag_id,
                completed_tasks=set(),
                failed_tasks=set(),
                in_progress_tasks=set(),
                task_result_ids={},
                start_time=time.time(),
            )

            # Save state to repository
            await self.state_repository.save_state(run_id, state)

            # Publish TaskSubmitEvent for root tasks
            root_tasks = self._get_root_tasks(execution_plan)
            for task_name in root_tasks:
                task = self._create_executable_task(run_id, dag_id, task_name, state)
                await self._publish_task_submit_event(task)

            logger.info("Started DAG execution: run_id=%s, dag_id=%s", run_id, dag_id)

        except Exception as error:
            logger.exception("Error handling DAG execution event: %s", error)

    async def _handle_task_submit_event(self, event: TaskSubmitEvent) -> None:
        """Handle task submit event.

        This method receives a TaskSubmitEvent and submits the task to the worker pool.

        Args:
            event: Task submit event
        """
        try:
            run_id = event.get_run_id()

            # Get state from repository
            state = await self.state_repository.get_state(run_id)
            if state is None:
                logger.error("ExecutionState not found for run_id: %s", run_id)
                return

            # Create ExecutableTask from event
            task = ExecutableTask(
                run_id=event.get_run_id(),
                task_name=event.get_task_name(),
                task_result_id=event.get_task_result_id(),
                inputs=event.get_inputs(),
                retry_count=event.get_retry_count(),
                max_retries=event.get_max_retries(),
            )

            # Mark task as in progress
            state.mark_task_in_progress(task.task_name)

            # Submit task to worker pool
            self.worker_pool_manager.submit_task(task)

            # Save state after submitting
            await self.state_repository.save_state(run_id, state)

            logger.debug(
                "Submitted task: run_id=%s, task_name=%s, retry=%d",
                run_id,
                task.task_name,
                task.retry_count,
            )

        except Exception as error:
            logger.exception("Error handling task submit event: %s", error)

    async def _handle_task_result_event(self, event: TaskResultEvent) -> None:
        """Handle task result event from worker.

        Args:
            event: Task result event
        """
        try:
            run_id = event.get_run_id()

            # Get state from repository
            state = await self.state_repository.get_state(run_id)
            if state is None:
                logger.error("ExecutionState not found for run_id: %s", run_id)
                return

            # Get execution plan
            execution_plan = self.execution_plans.get(state.dag_id)
            if execution_plan is None:
                logger.error("ExecutionPlan not found for dag_id: %s", state.dag_id)
                return

            # Convert event to result
            result = self._event_to_task_result(event)

            # Handle the result
            should_continue = await self._handle_task_result(execution_plan, state, result)

            # Save state after handling event
            await self.state_repository.save_state(run_id, state)

            # Check if execution is complete
            if self._is_execution_complete(execution_plan, state) or not should_continue:
                # Execution finished, publish result event
                await self._publish_execution_result(execution_plan, state)

                # Delete from repository
                await self.state_repository.delete_state(run_id)

        except Exception as error:
            logger.exception("Error handling task result event: %s", error)

    async def _publish_execution_result(
        self, execution_plan: ExecutionPlan, state: ExecutionState
    ) -> None:
        """Publish execution result event.

        Args:
            execution_plan: Execution plan containing DAG structure
            state: Final execution state
        """
        # Calculate final status
        has_non_failsafe_failure = False
        if state.failed_tasks:
            for task_name in state.failed_tasks:
                node = execution_plan.dag.get_node(task_name)
                if node is not None and not node.task.fail_safe:
                    has_non_failsafe_failure = True
                    break

        final_status = TaskStatus.FAILED if has_non_failsafe_failure else TaskStatus.SUCCESS

        # Create execution result event
        event = ExecutionResultEvent(
            topic="dag.execution.result",
            run_id=state.run_id,
            dag_id=state.dag_id,
            status=final_status.name,
            completed_tasks=list(state.completed_tasks),
            failed_tasks=list(state.failed_tasks),
            total_execution_time=state.get_execution_time(),
            metadata={},
        )

        # Publish the event
        await self.publisher.publish("dag.execution.result", event)

        logger.info(
            "Completed DAG execution: run_id=%s, status=%s, completed=%d, failed=%d",
            state.run_id,
            final_status.name,
            len(state.completed_tasks),
            len(state.failed_tasks),
        )

    async def orchestrate(
        self,
        topic_pattern: str = "dag.execution.**|task.submit.**|task.result.**",
    ) -> None:
        """Run the orchestration loop.

        This method runs indefinitely, consuming events from the queue and
        processing them. It handles three types of events:
        1. DAGExecutionEvent: Start new DAG execution
        2. TaskSubmitEvent: Submit task to worker pool
        3. TaskResultEvent: Track task completion and trigger downstream tasks

        State is saved to the repository after each event is processed,
        ensuring data consistency and enabling crash recovery.

        Args:
            topic_pattern: Topic pattern to consume events from
        """
        logger.info("Starting orchestrator event loop, listening to: %s", topic_pattern)

        try:
            async for event in self.consumer.consume_pattern(topic_pattern):
                # Handle different event types using pattern matching
                match event:
                    case DAGExecutionEvent():
                        await self._handle_dag_execution_event(event)
                    case TaskSubmitEvent():
                        await self._handle_task_submit_event(event)
                    case TaskResultEvent():
                        await self._handle_task_result_event(event)
                    case _:
                        logger.warning("Unknown event type: %s", type(event).__name__)

        except asyncio.CancelledError:
            logger.info("Orchestrator event loop cancelled")
            raise
        except Exception as error:
            logger.exception("Orchestrator error: %s", error)
            raise
        finally:
            logger.info("Orchestrator event loop stopped")
