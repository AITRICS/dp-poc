"""Simple integration tests for the event-driven executor."""

import asyncio
from collections.abc import Generator
from pathlib import Path

import pytest

from app.event_system.domain.events import (
    DAGExecutionEvent,
    EventBase,
    ExecutionResultEvent,
)
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.executor.domain.orchestrator import Orchestrator
from app.executor.infrastructure.multiprocess_pool_manager import MultiprocessPoolManager
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.planner.domain.dag_builder import DAGBuilder
from app.planner.domain.execution_plan import ExecutionPlan
from app.task_registry import get_registry, task
from tests.executor.fixtures import InMemoryStateRepository, TestHelper


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


@pytest.fixture
async def event_broker() -> InMemoryBroker[EventBase]:
    """Create an in-memory event broker."""
    broker: InMemoryBroker[EventBase] = InMemoryBroker[EventBase]()
    # Declare topics
    await broker.declare_topic("dag.execution")
    await broker.declare_topic("task.submit")
    await broker.declare_topic("task.result")
    await broker.declare_topic("dag.execution.result")
    return broker


class TestSimpleIntegration:
    """Simple integration tests for the event-driven executor."""

    @pytest.mark.asyncio
    async def test_execute_single_task(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
        """Test executing a single task."""

        # Register task using decorator to add to global registry
        @task(name="simple_task", dependencies=[])
        def simple_task() -> int:
            return 42

        # Create DAG and execution plan
        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        dag = dag_builder.build_dag(task_names=["simple_task"])
        execution_plan = ExecutionPlan(dag=dag)

        # Prepare execution plans dict
        execution_plans = {dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

        # Create worker pool and orchestrator
        pool_manager = MultiprocessPoolManager(
            num_workers=1, io_manager=io_manager, publisher=publisher
        )

        async with pool_manager:
            orchestrator = Orchestrator(
                execution_plans=execution_plans,
                worker_pool_manager=pool_manager,
                consumer=consumer,
                publisher=publisher,
                state_repository=state_repository,
                io_manager=io_manager,
            )

            # Start orchestrator in background
            orchestrator_task = asyncio.create_task(orchestrator.orchestrate())

            # Wait a bit for orchestrator to start
            await asyncio.sleep(0.1)

            # Setup result listener
            result_consumer = InMemoryConsumer(event_broker)
            result_event: ExecutionResultEvent | None = None
            completion_event = asyncio.Event()

            async def listen_for_result() -> None:
                nonlocal result_event
                async for event in result_consumer.consume("dag.execution.result"):
                    if isinstance(event, ExecutionResultEvent):
                        result_event = event
                        completion_event.set()
                        break

            listener_task = asyncio.create_task(listen_for_result())

            # Publish DAG execution event
            dag_event = DAGExecutionEvent(topic="dag.execution", dag_id=dag.dag_id)
            await publisher.publish("dag.execution", dag_event)

            # Wait for completion
            await TestHelper.run_until_complete(orchestrator_task, completion_event, timeout=5.0)
            listener_task.cancel()

            # Verify results
            assert result_event is not None
            assert result_event.get_status() == "SUCCESS"
            assert "simple_task" in result_event.get_completed_tasks()
            assert len(result_event.get_failed_tasks()) == 0

    @pytest.mark.asyncio
    async def test_execute_two_tasks_sequential(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
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
        execution_plan = ExecutionPlan(dag=dag)

        # Prepare execution plans dict
        execution_plans = {dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

        # Create worker pool and orchestrator
        pool_manager = MultiprocessPoolManager(
            num_workers=1, io_manager=io_manager, publisher=publisher
        )

        async with pool_manager:
            orchestrator = Orchestrator(
                execution_plans=execution_plans,
                worker_pool_manager=pool_manager,
                consumer=consumer,
                publisher=publisher,
                state_repository=state_repository,
                io_manager=io_manager,
            )

            # Start orchestrator in background
            orchestrator_task = asyncio.create_task(orchestrator.orchestrate())
            await asyncio.sleep(0.1)

            # Setup result listener
            result_consumer = InMemoryConsumer(event_broker)
            result_event: ExecutionResultEvent | None = None
            completion_event = asyncio.Event()

            async def listen_for_result() -> None:
                nonlocal result_event
                async for event in result_consumer.consume("dag.execution.result"):
                    if isinstance(event, ExecutionResultEvent):
                        result_event = event
                        completion_event.set()
                        break

            listener_task = asyncio.create_task(listen_for_result())

            # Publish DAG execution event
            dag_event = DAGExecutionEvent(topic="dag.execution", dag_id=dag.dag_id)
            await publisher.publish("dag.execution", dag_event)

            # Wait for completion
            await TestHelper.run_until_complete(orchestrator_task, completion_event, timeout=5.0)
            listener_task.cancel()

            # Verify results
            assert result_event is not None
            assert result_event.get_status() == "SUCCESS"
            assert "task_a" in result_event.get_completed_tasks()
            assert "task_b" in result_event.get_completed_tasks()
            assert len(result_event.get_completed_tasks()) == 2
            assert len(result_event.get_failed_tasks()) == 0

    @pytest.mark.asyncio
    async def test_execute_with_failure(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
        """Test executing DAG with task failure."""

        @task(name="success_task", dependencies=[])
        def success_task() -> int:
            return 42

        @task(name="failing_task", dependencies=["success_task"])
        def failing_task(success_task: int) -> None:
            raise ValueError("This task fails")

        # Create DAG and execution plan
        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        dag = dag_builder.build_dag(task_names=["success_task", "failing_task"])
        execution_plan = ExecutionPlan(dag=dag)

        # Prepare execution plans dict
        execution_plans = {dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

        # Create worker pool and orchestrator
        pool_manager = MultiprocessPoolManager(
            num_workers=1, io_manager=io_manager, publisher=publisher
        )

        async with pool_manager:
            orchestrator = Orchestrator(
                execution_plans=execution_plans,
                worker_pool_manager=pool_manager,
                consumer=consumer,
                publisher=publisher,
                state_repository=state_repository,
                io_manager=io_manager,
            )

            # Start orchestrator in background
            orchestrator_task = asyncio.create_task(orchestrator.orchestrate())
            await asyncio.sleep(0.1)

            # Setup result listener
            result_consumer = InMemoryConsumer(event_broker)
            result_event: ExecutionResultEvent | None = None
            completion_event = asyncio.Event()

            async def listen_for_result() -> None:
                nonlocal result_event
                async for event in result_consumer.consume("dag.execution.result"):
                    if isinstance(event, ExecutionResultEvent):
                        result_event = event
                        completion_event.set()
                        break

            listener_task = asyncio.create_task(listen_for_result())

            # Publish DAG execution event
            dag_event = DAGExecutionEvent(topic="dag.execution", dag_id=dag.dag_id)
            await publisher.publish("dag.execution", dag_event)

            # Wait for completion
            await TestHelper.run_until_complete(orchestrator_task, completion_event, timeout=5.0)
            listener_task.cancel()

            # Verify results
            assert result_event is not None
            assert result_event.get_status() == "FAILED"
            assert "success_task" in result_event.get_completed_tasks()
            assert "failing_task" in result_event.get_failed_tasks()
