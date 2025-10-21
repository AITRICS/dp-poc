"""Integration tests for Orchestrator and MultiprocessPoolManager."""

import asyncio
from collections.abc import Generator
from pathlib import Path

import pytest

from app.event_system.domain.events import DAGExecutionEvent, EventBase, ExecutionResultEvent
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.executor.domain.orchestrator import Orchestrator
from app.executor.infrastructure.multiprocess_pool_manager import MultiprocessPoolManager
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager
from app.planner import get_planner
from app.planner.domain.dag_builder import DAGBuilder
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

    @pytest.mark.asyncio
    async def test_execute_simple_dag(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
        """Test executing a simple DAG."""

        @task(name="add_one", dependencies=[])
        def add_one() -> int:
            return 1 + 1

        @task(name="multiply_by_two", dependencies=["add_one"])
        def multiply_by_two(add_one: int) -> int:
            return add_one * 2

        # Create execution plan
        from app.planner.domain.execution_plan import ExecutionPlan

        registry = get_registry()
        dag_builder = DAGBuilder(registry)

        dag = dag_builder.build_dag(task_names=["add_one", "multiply_by_two"])
        execution_plan = ExecutionPlan(dag=dag)

        # Prepare execution plans dict
        execution_plans = {dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

        # Execute
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
        assert len(result_event.get_completed_tasks()) == 2
        assert len(result_event.get_failed_tasks()) == 0
        assert "add_one" in result_event.get_completed_tasks()
        assert "multiply_by_two" in result_event.get_completed_tasks()

    @pytest.mark.asyncio
    async def test_execute_parallel_tasks(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
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
        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        planner = get_planner()
        planner.dag_builder = dag_builder

        execution_plan = planner.create_execution_plan(task_names=["task_a", "task_b", "task_c"])

        # Prepare execution plans dict
        execution_plans = {execution_plan.dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

        # Execute with multiple workers
        pool_manager = MultiprocessPoolManager(
            num_workers=2, io_manager=io_manager, publisher=publisher
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
            dag_event = DAGExecutionEvent(topic="dag.execution", dag_id=execution_plan.dag.dag_id)
            await publisher.publish("dag.execution", dag_event)

            # Wait for completion
            await TestHelper.run_until_complete(orchestrator_task, completion_event, timeout=5.0)
            listener_task.cancel()

        assert result_event is not None
        assert result_event.get_status() == "SUCCESS"
        assert len(result_event.get_completed_tasks()) == 3

    @pytest.mark.asyncio
    async def test_execute_with_fail_safe(
        self, io_manager: FilesystemIOManager, event_broker: InMemoryBroker[EventBase]
    ) -> None:
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

        registry = get_registry()
        dag_builder = DAGBuilder(registry)
        planner = get_planner()
        planner.dag_builder = dag_builder

        execution_plan = planner.create_execution_plan(
            task_names=["task_1", "task_2_failsafe", "task_3"]
        )

        # Prepare execution plans dict
        execution_plans = {execution_plan.dag.dag_id: execution_plan}

        # Create event infrastructure
        consumer = InMemoryConsumer(event_broker)
        publisher = InMemoryPublisher(event_broker)
        state_repository = InMemoryStateRepository()

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
            dag_event = DAGExecutionEvent(topic="dag.execution", dag_id=execution_plan.dag.dag_id)
            await publisher.publish("dag.execution", dag_event)

            # Wait for completion
            await TestHelper.run_until_complete(orchestrator_task, completion_event, timeout=5.0)
            listener_task.cancel()

        # Should succeed despite task_2 failure because it's failsafe
        assert result_event is not None
        assert result_event.get_status() == "SUCCESS"
        assert "task_1" in result_event.get_completed_tasks()
        assert (
            "task_2_failsafe" in result_event.get_completed_tasks()
        )  # Failsafe task is marked as completed
        assert "task_2_failsafe" in result_event.get_failed_tasks()  # But also in failed_tasks
        assert "task_3" in result_event.get_completed_tasks()
