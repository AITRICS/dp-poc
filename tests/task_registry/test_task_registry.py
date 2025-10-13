"""Tests for Task Registry system."""

from collections.abc import Generator

import pytest

from app.task_registry import (
    ExcutableTask,
    TaskMetadata,
    TaskRegistry,
    clear_registry,
    get_registry,
    task,
    validate_all_tasks,
)


@pytest.fixture(autouse=True)
def clean_registry() -> Generator[None, None, None]:
    """Clean registry before each test."""
    clear_registry()
    yield
    clear_registry()


class TestTaskMetadata:
    """Test TaskMetadata model."""

    def test_create_task_metadata(self) -> None:
        """Test creating task metadata."""

        def my_func() -> None:
            pass

        metadata = TaskMetadata(
            name="test_task",
            func=my_func,
            tags=["tag1", "tag2"],
            dependencies=["dep1"],
        )

        assert metadata.name == "test_task"
        assert metadata.func == my_func
        assert metadata.tags == ["tag1", "tag2"]
        assert metadata.dependencies == ["dep1"]
        assert not metadata.is_async

    def test_task_metadata_validation(self) -> None:
        """Test task metadata validation."""

        def my_func() -> None:
            pass

        # Empty name should raise
        with pytest.raises(ValueError, match="name cannot be empty"):
            TaskMetadata(name="", func=my_func)

        # Non-callable should raise
        with pytest.raises(ValueError, match="must be callable"):
            TaskMetadata(name="test", func="not_a_function")  # type: ignore[arg-type]

        # Duplicate dependencies should raise
        with pytest.raises(ValueError, match="duplicate dependencies"):
            TaskMetadata(name="test", func=my_func, dependencies=["dep1", "dep1"])

    def test_task_metadata_to_dict(self) -> None:
        """Test converting task metadata to dictionary."""

        def my_func() -> None:
            pass

        metadata = TaskMetadata(
            name="test_task",
            func=my_func,
            tags=["tag1"],
            description="Test description",
        )

        data = metadata.to_dict()

        assert data["name"] == "test_task"
        assert data["tags"] == ["tag1"]
        assert data["description"] == "Test description"
        assert "func" not in data  # Function not serialized


class TestTaskRegistry:
    """Test TaskRegistry implementation."""

    def test_register_task(self) -> None:
        """Test registering a task."""
        registry = TaskRegistry()

        def my_func() -> None:
            pass

        task_meta = TaskMetadata(name="test_task", func=my_func)
        registry.register(task_meta)

        retrieved = registry.get("test_task")
        assert retrieved is not None
        assert retrieved.name == "test_task"

    def test_register_duplicate_task_fails(self) -> None:
        """Test that registering duplicate task fails."""
        registry = TaskRegistry()

        def my_func() -> None:
            pass

        task_meta = TaskMetadata(name="test_task", func=my_func)
        registry.register(task_meta)

        # Registering again should fail
        with pytest.raises(ValueError, match="already registered"):
            registry.register(task_meta)

    def test_get_all_tasks(self) -> None:
        """Test getting all tasks."""
        registry = TaskRegistry()

        def func1() -> None:
            pass

        def func2() -> None:
            pass

        registry.register(TaskMetadata(name="task1", func=func1))
        registry.register(TaskMetadata(name="task2", func=func2))

        all_tasks = registry.get_all()

        assert len(all_tasks) == 2
        assert "task1" in all_tasks
        assert "task2" in all_tasks

    def test_get_tasks_by_tag(self) -> None:
        """Test getting tasks by tag."""
        registry = TaskRegistry()

        def func1() -> None:
            pass

        def func2() -> None:
            pass

        def func3() -> None:
            pass

        registry.register(TaskMetadata(name="task1", func=func1, tags=["etl", "extract"]))
        registry.register(TaskMetadata(name="task2", func=func2, tags=["etl", "transform"]))
        registry.register(TaskMetadata(name="task3", func=func3, tags=["ml"]))

        etl_tasks = registry.get_tasks_by_tag("etl")
        assert len(etl_tasks) == 2
        assert all(t.name in ["task1", "task2"] for t in etl_tasks)

        ml_tasks = registry.get_tasks_by_tag("ml")
        assert len(ml_tasks) == 1
        assert ml_tasks[0].name == "task3"

    def test_validate_dependencies(self) -> None:
        """Test dependency validation."""
        registry = TaskRegistry()

        def func1() -> None:
            pass

        def func2() -> None:
            pass

        registry.register(TaskMetadata(name="task1", func=func1))
        registry.register(
            TaskMetadata(name="task2", func=func2, dependencies=["task1", "nonexistent"])
        )

        errors = registry.validate_dependencies()

        assert len(errors) == 1
        assert "nonexistent" in errors[0]
        assert "task2" in errors[0]

    def test_unregister_task(self) -> None:
        """Test unregistering a task."""
        registry = TaskRegistry()

        def my_func() -> None:
            pass

        registry.register(TaskMetadata(name="test_task", func=my_func))
        assert registry.get("test_task") is not None

        result = registry.unregister("test_task")
        assert result is True
        assert registry.get("test_task") is None

        # Unregistering again should return False
        result = registry.unregister("test_task")
        assert result is False

    def test_get_stats(self) -> None:
        """Test getting registry statistics."""
        registry = TaskRegistry()

        def sync_func() -> None:
            pass

        async def async_func() -> None:
            pass

        registry.register(TaskMetadata(name="sync1", func=sync_func))
        registry.register(TaskMetadata(name="sync2", func=sync_func, dependencies=["sync1"]))
        registry.register(TaskMetadata(name="async1", func=async_func, is_async=True))

        stats = registry.get_stats()

        assert stats["total_tasks"] == 3
        assert stats["sync_tasks"] == 2
        assert stats["async_tasks"] == 1
        assert stats["tasks_with_dependencies"] == 1


class TestTaskDecorator:
    """Test @task decorator."""

    def test_task_decorator_sync(self) -> None:
        """Test decorating sync function."""

        @task(name="my_task", tags=["test"])
        def my_function() -> str:
            return "Hello"

        registry = get_registry()
        task_meta = registry.get("my_task")

        assert task_meta is not None
        assert task_meta.name == "my_task"
        assert task_meta.tags == ["test"]
        assert not task_meta.is_async

        # Function should still work
        result = my_function()
        assert result == "Hello"

    def test_task_decorator_async(self) -> None:
        """Test decorating async function."""

        @task(name="my_async_task")
        async def my_async_function() -> str:
            return "Async Hello"

        registry = get_registry()
        task_meta = registry.get("my_async_task")

        assert task_meta is not None
        assert task_meta.is_async

    def test_task_decorator_with_dependencies(self) -> None:
        """Test task with dependencies."""

        @task(name="task_a")
        def task_a() -> str:
            return "A"

        @task(name="task_b", dependencies=["task_a"])
        def task_b() -> str:
            return "B"

        registry = get_registry()
        task_b_meta = registry.get("task_b")

        assert task_b_meta is not None
        assert task_b_meta.dependencies == ["task_a"]

        # Validation should pass
        errors = validate_all_tasks()
        assert len(errors) == 0

    def test_task_decorator_default_name(self) -> None:
        """Test task decorator uses function name as default."""

        @task()
        def my_function_name() -> None:
            pass

        registry = get_registry()
        task_meta = registry.get("my_function_name")

        assert task_meta is not None
        assert task_meta.name == "my_function_name"


@pytest.mark.asyncio
class TestExcutableTask:
    """Test ExcutableTask."""

    async def test_execute_sync_task(self) -> None:
        """Test executing sync task."""

        def add(a: int, b: int) -> int:
            return a + b

        metadata = TaskMetadata(name="add", func=add)
        executor = ExcutableTask(metadata)

        result = await executor.execute(5, 3)
        assert result == 8

    async def test_execute_async_task(self) -> None:
        """Test executing async task."""

        async def async_add(a: int, b: int) -> int:
            return a + b

        metadata = TaskMetadata(name="async_add", func=async_add, is_async=True)
        executor = ExcutableTask(metadata)

        result = await executor.execute(10, 20)
        assert result == 30

    async def test_executor_get_name(self) -> None:
        """Test getting task name from executor."""

        def my_func() -> None:
            pass

        metadata = TaskMetadata(name="test_task", func=my_func)
        executor = ExcutableTask(metadata)

        assert executor.get_name() == "test_task"

    async def test_executor_get_dependencies(self) -> None:
        """Test getting dependencies from executor."""

        def my_func() -> None:
            pass

        metadata = TaskMetadata(name="test_task", func=my_func, dependencies=["dep1", "dep2"])
        executor = ExcutableTask(metadata)

        deps = executor.get_dependencies()
        assert deps == ["dep1", "dep2"]


@pytest.mark.asyncio
class TestTaskIntegration:
    """Integration tests."""

    async def test_full_workflow(self) -> None:
        """Test full workflow: register, validate, execute."""

        @task(name="extract", tags=["etl"])
        def extract() -> dict[str, list[int]]:
            return {"data": [1, 2, 3]}

        @task(name="transform", tags=["etl"], dependencies=["extract"])
        async def transform(data: dict[str, list[int]]) -> list[int]:
            return [x * 2 for x in data["data"]]

        # Validate
        errors = validate_all_tasks()
        assert len(errors) == 0

        # Get registry
        registry = get_registry()

        # Check tasks are registered
        assert registry.get("extract") is not None
        assert registry.get("transform") is not None

        # Execute extract
        extract_task = registry.get("extract")
        assert extract_task is not None
        extract_executor = ExcutableTask(extract_task)
        extract_result = await extract_executor.execute()
        assert extract_result == {"data": [1, 2, 3]}

        # Execute transform with extract result
        transform_task = registry.get("transform")
        assert transform_task is not None
        transform_executor = ExcutableTask(transform_task)
        transform_result = await transform_executor.execute(extract_result)
        assert transform_result == [2, 4, 6]

    async def test_missing_dependency_detected(self) -> None:
        """Test that missing dependencies are detected."""

        @task(name="task_with_missing_dep", dependencies=["nonexistent"])
        def bad_task() -> None:
            pass

        errors = validate_all_tasks()

        assert len(errors) == 1
        assert "nonexistent" in errors[0]
