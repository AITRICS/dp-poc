"""
Tests for planner module.
"""

from collections.abc import Generator

import pytest

from app.planner import DAG, DAGBuilder, ExecutionPlan, Node, Planner
from app.task_registry import clear_registry, get_registry, task


@pytest.fixture(autouse=True)
def clean_registry() -> Generator[None, None, None]:
    """Clean task registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


class TestNode:
    """Tests for Node class."""

    def test_node_creation(self) -> None:
        """Test basic node creation."""
        from app.task_registry.domain.task_model import TaskMetadata

        task_meta = TaskMetadata(name="test_task", func=lambda: None)
        node = Node(task=task_meta, node_id="test_task")

        assert node.node_id == "test_task"
        assert node.task.name == "test_task"
        assert node.upstream == set()
        assert node.downstream == set()
        assert node.level == 0

    def test_node_with_dependencies(self) -> None:
        """Test node with upstream dependencies."""
        from app.task_registry.domain.task_model import TaskMetadata

        task_meta = TaskMetadata(name="task_b", func=lambda: None, dependencies=["task_a"])
        node = Node(task=task_meta, node_id="task_b", upstream={"task_a"})

        assert node.upstream == {"task_a"}
        assert not node.is_root()

    def test_node_is_ready(self) -> None:
        """Test node readiness check."""
        from app.task_registry.domain.task_model import TaskMetadata

        task_meta = TaskMetadata(name="task_b", func=lambda: None, dependencies=["task_a"])
        node = Node(task=task_meta, node_id="task_b", upstream={"task_a"})

        assert not node.is_ready(set())
        assert not node.is_ready({"other_task"})
        assert node.is_ready({"task_a"})
        assert node.is_ready({"task_a", "task_c"})

    def test_node_validation(self) -> None:
        """Test node validation."""
        from app.task_registry.domain.task_model import TaskMetadata

        task_meta = TaskMetadata(name="test_task", func=lambda: None)

        # Node ID must match task name
        with pytest.raises(ValueError, match="must match task name"):
            Node(task=task_meta, node_id="different_name")


class TestDAG:
    """Tests for DAG class."""

    def test_empty_dag(self) -> None:
        """Test empty DAG creation."""
        dag = DAG()
        assert len(dag) == 0
        assert dag.get_root_nodes() == []
        assert dag.get_leaf_nodes() == []

    def test_add_node(self) -> None:
        """Test adding nodes to DAG."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()
        task_meta = TaskMetadata(name="task_a", func=lambda: None)
        node = Node(task=task_meta, node_id="task_a")

        dag.add_node(node)
        assert len(dag) == 1
        assert "task_a" in dag
        assert dag.get_node("task_a") == node

    def test_duplicate_node(self) -> None:
        """Test that duplicate nodes are rejected."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()
        task_meta = TaskMetadata(name="task_a", func=lambda: None)
        node1 = Node(task=task_meta, node_id="task_a")
        node2 = Node(task=task_meta, node_id="task_a")

        dag.add_node(node1)
        with pytest.raises(ValueError, match="already exists"):
            dag.add_node(node2)

    def test_simple_linear_dag(self) -> None:
        """Test simple linear DAG: A -> B -> C."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        # Create nodes
        task_a = TaskMetadata(name="A", func=lambda: None)
        task_b = TaskMetadata(name="B", func=lambda: None, dependencies=["A"])
        task_c = TaskMetadata(name="C", func=lambda: None, dependencies=["B"])

        node_a = Node(task=task_a, node_id="A")
        node_b = Node(task=task_b, node_id="B", upstream={"A"})
        node_c = Node(task=task_c, node_id="C", upstream={"B"})

        # Add downstream relationships
        node_a.downstream.add("B")
        node_b.downstream.add("C")

        dag.add_node(node_a)
        dag.add_node(node_b)
        dag.add_node(node_c)

        # Validate and test
        errors = dag.validate()
        assert errors == []

        order = dag.topological_sort()
        assert order == ["A", "B", "C"]

        dag.calculate_levels()
        assert node_a.level == 0
        assert node_b.level == 1
        assert node_c.level == 2

    def test_parallel_dag(self) -> None:
        """Test parallel DAG: A -> B, A -> C."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        task_a = TaskMetadata(name="A", func=lambda: None)
        task_b = TaskMetadata(name="B", func=lambda: None, dependencies=["A"])
        task_c = TaskMetadata(name="C", func=lambda: None, dependencies=["A"])

        node_a = Node(task=task_a, node_id="A")
        node_b = Node(task=task_b, node_id="B", upstream={"A"})
        node_c = Node(task=task_c, node_id="C", upstream={"A"})

        node_a.downstream.add("B")
        node_a.downstream.add("C")

        dag.add_node(node_a)
        dag.add_node(node_b)
        dag.add_node(node_c)

        errors = dag.validate()
        assert errors == []

        dag.calculate_levels()
        assert node_a.level == 0
        assert node_b.level == 1
        assert node_c.level == 1  # B and C can run in parallel

    def test_diamond_dag(self) -> None:
        """Test diamond DAG: A -> B -> D, A -> C -> D."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        task_a = TaskMetadata(name="A", func=lambda: None)
        task_b = TaskMetadata(name="B", func=lambda: None, dependencies=["A"])
        task_c = TaskMetadata(name="C", func=lambda: None, dependencies=["A"])
        task_d = TaskMetadata(name="D", func=lambda: None, dependencies=["B", "C"])

        node_a = Node(task=task_a, node_id="A")
        node_b = Node(task=task_b, node_id="B", upstream={"A"})
        node_c = Node(task=task_c, node_id="C", upstream={"A"})
        node_d = Node(task=task_d, node_id="D", upstream={"B", "C"})

        node_a.downstream = {"B", "C"}
        node_b.downstream.add("D")
        node_c.downstream.add("D")

        dag.add_node(node_a)
        dag.add_node(node_b)
        dag.add_node(node_c)
        dag.add_node(node_d)

        errors = dag.validate()
        assert errors == []

        dag.calculate_levels()
        assert node_a.level == 0
        assert node_b.level == 1
        assert node_c.level == 1
        assert node_d.level == 2

    def test_cycle_detection(self) -> None:
        """Test cycle detection: A -> B -> C -> A."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        task_a = TaskMetadata(name="A", func=lambda: None)
        task_b = TaskMetadata(name="B", func=lambda: None)
        task_c = TaskMetadata(name="C", func=lambda: None)

        node_a = Node(task=task_a, node_id="A", upstream={"C"})
        node_b = Node(task=task_b, node_id="B", upstream={"A"})
        node_c = Node(task=task_c, node_id="C", upstream={"B"})

        node_a.downstream.add("B")
        node_b.downstream.add("C")
        node_c.downstream.add("A")

        dag.add_node(node_a)
        dag.add_node(node_b)
        dag.add_node(node_c)

        errors = dag.validate()
        assert len(errors) > 0
        assert any("Cycle detected" in error for error in errors)

    def test_orphan_node_detection(self) -> None:
        """Test orphan node detection."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        task_a = TaskMetadata(name="A", func=lambda: None, dependencies=["unknown"])
        node_a = Node(task=task_a, node_id="A", upstream={"unknown"})

        dag.add_node(node_a)

        errors = dag.validate()
        assert len(errors) > 0
        assert any("does not exist" in error for error in errors)

    def test_self_reference_detection(self) -> None:
        """Test self-reference detection."""
        from app.task_registry.domain.task_model import TaskMetadata

        dag = DAG()

        task_a = TaskMetadata(name="A", func=lambda: None)
        node_a = Node(task=task_a, node_id="A", upstream={"A"})

        dag.add_node(node_a)

        errors = dag.validate()
        assert len(errors) > 0
        assert any("self-reference" in error for error in errors)


class TestDAGBuilder:
    """Tests for DAGBuilder class."""

    def test_build_simple_dag(self) -> None:
        """Test building a simple DAG from registry."""
        registry = get_registry()

        @task(name="task_a")
        def task_a() -> str:
            return "a"

        @task(name="task_b", dependencies=["task_a"])
        def task_b() -> str:
            return "b"

        builder = DAGBuilder(registry)
        dag = builder.build_dag()

        assert len(dag) == 2
        assert "task_a" in dag
        assert "task_b" in dag

        node_a = dag.get_node("task_a")
        node_b = dag.get_node("task_b")

        assert node_a is not None
        assert node_b is not None
        assert node_a.level == 0
        assert node_b.level == 1
        assert node_b.upstream == {"task_a"}
        assert node_a.downstream == {"task_b"}

    def test_build_dag_with_tag_filter(self) -> None:
        """Test building DAG with tag filtering."""
        registry = get_registry()

        @task(name="etl_task", tags=["etl"])
        def etl_task() -> str:
            return "etl"

        @task(name="other_task", tags=["other"])
        def other_task() -> str:
            return "other"

        builder = DAGBuilder(registry)
        dag = builder.build_dag(tags=["etl"])

        assert len(dag) == 1
        assert "etl_task" in dag
        assert "other_task" not in dag

    def test_build_dag_with_task_names(self) -> None:
        """Test building DAG with specific task names."""
        registry = get_registry()

        @task(name="task_a")
        def task_a() -> str:
            return "a"

        @task(name="task_b")
        def task_b() -> str:
            return "b"

        @task(name="task_c")
        def task_c() -> str:
            return "c"

        builder = DAGBuilder(registry)
        dag = builder.build_dag(task_names=["task_a", "task_b"])

        assert len(dag) == 2
        assert "task_a" in dag
        assert "task_b" in dag
        assert "task_c" not in dag

    def test_build_dag_with_missing_task(self) -> None:
        """Test that missing task names raise error."""
        registry = get_registry()

        @task(name="task_a")
        def task_a() -> str:
            return "a"

        builder = DAGBuilder(registry)

        with pytest.raises(ValueError, match="not found in registry"):
            builder.build_dag(task_names=["task_a", "missing_task"])

    def test_build_dag_with_invalid_dependency(self) -> None:
        """Test that invalid dependencies are detected."""
        registry = get_registry()

        @task(name="task_a", dependencies=["non_existent"])
        def task_a() -> str:
            return "a"

        builder = DAGBuilder(registry)

        with pytest.raises(ValueError, match="does not exist"):
            builder.build_dag()


class TestExecutionPlan:
    """Tests for ExecutionPlan class."""

    def test_execution_plan_creation(self) -> None:
        """Test execution plan creation."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C", dependencies=["A"])
        def task_c() -> str:
            return "c"

        builder = DAGBuilder(registry)
        dag = builder.build_dag()
        plan = ExecutionPlan(dag=dag)

        assert len(plan) == 3
        assert plan.execution_order == ["A", "B", "C"] or plan.execution_order == [
            "A",
            "C",
            "B",
        ]
        assert plan.parallel_levels[0] == ["A"]
        assert set(plan.parallel_levels[1]) == {"B", "C"}

    def test_get_ready_nodes(self) -> None:
        """Test getting ready nodes."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C", dependencies=["B"])
        def task_c() -> str:
            return "c"

        builder = DAGBuilder(registry)
        dag = builder.build_dag()
        plan = ExecutionPlan(dag=dag)

        # Initially, only A is ready
        ready = plan.get_ready_nodes(set())
        assert len(ready) == 1
        assert ready[0].node_id == "A"

        # After A completes, B is ready
        ready = plan.get_ready_nodes({"A"})
        assert len(ready) == 1
        assert ready[0].node_id == "B"

        # After B completes, C is ready
        ready = plan.get_ready_nodes({"A", "B"})
        assert len(ready) == 1
        assert ready[0].node_id == "C"

        # All completed
        ready = plan.get_ready_nodes({"A", "B", "C"})
        assert len(ready) == 0

    def test_parallel_execution_plan(self) -> None:
        """Test parallel execution levels."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C", dependencies=["A"])
        def task_c() -> str:
            return "c"

        @task(name="D", dependencies=["B", "C"])
        def task_d() -> str:
            return "d"

        builder = DAGBuilder(registry)
        dag = builder.build_dag()
        plan = ExecutionPlan(dag=dag)

        assert plan.get_total_levels() == 3
        assert plan.parallel_levels[0] == ["A"]
        assert set(plan.parallel_levels[1]) == {"B", "C"}
        assert plan.parallel_levels[2] == ["D"]


class TestPlanner:
    """Tests for Planner class."""

    def test_planner_create_plan(self) -> None:
        """Test planner creates execution plan."""
        registry = get_registry()

        @task(name="extract", tags=["etl"])
        def extract() -> dict[str, list[int]]:
            return {"data": [1, 2, 3]}

        @task(name="transform", tags=["etl"], dependencies=["extract"])
        def transform() -> list[int]:
            return [2, 4, 6]

        @task(name="load", tags=["etl"], dependencies=["transform"])
        def load() -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(tags=["etl"])

        assert len(plan) == 3
        assert plan.execution_order == ["extract", "transform", "load"]

    def test_planner_validate(self) -> None:
        """Test planner validation."""
        registry = get_registry()

        @task(name="task_a")
        def task_a() -> str:
            return "a"

        @task(name="task_b", dependencies=["task_a"])
        def task_b() -> str:
            return "b"

        planner = Planner(registry)
        errors = planner.validate_plan()

        assert errors == []

    def test_planner_validate_with_error(self) -> None:
        """Test planner validation with errors."""
        registry = get_registry()

        @task(name="task_a", dependencies=["non_existent"])
        def task_a() -> str:
            return "a"

        planner = Planner(registry)
        errors = planner.validate_plan()

        assert len(errors) > 0

    def test_planner_get_task_count(self) -> None:
        """Test getting task count."""
        registry = get_registry()

        @task(name="task_a", tags=["etl"])
        def task_a() -> str:
            return "a"

        @task(name="task_b", tags=["etl"])
        def task_b() -> str:
            return "b"

        @task(name="task_c", tags=["other"])
        def task_c() -> str:
            return "c"

        planner = Planner(registry)

        assert planner.get_task_count() == 3
        assert planner.get_task_count(tags=["etl"]) == 2
        assert planner.get_task_count(task_names=["task_a"]) == 1


class TestIntegration:
    """Integration tests."""

    def test_full_workflow(self) -> None:
        """Test complete workflow from task registration to execution plan."""
        registry = get_registry()

        # Register tasks
        @task(name="extract_data", tags=["etl", "extraction"])
        def extract() -> dict[str, list[int]]:
            """Extract data from source."""
            return {"values": [1, 2, 3, 4, 5]}

        @task(
            name="transform_data",
            tags=["etl", "transformation"],
            dependencies=["extract_data"],
        )
        def transform() -> dict[str, float]:
            """Transform extracted data."""
            return {"sum": 15.0, "count": 5.0, "avg": 3.0}

        @task(name="load_data", tags=["etl", "loading"], dependencies=["transform_data"])
        def load() -> None:
            """Load transformed data."""
            pass

        # Create planner
        planner = Planner(registry)

        # Validate
        errors = planner.validate_plan(tags=["etl"])
        assert errors == []

        # Create execution plan
        plan = planner.create_execution_plan(tags=["etl"])

        assert len(plan) == 3
        assert plan.execution_order == ["extract_data", "transform_data", "load_data"]
        assert plan.parallel_levels == [
            ["extract_data"],
            ["transform_data"],
            ["load_data"],
        ]

        # Simulate execution
        completed: set[str] = set()
        for level_tasks in plan.parallel_levels:
            # All tasks in this level can execute in parallel
            for task_id in level_tasks:
                node = plan.get_node(task_id)
                assert node is not None
                assert node.is_ready(completed)

            # Mark as completed
            completed.update(level_tasks)

        assert len(completed) == 3


class TestDAGID:
    """Tests for DAG ID functionality."""

    def test_dag_content_hash_generation(self) -> None:
        """Test that DAG generates content hash automatically."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        builder = DAGBuilder(registry)
        dag = builder.build_dag()

        # Should have a dag_id (content hash)
        assert dag.dag_id
        assert len(dag.dag_id) == 16  # First 16 chars of SHA256

    def test_dag_user_provided_id(self) -> None:
        """Test that user-provided DAG ID is used."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        builder = DAGBuilder(registry)
        dag = builder.build_dag(dag_id="my_custom_dag")

        assert dag.dag_id == "my_custom_dag"

    def test_same_structure_same_hash(self) -> None:
        """Test that same DAG structure produces same content hash."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        builder = DAGBuilder(registry)
        dag1 = builder.build_dag()
        dag2 = builder.build_dag()

        # Same structure → same content hash
        assert dag1.get_content_hash() == dag2.get_content_hash()
        assert dag1.is_same_structure(dag2)

    def test_different_structure_different_hash(self) -> None:
        """Test that different DAG structures produce different hashes."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C")
        def task_c() -> str:
            return "c"

        builder = DAGBuilder(registry)
        dag1 = builder.build_dag(task_names=["A", "B"])
        dag2 = builder.build_dag(task_names=["C"])

        # Different structure → different hash
        assert dag1.get_content_hash() != dag2.get_content_hash()
        assert not dag1.is_same_structure(dag2)


class TestRootTasks:
    """Tests for root_tasks functionality."""

    def test_build_dag_from_single_root(self) -> None:
        """Test building DAG from a single root task."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C")
        def task_c() -> str:
            return "c"

        planner = Planner(registry)
        plan = planner.create_execution_plan(root_tasks=["A"])

        # Should only include A and B (A's downstream)
        assert len(plan) == 2
        assert "A" in plan.execution_order
        assert "B" in plan.execution_order
        assert "C" not in plan.execution_order

    def test_build_dag_from_multiple_roots(self) -> None:
        """Test building DAG from multiple root tasks."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C")
        def task_c() -> str:
            return "c"

        @task(name="D", dependencies=["C"])
        def task_d() -> str:
            return "d"

        planner = Planner(registry)
        plan = planner.create_execution_plan(root_tasks=["A", "C"])

        # Should include A, B, C, D
        assert len(plan) == 4

    def test_build_dag_with_include_upstream(self) -> None:
        """Test building DAG with include_upstream."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        @task(name="B", dependencies=["A"])
        def task_b() -> str:
            return "b"

        @task(name="C", dependencies=["B"])
        def task_c() -> str:
            return "c"

        planner = Planner(registry)

        # Start from C without upstream
        plan1 = planner.create_execution_plan(root_tasks=["C"])
        assert len(plan1) == 1  # Only C
        assert "C" in plan1.execution_order

        # Start from C with upstream
        plan2 = planner.create_execution_plan(root_tasks=["C"], include_upstream=True)
        assert len(plan2) == 3  # A, B, C
        assert set(plan2.execution_order) == {"A", "B", "C"}

    def test_independent_dags_from_same_registry(self) -> None:
        """Test creating independent DAGs from the same registry."""
        registry = get_registry()

        @task(name="extract1")
        def extract1() -> str:
            return "data1"

        @task(name="transform1", dependencies=["extract1"])
        def transform1() -> str:
            return "transformed1"

        @task(name="extract2")
        def extract2() -> str:
            return "data2"

        @task(name="transform2", dependencies=["extract2"])
        def transform2() -> str:
            return "transformed2"

        planner = Planner(registry)

        # Create two independent DAGs
        plan1 = planner.create_execution_plan(root_tasks=["extract1"])
        plan2 = planner.create_execution_plan(root_tasks=["extract2"])

        # Should be different DAGs
        assert len(plan1) == 2
        assert len(plan2) == 2
        assert set(plan1.execution_order) == {"extract1", "transform1"}
        assert set(plan2.execution_order) == {"extract2", "transform2"}

        # Different structures → different content hashes
        assert plan1.dag.get_content_hash() != plan2.dag.get_content_hash()


class TestPlannerWithDAGID:
    """Tests for Planner with DAG ID."""

    def test_planner_with_explicit_dag_id(self) -> None:
        """Test planner with explicit DAG ID."""
        registry = get_registry()

        @task(name="extract", tags=["etl"])
        def extract() -> str:
            return "data"

        @task(name="load", dependencies=["extract"], tags=["etl"])
        def load() -> None:
            pass

        planner = Planner(registry)
        plan = planner.create_execution_plan(root_tasks=["extract"], dag_id="daily_etl")

        assert plan.dag.dag_id == "daily_etl"

    def test_planner_auto_generates_dag_id(self) -> None:
        """Test that planner auto-generates DAG ID if not provided."""
        registry = get_registry()

        @task(name="A")
        def task_a() -> str:
            return "a"

        planner = Planner(registry)
        plan = planner.create_execution_plan(root_tasks=["A"])

        # Should have auto-generated content hash
        assert plan.dag.dag_id
        assert len(plan.dag.dag_id) == 16
