"""Tests for DAGExecutionEvent."""

import pytest

from app.event_system.domain.events import DAGExecutionEvent


class TestDAGExecutionEvent:
    """Test cases for DAGExecutionEvent."""

    def test_create_event_with_dag_id(self) -> None:
        """Test creating event with dag_id."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            dag_id="dag_123",
        )

        assert event.meta.topic == "dag.execution.test"
        assert event.get_dag_id() == "dag_123"
        assert event.get_tags() is None
        assert event.get_task_names() is None
        assert event.get_root_tasks() is None
        assert event.get_include_upstream() is False
        assert event.get_run_metadata() is None

    def test_create_event_with_tags(self) -> None:
        """Test creating event with tags."""
        event = DAGExecutionEvent(
            topic="dag.execution.etl",
            tags=["etl", "daily"],
        )

        assert event.meta.topic == "dag.execution.etl"
        assert event.get_dag_id() is None
        assert event.get_tags() == ["etl", "daily"]
        assert event.get_task_names() is None

    def test_create_event_with_task_names(self) -> None:
        """Test creating event with task_names."""
        event = DAGExecutionEvent(
            topic="dag.execution.custom",
            task_names=["extract", "transform", "load"],
        )

        assert event.meta.topic == "dag.execution.custom"
        assert event.get_dag_id() is None
        assert event.get_tags() is None
        assert event.get_task_names() == ["extract", "transform", "load"]

    def test_create_event_with_root_tasks(self) -> None:
        """Test creating event with root_tasks."""
        event = DAGExecutionEvent(
            topic="dag.execution.partial",
            dag_id="dag_123",
            root_tasks=["transform", "load"],
            include_upstream=False,
        )

        assert event.get_dag_id() == "dag_123"
        assert event.get_root_tasks() == ["transform", "load"]
        assert event.get_include_upstream() is False

    def test_create_event_with_include_upstream(self) -> None:
        """Test creating event with include_upstream flag."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            tags=["etl"],
            root_tasks=["transform"],
            include_upstream=True,
        )

        assert event.get_root_tasks() == ["transform"]
        assert event.get_include_upstream() is True

    def test_create_event_with_run_metadata(self) -> None:
        """Test creating event with run_metadata."""
        metadata = {
            "triggered_by": "user",
            "reason": "manual_rerun",
            "retry_count": 2,
        }
        event = DAGExecutionEvent(
            topic="dag.execution.retry",
            dag_id="dag_456",
            run_metadata=metadata,
        )

        assert event.get_run_metadata() == metadata

    def test_create_event_with_all_parameters(self) -> None:
        """Test creating event with all parameters."""
        metadata = {"user": "admin"}
        event = DAGExecutionEvent(
            topic="dag.execution.full",
            dag_id="dag_789",
            tags=["etl", "hourly"],
            task_names=["task_a", "task_b"],
            root_tasks=["task_b"],
            include_upstream=True,
            run_metadata=metadata,
        )

        assert event.get_dag_id() == "dag_789"
        assert event.get_tags() == ["etl", "hourly"]
        assert event.get_task_names() == ["task_a", "task_b"]
        assert event.get_root_tasks() == ["task_b"]
        assert event.get_include_upstream() is True
        assert event.get_run_metadata() == metadata

    def test_create_event_without_identifiers_raises_error(self) -> None:
        """Test that creating event without identifiers raises ValueError."""
        with pytest.raises(ValueError, match="At least one of dag_id, tags, or task_names"):
            DAGExecutionEvent(
                topic="dag.execution.invalid",
            )

    def test_create_event_with_only_root_tasks_raises_error(self) -> None:
        """Test that root_tasks alone without identifiers raises ValueError."""
        with pytest.raises(ValueError, match="At least one of dag_id, tags, or task_names"):
            DAGExecutionEvent(
                topic="dag.execution.invalid",
                root_tasks=["transform"],
            )

    def test_create_event_with_only_run_metadata_raises_error(self) -> None:
        """Test that run_metadata alone without identifiers raises ValueError."""
        with pytest.raises(ValueError, match="At least one of dag_id, tags, or task_names"):
            DAGExecutionEvent(
                topic="dag.execution.invalid",
                run_metadata={"key": "value"},
            )

    def test_event_has_meta_attributes(self) -> None:
        """Test that event has proper meta attributes."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            dag_id="dag_123",
        )

        assert event.meta.topic == "dag.execution.test"
        assert event.meta.event_id is not None
        assert event.meta.timestamp is not None

    def test_event_str_representation(self) -> None:
        """Test event string representation."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            tags=["etl"],
        )

        str_repr = str(event)
        assert "DAGExecutionEvent" in str_repr
        assert "dag.execution.test" in str_repr

    def test_event_content_is_dataframe(self) -> None:
        """Test that event content is a DataFrame."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            dag_id="dag_123",
            tags=["etl"],
        )

        assert event.content is not None
        assert hasattr(event.content, "shape")  # Polars DataFrame check

    def test_empty_run_metadata(self) -> None:
        """Test that empty run_metadata is handled correctly."""
        event = DAGExecutionEvent(
            topic="dag.execution.test",
            dag_id="dag_123",
            run_metadata={},
        )

        assert event.get_dag_id() == "dag_123"
        assert event.get_run_metadata() == {}

    def test_empty_tags_raises_error(self) -> None:
        """Test that empty tags list raises ValueError."""
        with pytest.raises(ValueError, match="At least one of dag_id, tags, or task_names"):
            DAGExecutionEvent(
                topic="dag.execution.test",
                tags=[],
            )

    def test_complex_metadata(self) -> None:
        """Test that complex metadata structures are preserved."""
        metadata = {
            "user": "admin",
            "config": {
                "retry": True,
                "max_retries": 3,
            },
            "tags": ["important", "manual"],
        }
        event = DAGExecutionEvent(
            topic="dag.execution.complex",
            dag_id="dag_complex",
            run_metadata=metadata,
        )

        retrieved = event.get_run_metadata()
        assert retrieved == metadata
        assert retrieved["config"]["retry"] is True
        assert retrieved["tags"] == ["important", "manual"]
