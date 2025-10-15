import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import polars as pl
import uuid6
from pydantic import BaseModel, Field


class EventMeta(BaseModel):
    """
    The meta data for all events in the system.
    It automatically provides a unique event ID and a timestamp.
    """

    topic: str = Field(
        default="",
        description="The topic of the event",
        examples=["data_pipeline_events"],
    )
    event_id: uuid.UUID = Field(description="The unique event ID", default_factory=uuid6.uuid7)
    timestamp: datetime = Field(
        description="The timestamp of the event",
        default_factory=lambda: datetime.now(UTC),
    )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.event_id}, topic={self.topic})"

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class EventBase:
    """
    The abstract base class for all events in the system.
    Contains metadata and optional content (DataFrame).
    """

    meta: EventMeta
    content: pl.DataFrame | None

    def __init__(self, topic: str = "", **kwargs: Any) -> None:
        """
        Initialize an event with topic and optional content.

        Args:
            topic: The topic name for this event.
            **kwargs: Additional data to be stored in the content DataFrame.
        """
        self.meta = EventMeta(topic=topic)
        if kwargs:
            self.content = pl.DataFrame(kwargs)
        else:
            self.content = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.meta.event_id}, topic={self.meta.topic})"

    def __repr__(self) -> str:
        return self.__str__()


class CompletedEvent(EventBase):
    """
    Signal event to indicate the completion of an event stream.
    """

    def __init__(self, topic: str = "") -> None:
        """
        Initialize a CompletedEvent.

        Args:
            topic: The topic name for this event.
        """
        super().__init__(topic=topic)


class DAGExecutionEvent(EventBase):
    """Event to trigger DAG execution in Orchestrator.

    This event carries all necessary information for the Orchestrator
    to identify and execute a specific DAG or subset of tasks.

    The event can specify the DAG to execute in multiple ways:
    - dag_id: Direct DAG identifier
    - tags: Filter DAG by tags
    - task_names: Specific task names to include

    Additionally, you can control execution flow:
    - root_tasks: Start execution from specific tasks (partial execution)
    - include_upstream: Include upstream dependencies when using root_tasks
    - run_metadata: Additional metadata for the run

    At least one of dag_id, tags, or task_names must be provided.

    Example:
        # Execute entire DAG by tags
        event = DAGExecutionEvent(
            topic="dag.execution.etl",
            tags=["etl", "daily"]
        )

        # Execute from specific root tasks
        event = DAGExecutionEvent(
            topic="dag.execution.partial",
            dag_id="my_dag_123",
            root_tasks=["transform", "load"],
            include_upstream=False
        )

        # Execute specific tasks only
        event = DAGExecutionEvent(
            topic="dag.execution.custom",
            task_names=["extract", "transform"],
            run_metadata={"triggered_by": "user", "reason": "manual_rerun"}
        )
    """

    def __init__(
        self,
        topic: str,
        dag_id: str | None = None,
        tags: list[str] | None = None,
        task_names: list[str] | None = None,
        root_tasks: list[str] | None = None,
        include_upstream: bool = False,
        run_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize DAGExecutionEvent with execution configuration.

        Args:
            topic: Event topic (e.g., "dag.execution.etl")
            dag_id: Direct DAG identifier (optional)
            tags: List of tags to filter DAG (optional)
            task_names: Specific task names to include (optional)
            root_tasks: Tasks to start execution from (optional)
            include_upstream: Include upstream dependencies with root_tasks
            run_metadata: Additional metadata for the run (optional)

        Raises:
            ValueError: If none of dag_id, tags, or task_names is provided
        """
        # Validate at least one DAG identifier is provided
        has_dag_id = dag_id is not None
        has_tags = tags is not None and len(tags) > 0
        has_task_names = task_names is not None and len(task_names) > 0

        if not any([has_dag_id, has_tags, has_task_names]):
            raise ValueError("At least one of dag_id, tags, or task_names must be provided")

        # Prepare kwargs for EventBase
        # Polars natively supports list and struct (dict) types, so no JSON conversion needed
        kwargs: dict[str, Any] = {
            "dag_id": dag_id,
            "tags": [tags],
            "task_names": [task_names],
            "root_tasks": [root_tasks] if root_tasks is not None else None,
            "include_upstream": [include_upstream] if include_upstream else None,
            "run_metadata": [run_metadata] if run_metadata is not None else None,
        }

        super().__init__(topic=topic, **kwargs)

    def get_dag_id(self) -> str | None:
        """Get DAG ID from event content.

        Returns:
            DAG ID if present, None otherwise
        """
        if self.content is None:
            return None
        dag_id = self.content["dag_id"].item()
        return dag_id if dag_id is not None else None

    def get_tags(self) -> list[str] | None:
        """Get tags list from event content.

        Returns:
            List of tags if present, None otherwise
        """
        if self.content is None:
            return None
        tags = self.content["tags"].item()
        return list(tags) if tags is not None else None

    def get_task_names(self) -> list[str] | None:
        """Get task names list from event content.

        Returns:
            List of task names if present, None otherwise
        """
        if self.content is None:
            return None
        task_names = self.content["task_names"].item()
        return list(task_names) if task_names is not None else None

    def get_root_tasks(self) -> list[str] | None:
        """Get root tasks list from event content.

        Returns:
            List of root tasks if present, None otherwise
        """
        if self.content is None:
            return None
        root_tasks = self.content["root_tasks"].item()
        return list(root_tasks) if root_tasks is not None else None

    def get_include_upstream(self) -> bool:
        """Get include_upstream flag from event content.

        Returns:
            Boolean flag indicating whether to include upstream dependencies
        """
        if self.content is None:
            return False
        return bool(self.content["include_upstream"].item())

    def get_run_metadata(self) -> dict[str, Any] | None:
        """Get run metadata dict from event content.

        Returns:
            Dictionary of run metadata if present, None otherwise
        """
        if self.content is None:
            return None
        metadata = self.content["run_metadata"].item()
        return dict(metadata) if metadata is not None else None
