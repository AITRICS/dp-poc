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


class TaskResultEvent(EventBase):
    """Event to signal task execution result from Worker to Orchestrator.

    This event carries task execution result information including status,
    timing, retry information, and streaming details.

    Example:
        # Success result
        event = TaskResultEvent(
            topic="task.result.success",
            run_id="run_123",
            task_name="extract",
            task_result_id="result_456",
            status="SUCCESS",
            execution_time=1.5,
            retry_count=0
        )

        # Streaming result
        event = TaskResultEvent(
            topic="task.result.streaming",
            run_id="run_123",
            task_name="stream_data",
            task_result_id="result_789",
            status="SUCCESS",
            execution_time=0.5,
            retry_count=0,
            is_streaming=True,
            stream_index=0,
            stream_complete=False
        )
    """

    def __init__(
        self,
        topic: str,
        run_id: str,
        task_name: str,
        task_result_id: str,
        status: str,
        execution_time: float,
        retry_count: int = 0,
        error_message: str | None = None,
        error_type: str | None = None,
        is_streaming: bool = False,
        stream_index: int | None = None,
        stream_complete: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize TaskResultEvent with execution result information.

        Args:
            topic: Event topic (e.g., "task.result.success")
            run_id: Unique identifier for the pipeline run
            task_name: Name of the executed task
            task_result_id: Unique identifier for this execution result
            status: Execution status (SUCCESS, FAILED, TIMEOUT, CANCELLED)
            execution_time: Time taken to execute in seconds
            retry_count: Number of retries attempted
            error_message: Error message if task failed (optional)
            error_type: Type of error that occurred (optional)
            is_streaming: Whether this result is part of streaming output
            stream_index: Index of this result in stream (optional)
            stream_complete: Whether this is the last result in stream (optional)
            metadata: Additional metadata (optional)
        """
        kwargs: dict[str, Any] = {
            "run_id": run_id,
            "task_name": task_name,
            "task_result_id": task_result_id,
            "status": status,
            "execution_time": execution_time,
            "retry_count": retry_count,
            "error_message": error_message,
            "error_type": error_type,
            "is_streaming": is_streaming,
            "stream_index": stream_index,
            "stream_complete": stream_complete,
            "metadata": [metadata] if metadata is not None else None,
        }

        super().__init__(topic=topic, **kwargs)

    def get_run_id(self) -> str:
        """Get run ID from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return str(self.content["run_id"].item())

    def get_task_name(self) -> str:
        """Get task name from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return str(self.content["task_name"].item())

    def get_task_result_id(self) -> str:
        """Get task result ID from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return str(self.content["task_result_id"].item())

    def get_status(self) -> str:
        """Get status from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return str(self.content["status"].item())

    def get_execution_time(self) -> float:
        """Get execution time from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return float(self.content["execution_time"].item())

    def get_retry_count(self) -> int:
        """Get retry count from event content."""
        if self.content is None:
            raise ValueError("TaskResultEvent content is None")
        return int(self.content["retry_count"].item())

    def get_error_message(self) -> str | None:
        """Get error message from event content."""
        if self.content is None:
            return None
        msg = self.content["error_message"].item()
        return str(msg) if msg is not None else None

    def get_error_type(self) -> str | None:
        """Get error type from event content."""
        if self.content is None:
            return None
        err_type = self.content["error_type"].item()
        return str(err_type) if err_type is not None else None

    def get_is_streaming(self) -> bool:
        """Get is_streaming flag from event content."""
        if self.content is None:
            return False
        return bool(self.content["is_streaming"].item())

    def get_stream_index(self) -> int | None:
        """Get stream index from event content."""
        if self.content is None:
            return None
        idx = self.content["stream_index"].item()
        return int(idx) if idx is not None else None

    def get_stream_complete(self) -> bool | None:
        """Get stream_complete flag from event content."""
        if self.content is None:
            return None
        complete = self.content["stream_complete"].item()
        return bool(complete) if complete is not None else None

    def get_metadata(self) -> dict[str, Any] | None:
        """Get metadata dict from event content."""
        if self.content is None:
            return None
        metadata = self.content["metadata"].item()
        return dict(metadata) if metadata is not None else None


class TaskSubmitEvent(EventBase):
    """Event to submit a task for execution.

    This event carries all information needed to execute a task.
    When received by the Orchestrator, it submits the task to the worker pool.

    Example:
        event = TaskSubmitEvent(
            topic="task.submit",
            run_id="run_123",
            task_name="extract",
            task_result_id="result_456",
            inputs={"param1": ("upstream_task", "result_id")},
            retry_count=0,
            max_retries=3
        )
    """

    def __init__(
        self,
        topic: str,
        run_id: str,
        task_name: str,
        task_result_id: str,
        inputs: dict[str, tuple[str, str]] | None = None,
        retry_count: int = 0,
        max_retries: int = 0,
    ) -> None:
        """Initialize TaskSubmitEvent with task execution information.

        Args:
            topic: Event topic (e.g., "task.submit")
            run_id: Unique identifier for the pipeline run
            task_name: Name of the task to execute
            task_result_id: Unique identifier for this execution result
            inputs: Dictionary mapping parameter names to input locations
            retry_count: Current retry attempt number
            max_retries: Maximum number of retries allowed
        """
        # Convert inputs dict to a serializable format
        # inputs_list will be a list of dicts: [{"param": "name", "upstream": "task", "result_id": "id"}]
        inputs_list = []
        if inputs:
            for param_name, (upstream_task, result_id) in inputs.items():
                inputs_list.append({
                    "param": param_name,
                    "upstream": upstream_task,
                    "result_id": result_id,
                })

        kwargs: dict[str, Any] = {
            "run_id": run_id,
            "task_name": task_name,
            "task_result_id": task_result_id,
            "inputs": [inputs_list] if inputs_list else None,
            "retry_count": retry_count,
            "max_retries": max_retries,
        }

        super().__init__(topic=topic, **kwargs)

    def get_run_id(self) -> str:
        """Get run ID from event content."""
        if self.content is None:
            raise ValueError("TaskSubmitEvent content is None")
        return str(self.content["run_id"].item())

    def get_task_name(self) -> str:
        """Get task name from event content."""
        if self.content is None:
            raise ValueError("TaskSubmitEvent content is None")
        return str(self.content["task_name"].item())

    def get_task_result_id(self) -> str:
        """Get task result ID from event content."""
        if self.content is None:
            raise ValueError("TaskSubmitEvent content is None")
        return str(self.content["task_result_id"].item())

    def get_inputs(self) -> dict[str, tuple[str, str]]:
        """Get inputs dict from event content."""
        if self.content is None:
            return {}
        
        inputs_list = self.content["inputs"].item()
        if inputs_list is None:
            return {}
        
        # Convert list of dicts back to inputs dict
        inputs: dict[str, tuple[str, str]] = {}
        for item in inputs_list:
            param_name = item["param"]
            upstream_task = item["upstream"]
            result_id = item["result_id"]
            inputs[param_name] = (upstream_task, result_id)
        
        return inputs

    def get_retry_count(self) -> int:
        """Get retry count from event content."""
        if self.content is None:
            return 0
        return int(self.content["retry_count"].item())

    def get_max_retries(self) -> int:
        """Get max retries from event content."""
        if self.content is None:
            return 0
        return int(self.content["max_retries"].item())


class ExecutionResultEvent(EventBase):
    """Event to signal DAG execution completion.

    This event is published when a DAG execution completes (successfully or not).
    It contains summary information about the execution including status,
    completed/failed tasks, and timing information.

    Example:
        event = ExecutionResultEvent(
            topic="dag.execution.result",
            run_id="run_123",
            dag_id="my_dag",
            status="SUCCESS",
            completed_tasks=["extract", "transform", "load"],
            failed_tasks=[],
            total_execution_time=10.5,
            metadata={"triggered_by": "scheduler"}
        )
    """

    def __init__(
        self,
        topic: str,
        run_id: str,
        dag_id: str,
        status: str,
        completed_tasks: list[str],
        failed_tasks: list[str],
        total_execution_time: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize ExecutionResultEvent with DAG execution result.

        Args:
            topic: Event topic (e.g., "dag.execution.result")
            run_id: Unique identifier for the pipeline run
            dag_id: DAG identifier
            status: Final execution status (SUCCESS, FAILED)
            completed_tasks: List of successfully completed task names
            failed_tasks: List of failed task names
            total_execution_time: Total execution time in seconds
            metadata: Additional metadata (optional)
        """
        kwargs: dict[str, Any] = {
            "run_id": run_id,
            "dag_id": dag_id,
            "status": status,
            "completed_tasks": [completed_tasks],
            "failed_tasks": [failed_tasks],
            "total_execution_time": total_execution_time,
            "metadata": [metadata] if metadata is not None else None,
        }

        super().__init__(topic=topic, **kwargs)

    def get_run_id(self) -> str:
        """Get run ID from event content."""
        if self.content is None:
            raise ValueError("ExecutionResultEvent content is None")
        return str(self.content["run_id"].item())

    def get_dag_id(self) -> str:
        """Get DAG ID from event content."""
        if self.content is None:
            raise ValueError("ExecutionResultEvent content is None")
        return str(self.content["dag_id"].item())

    def get_status(self) -> str:
        """Get status from event content."""
        if self.content is None:
            raise ValueError("ExecutionResultEvent content is None")
        return str(self.content["status"].item())

    def get_completed_tasks(self) -> list[str]:
        """Get completed tasks list from event content."""
        if self.content is None:
            return []
        tasks = self.content["completed_tasks"].item()
        return list(tasks) if tasks is not None else []

    def get_failed_tasks(self) -> list[str]:
        """Get failed tasks list from event content."""
        if self.content is None:
            return []
        tasks = self.content["failed_tasks"].item()
        return list(tasks) if tasks is not None else []

    def get_total_execution_time(self) -> float:
        """Get total execution time from event content."""
        if self.content is None:
            raise ValueError("ExecutionResultEvent content is None")
        return float(self.content["total_execution_time"].item())

    def get_metadata(self) -> dict[str, Any] | None:
        """Get metadata dict from event content."""
        if self.content is None:
            return None
        metadata = self.content["metadata"].item()
        return dict(metadata) if metadata is not None else None
