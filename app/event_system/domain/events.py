import uuid
from datetime import UTC, datetime

import polars as pl
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
    event_id: uuid.UUID = Field(description="The unique event ID", default_factory=uuid.uuid4)
    timestamp: datetime = Field(
        description="The timestamp of the event",
        default_factory=lambda: datetime.now(UTC),
    )


class EventBase(BaseModel):
    """
    The abstract base class for all events in the system.
    Contains metadata and optional content (DataFrame).
    """

    model_config = {"arbitrary_types_allowed": True}

    meta: EventMeta = Field(default_factory=EventMeta, description="Event metadata")
    content: pl.DataFrame | None = Field(default=None, description="Event data content")


class CompletedEvent(EventBase):
    """
    Signal event to indicate the completion of an event stream.
    """

    pass
