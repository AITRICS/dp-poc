import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    """
    The abstract base class for all events in the system.
    It automatically provides a unique event ID and a timestamp.
    Note: This is a marker class for all events, no abstract methods required.
    """

    topic: str = Field(examples=["data_pipeline_events"])
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4, examples=["Event ID"])
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        examples=["2025-01-01 00:00:00"],
    )


class CompletedEvent(EventBase):
    """
    The abstract base class for all completed events in the system.
    It automatically provides a unique event ID and a timestamp.
    """

    pass
