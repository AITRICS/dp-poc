import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    """
    The abstract base class for all events in the system.
    It automatically provides a unique event ID and a timestamp.
    Note: This is a marker class for all events, no abstract methods required.
    """

    topic: str = Field(init=False, default="")
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4, init=False)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC), init=False)


class CompletedEvent(EventBase):
    """
    The abstract base class for all completed events in the system.
    It automatically provides a unique event ID and a timestamp.
    """

    pass
