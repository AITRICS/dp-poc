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
