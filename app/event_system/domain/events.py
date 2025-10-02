from abc import ABC
from dataclasses import dataclass, field
import uuid
from datetime import datetime, timezone


@dataclass(frozen=True)
class EventBase(ABC):
    """
    The abstract base class for all events in the system.
    It automatically provides a unique event ID and a timestamp.
    """

    topic: str = field(init=False)
    event_id: uuid.UUID = field(default_factory=uuid.uuid4, init=False)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc), init=False
    )


@dataclass(frozen=True)
class CompletedEvent(EventBase):
    """
    The abstract base class for all completed events in the system.
    It automatically provides a unique event ID and a timestamp.
    """

    pass
