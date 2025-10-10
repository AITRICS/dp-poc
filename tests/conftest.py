"""Pytest configuration and shared fixtures."""

from typing import Any

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher


class DummyEvent(EventBase):
    """Test event for testing purposes."""

    message: str

    def __init__(self, topic: str = "", message: str = "", **kwargs: Any) -> None:
        super().__init__(topic=topic, message=message, **kwargs)
        self.message = message


class AnotherDummyEvent(EventBase):
    """Another test event for testing purposes."""

    value: int

    def __init__(self, topic: str = "", value: int = 0, **kwargs: Any) -> None:
        super().__init__(topic=topic, value=value, **kwargs)
        self.value = value


@pytest.fixture
def broker() -> InMemoryBroker[EventBase]:
    """Create an in-memory broker instance."""
    return InMemoryBroker()


@pytest.fixture
def publisher(broker: InMemoryBroker[EventBase]) -> InMemoryPublisher[EventBase]:
    """Create an in-memory publisher instance."""
    return InMemoryPublisher(broker)


@pytest.fixture
def consumer(broker: InMemoryBroker[EventBase]) -> InMemoryConsumer[EventBase]:
    """Create an in-memory consumer instance."""
    return InMemoryConsumer(broker)


@pytest.fixture
def test_topic() -> str:
    """Return a test topic name."""
    return "test_topic"
