"""Pytest configuration and shared fixtures."""

import pytest

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher


class DummyEvent(EventBase):
    """Test event for testing purposes."""

    message: str


class AnotherDummyEvent(EventBase):
    """Another test event for testing purposes."""

    value: int


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
