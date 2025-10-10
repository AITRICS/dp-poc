import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher


# 1. Define concrete events based on EventBase
@dataclass
class PipelineStarted(EventBase):
    pipeline_name: str

    def __init__(self, topic: str = "", pipeline_name: str = "", **kwargs: Any) -> None:
        super().__init__(topic=topic, pipeline_name=pipeline_name, **kwargs)
        self.pipeline_name = pipeline_name


class DataIngestionComplete(EventBase):
    source_name: str
    rows_ingested: int

    def __init__(
        self,
        topic: str = "",
        source_name: str = "",
        rows_ingested: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            topic=topic, source_name=source_name, rows_ingested=rows_ingested, **kwargs
        )
        self.source_name = source_name
        self.rows_ingested = rows_ingested


@dataclass
class EventSystem:
    """A container for the core components of the event system."""

    broker: InMemoryBroker[EventBase]
    publisher: InMemoryPublisher[EventBase]
    consumer: InMemoryConsumer[EventBase]


def setup_event_system() -> EventSystem:
    """
    Initializes and wires up the core components of the event system.
    This function acts as the Composition Root.
    """
    logging.info("Setting up the event system...")
    broker = InMemoryBroker[EventBase]()
    publisher = InMemoryPublisher(broker)
    consumer = InMemoryConsumer(broker)
    logging.info("Event system setup complete.")
    return EventSystem(broker=broker, publisher=publisher, consumer=consumer)


async def consume_events(consumer: InMemoryConsumer[EventBase], topic: str) -> None:
    """
    Consumes events from a specific topic.
    """
    logging.info("Starting event consumption...")
    async for event in consumer.consume(topic):
        logging.info(f"Processing event: {event.__class__.__name__}")
        if isinstance(event, PipelineStarted):
            logging.info(f"  Pipeline '{event.pipeline_name}' has started!")
        elif isinstance(event, DataIngestionComplete):
            logging.info(f"  Ingested {event.rows_ingested} rows from {event.source_name}")
    logging.info("Event consumption completed.")


async def consume_pattern_events(consumer: InMemoryConsumer[EventBase], topic_pattern: str) -> None:
    """
    Consumes events from topics matching a pattern.
    """
    logging.info(f"Starting pattern consumption for: {topic_pattern}")
    event_count = 0
    async for event in consumer.consume_pattern(topic_pattern):
        event_count += 1
        logging.info(f"[Pattern {topic_pattern}] Event #{event_count}: {event.__class__.__name__}")
        if isinstance(event, PipelineStarted):
            logging.info(f"  Pipeline '{event.pipeline_name}' has started!")
        elif isinstance(event, DataIngestionComplete):
            logging.info(f"  Ingested {event.rows_ingested} rows from {event.source_name}")
    logging.info(f"Pattern consumption completed. Total events: {event_count}")


async def main(
    publisher: InMemoryPublisher[EventBase], consumer: InMemoryConsumer[EventBase]
) -> None:
    """
    Demonstrates the standard event system simulation.
    """
    topic = "data_pipeline_events"
    consumer_task = asyncio.create_task(consume_events(consumer, topic))
    await asyncio.sleep(0.1)

    await publisher.publish(topic, PipelineStarted(topic=topic, pipeline_name="daily_sales_report"))
    await publisher.publish(
        topic,
        DataIngestionComplete(topic=topic, source_name="pos_terminal_1", rows_ingested=1500),
    )
    await publisher.publish(topic, CompletedEvent(topic=topic))
    await consumer_task


async def main_with_pattern(
    broker: InMemoryBroker[EventBase],
    publisher: InMemoryPublisher[EventBase],
    consumer: InMemoryConsumer[EventBase],
) -> None:
    """
    Demonstrates the wildcard pattern matching feature.
    """
    logging.info("\n" + "=" * 80)
    logging.info("WILDCARD PATTERN MATCHING DEMO")
    logging.info("=" * 80)

    topics = [
        "data.pipeline.ingestion",
        "data.pipeline.transformation",
        "data.warehouse.load",
        "analytics.report.daily",
    ]

    logging.info("\n1. Declaring topics...")
    for topic in topics:
        await broker.declare_topic(topic)
    logging.info(f"   Declared {len(topics)} topics: {topics}")

    logging.info("2. Starting pattern consumer for 'data.**'")
    data_all_consumer_task = asyncio.create_task(consume_pattern_events(consumer, "data.**"))
    await asyncio.sleep(0.1)

    logging.info("\n3. Publishing events to various topics...")
    await publisher.publish(
        "data.pipeline.ingestion",
        PipelineStarted(topic="data.pipeline.ingestion", pipeline_name="ingestion_job"),
    )
    await publisher.publish(
        "data.pipeline.transformation",
        DataIngestionComplete(
            topic="data.pipeline.transformation",
            source_name="raw_data",
            rows_ingested=5000,
        ),
    )

    logging.info("\n4. Publishing to pattern 'data.pipeline.*'...")
    await publisher.publish_pattern(
        "data.pipeline.*",
        PipelineStarted(topic="data.pipeline.*", pipeline_name="batch_pipeline"),
    )

    logging.info("\n5. Sending CompletedEvent to all topics...")
    for topic in topics:
        await publisher.publish(topic, CompletedEvent(topic=topic))

    await data_all_consumer_task

    logging.info("\n" + "=" * 80)
    logging.info("DEMO COMPLETED")
    logging.info("=" * 80)


def configure_logging(level: int) -> None:
    """Configures the root logger."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )


def main_cli() -> None:
    """
    Command-line interface for running event system demos.
    """
    import sys

    # Default to standard demo
    use_pattern_demo = "--pattern" in sys.argv

    if use_pattern_demo:
        configure_logging(logging.INFO)
        logging.info("Running wildcard pattern matching demo...")
    else:
        configure_logging(logging.INFO)
        logging.info("Running standard event system simulation...")
        logging.info("(Use --pattern flag for wildcard demo)")

    # The Composition Root: create and inject dependencies here
    event_system = setup_event_system()

    try:
        if use_pattern_demo:
            asyncio.run(
                main_with_pattern(
                    broker=event_system.broker,
                    publisher=event_system.publisher,
                    consumer=event_system.consumer,
                )
            )
        else:
            asyncio.run(
                main(publisher=event_system.publisher, consumer=event_system.consumer)
            )
    except ImportError as e:
        logging.error(f"[Execution Error] {e}")
        logging.error("Please run this script from the project's root directory using:")
        logging.error("python -m app.main")

    logging.info("Simulation finished.")


if __name__ == "__main__":
    main_cli()