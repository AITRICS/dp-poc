import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher

logger = logging.getLogger(__name__)


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


async def consume_events(consumer: InMemoryConsumer[EventBase], topic: str) -> None:
    """
    이벤트를 소비하는 비동기 함수
    """
    logger.info("Starting event consumption...")
    async for event in consumer.consume(topic):
        logger.info("Processing event: %s", event.__class__.__name__)
        # 여기서 각 이벤트에 대한 구체적인 처리 로직을 구현할 수 있습니다
        if isinstance(event, PipelineStarted):
            logger.info("Pipeline '%s' has started!", event.pipeline_name)
        elif isinstance(event, DataIngestionComplete):
            logger.info("Ingested %s rows from %s", event.rows_ingested, event.source_name)

    logger.info("Event consumption completed.")


async def consume_pattern_events(consumer: InMemoryConsumer[EventBase], topic_pattern: str) -> None:
    """
    패턴으로 이벤트를 소비하는 비동기 함수
    """
    print(f"\nStarting pattern consumption for: {topic_pattern}")
    event_count = 0
    async for event in consumer.consume_pattern(topic_pattern):
        event_count += 1
        print(f"[Pattern {topic_pattern}] Event #{event_count}: {event.__class__.__name__}")
        if isinstance(event, PipelineStarted):
            print(f"  Pipeline '{event.pipeline_name}' has started!")
        elif isinstance(event, DataIngestionComplete):
            print(f"  Ingested {event.rows_ingested} rows from {event.source_name}")

    print(f"Pattern consumption completed. Total events: {event_count}")


async def main() -> None:
    """
    Demonstrates the highly decoupled event system where ports and adapters
    are in separate files, using async generator pattern for event consumption.
    """
    # 2. Instantiate the infrastructure-specific implementation (Adapters)
    broker: InMemoryBroker[EventBase] = InMemoryBroker()
    publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)
    consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

    topic = "data_pipeline_events"

    # 3. Start consuming events in the background
    consumer_task = asyncio.create_task(consume_events(consumer, topic))

    # Give the consumer a moment to start up
    await asyncio.sleep(0.1)

    # 4. Publish concrete event objects
    await publisher.publish(topic, PipelineStarted(topic=topic, pipeline_name="daily_sales_report"))
    await publisher.publish(
        topic,
        DataIngestionComplete(topic=topic, source_name="pos_terminal_1", rows_ingested=1500),
    )

    # 5. Send CompletedEvent to signal the end of the stream
    await publisher.publish(topic, CompletedEvent(topic=topic))

    # 6. Wait for the consumer to finish processing
    await consumer_task


async def main_with_pattern() -> None:
    """
    Demonstrates the wildcard pattern matching feature for topic subscriptions.
    Shows how to publish and consume events using topic patterns.
    """
    print("\n" + "=" * 80)
    print("WILDCARD PATTERN MATCHING DEMO")
    print("=" * 80)

    # Setup
    broker: InMemoryBroker[EventBase] = InMemoryBroker()
    publisher: InMemoryPublisher[EventBase] = InMemoryPublisher(broker)
    consumer: InMemoryConsumer[EventBase] = InMemoryConsumer(broker)

    # Create multiple topics with hierarchical structure
    topics = [
        "data.pipeline.ingestion",
        "data.pipeline.transformation",
        "data.warehouse.load",
        "analytics.report.daily",
    ]

    # Declare topics (idempotent operation - safe to call multiple times)
    print("\n1. Declaring topics...")
    for topic in topics:
        await broker.declare_topic(topic)
    print(f"   Declared {len(topics)} topics: {topics}")

    # Start another pattern consumer for all data.** topics
    print("2. Starting pattern consumer for 'data.**'")
    data_all_consumer_task = asyncio.create_task(consume_pattern_events(consumer, "data.**"))

    await asyncio.sleep(0.1)

    # Publish events to different topics
    print("\n3. Publishing events to various topics...")
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
    await publisher.publish(
        "data.warehouse.load",
        PipelineStarted(topic="data.warehouse.load", pipeline_name="warehouse_load"),
    )
    await publisher.publish(
        "analytics.report.daily",
        DataIngestionComplete(
            topic="analytics.report.daily",
            source_name="aggregated_data",
            rows_ingested=100,
        ),
    )

    # Test pattern publishing: publish to all data.pipeline.* topics at once
    print("\n4. Publishing to pattern 'data.pipeline.*'...")
    await publisher.publish_pattern(
        "data.pipeline.*",
        PipelineStarted(topic="data.pipeline.*", pipeline_name="batch_pipeline"),
    )

    # Send CompletedEvent to all topics
    print("\n5. Sending CompletedEvent to all topics...")
    for topic in topics:
        await publisher.publish(topic, CompletedEvent(topic=topic))

    # Wait for consumers to finish
    # await pattern_consumer_task
    await data_all_consumer_task

    print("\n" + "=" * 80)
    print("DEMO COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(
        level=logging.WARNING,  # Set to WARNING for benchmark to reduce noise
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--pattern":
            logging.basicConfig(level=logging.INFO, force=True)
            print("Running wildcard pattern matching demo...")
            try:
                asyncio.run(main_with_pattern())
            except ImportError as e:
                logging.error(f"[Execution Error] {e}")
                logging.error("Please run this script from the project's root directory using:")
                logging.error("python -m app.main --pattern")
    else:
        logging.basicConfig(level=logging.INFO, force=True)
        logging.info("Running standard event system simulation...")
        logging.info("(Use --pattern flag for wildcard demo, --benchmark for performance test)")
        try:
            asyncio.run(main())
        except ImportError as e:
            logging.error(f"[Execution Error] {e}")
            logging.error("Please run this script from the project's root directory using:")
            logging.error("python -m app.main")

        logging.info("Simulation finished.")
