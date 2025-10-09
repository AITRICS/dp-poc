import asyncio
import logging
from dataclasses import dataclass

from app.event_system.domain.events import CompletedEvent, EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher


# 1. Define concrete events based on EventBase
class PipelineStarted(EventBase):
    pipeline_name: str


class DataIngestionComplete(EventBase):
    source_name: str
    rows_ingested: int


async def consume_events(consumer: InMemoryConsumer[EventBase], topic: str) -> None:
    """
    이벤트를 소비하는 비동기 함수
    """
    print("Starting event consumption...")
    async for event in consumer.consume(topic):
        print(f"Processing event: {event.__class__.__name__}")
        # 여기서 각 이벤트에 대한 구체적인 처리 로직을 구현할 수 있습니다
        if isinstance(event, PipelineStarted):
            print(f"  Pipeline '{event.pipeline_name}' has started!")
        elif isinstance(event, DataIngestionComplete):
            print(f"  Ingested {event.rows_ingested} rows from {event.source_name}")

    print("Event consumption completed.")


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

    # 3. Start the consumer in the background
    logging.info("Starting consumer task...")
    consumer_task = asyncio.create_task(consumer.consume(topic))

    # Give the consumer a moment to start up
    await asyncio.sleep(0.1)

    # 4. Publish concrete event objects
    await publisher.publish(topic, PipelineStarted(pipeline_name="daily_sales_report", topic=topic))
    await publisher.publish(
        topic,
        DataIngestionComplete(source_name="pos_terminal_1", rows_ingested=1500, topic=topic),
    )

    # 5. Send CompletedEvent to signal the end of the stream
    await publisher.publish(topic, CompletedEvent(topic=topic))

    # 6. Wait for the consumer to finish processing
    await consumer_task

    # 6. Gracefully shut down the consumer
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logging.info("Consumer task has been successfully cancelled.")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    logging.info("Running final decoupled event system simulation...")
    try:
        asyncio.run(main())
    except ImportError as e:
        logging.error(f"[Execution Error] {e}")
        logging.error("Please run this script from the project's root directory using the command:")
        logging.error("python -m app.main")

    logging.info("Simulation finished.")
