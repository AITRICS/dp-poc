import asyncio
import logging
from dataclasses import dataclass

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer

# 1. Define concrete events based on EventBase
@dataclass(frozen=True)
class PipelineStarted(EventBase):
    pipeline_name: str

@dataclass(frozen=True)
class DataIngestionComplete(EventBase):
    source_name: str
    rows_ingested: int

async def main():
    """
    Demonstrates the highly decoupled event system where ports and adapters
    are in separate files.
    """
    # 2. Instantiate the infrastructure-specific implementation (Adapters)
    broker = InMemoryBroker()
    publisher = InMemoryPublisher(broker)
    consumer = InMemoryConsumer(broker)

    topic = "data_pipeline_events"

    # 3. Start the consumer in the background
    logging.info("Starting consumer task...")
    consumer_task = asyncio.create_task(consumer.consume(topic))

    # Give the consumer a moment to start up
    await asyncio.sleep(0.1)

    # 4. Publish concrete event objects
    await publisher.publish(topic, PipelineStarted(pipeline_name="daily_sales_report"))
    await publisher.publish(topic, DataIngestionComplete(source_name="pos_terminal_1", rows_ingested=1500))

    # 5. Wait for the consumer to process all events
    queue = await broker.get_queue(topic)
    await queue.join()

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