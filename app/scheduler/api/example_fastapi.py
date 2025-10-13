"""
Complete FastAPI example with APITrigger integration.
Demonstrates how to use APITrigger with a real web API.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from app.event_system.domain.events import EventBase
from app.event_system.infrastructure.in_memory_broker import InMemoryBroker
from app.event_system.infrastructure.in_memory_consumer import InMemoryConsumer
from app.event_system.infrastructure.in_memory_publisher import InMemoryPublisher
from app.scheduler import Scheduler, create_manual_trigger
from app.scheduler.api.fastapi_integration import create_trigger_router


# Define event
class WebhookEvent(EventBase):
    """Event triggered by webhook."""

    webhook_id: str
    user_id: int
    action: str

    def __init__(
        self,
        topic: str = "",
        webhook_id: str = "",
        user_id: int = 0,
        action: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(topic=topic, **kwargs)
        self.webhook_id = webhook_id
        self.user_id = user_id
        self.action = action


# Global state (in production, use dependency injection)
broker: InMemoryBroker[WebhookEvent] | None = None
publisher: InMemoryPublisher[WebhookEvent] | None = None
consumer: InMemoryConsumer[WebhookEvent] | None = None
scheduler: Scheduler[WebhookEvent] | None = None
api_trigger = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for FastAPI app."""
    global broker, publisher, consumer, scheduler, api_trigger

    # Startup
    print("🚀 Starting scheduler system...")

    broker = InMemoryBroker()
    publisher = InMemoryPublisher(broker)
    consumer = InMemoryConsumer(broker)
    scheduler = Scheduler(publisher)

    # Declare topic
    await broker.declare_topic("webhooks.incoming")

    # Create API trigger
    api_trigger = create_manual_trigger(
        event_factory=lambda payload: WebhookEvent(
            topic="webhooks.incoming",
            webhook_id=payload.get("webhook_id", "unknown"),
            user_id=payload.get("user_id", 0),
            action=payload.get("action", "unknown"),
        )
    )

    # Register and start
    await scheduler.register_trigger("webhook_handler", api_trigger, "webhooks.incoming")
    await scheduler.start()

    # Start consuming events in background
    asyncio.create_task(consume_events())

    print("✅ Scheduler system started")

    yield

    # Shutdown
    print("🛑 Stopping scheduler system...")
    await scheduler.stop()
    print("✅ Scheduler system stopped")


async def consume_events() -> None:
    """Background task to consume and process events."""
    global consumer
    if not consumer:
        return

    async for event in consumer.consume("webhooks.incoming"):
        if isinstance(event, WebhookEvent):
            print(
                f"📨 Processed webhook: {event.webhook_id} - "
                f"User {event.user_id} performed '{event.action}'"
            )


# Create FastAPI app
app = FastAPI(
    title="Scheduler API Example",
    description="Example API demonstrating APITrigger integration with FastAPI",
    version="1.0.0",
    lifespan=lifespan,
)


# Add trigger routes
@app.on_event("startup")
async def add_routes() -> None:
    """Add trigger routes after startup."""
    if api_trigger:
        router = create_trigger_router(
            api_trigger,
            prefix="/api/webhooks",
            tags=["Webhooks"],
        )
        app.include_router(router)


# Additional custom endpoints
@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {
        "message": "Scheduler API is running",
        "docs": "/docs",
        "webhook_trigger": "/api/webhooks/trigger",
        "webhook_status": "/api/webhooks/status",
    }


@app.get("/health")
async def health_check() -> dict[str, Any]:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "scheduler_running": (scheduler.get_registered_triggers() if scheduler else False),
    }


# Manual trigger example (alternative to using the auto-generated route)
@app.post("/webhooks/github")
async def github_webhook(payload: dict[str, Any]) -> dict[str, Any] | tuple[dict[str, str], int]:
    """
    Example: GitHub webhook endpoint.
    Demonstrates how to manually trigger from a custom endpoint.
    """
    if not api_trigger:
        return {"error": "Trigger not initialized"}

    try:
        # Extract relevant data from GitHub payload
        webhook_data = {
            "webhook_id": f"gh_{payload.get('hook_id', 'unknown')}",
            "user_id": payload.get("sender", {}).get("id", 0),
            "action": payload.get("action", "unknown"),
        }

        await api_trigger.trigger(webhook_data)

        return {
            "status": "processed",
            "webhook_id": webhook_data["webhook_id"],
            "trigger_count": api_trigger.get_trigger_count(),
        }
    except Exception as e:
        return {"error": str(e)}, 500


if __name__ == "__main__":
    import uvicorn

    print("\n" + "=" * 80)
    print("FASTAPI + APITRIGGER EXAMPLE")
    print("=" * 80)
    print("\nStarting server...")
    print("📖 API Docs: http://localhost:8000/docs")
    print("🔗 Trigger endpoint: http://localhost:8000/api/webhooks/trigger")
    print("\nExample curl command:")
    print(
        """
curl -X POST http://localhost:8000/api/webhooks/trigger \\
  -H "Content-Type: application/json" \\
  -d '{"data": {"webhook_id": "wh_001", "user_id": 123, "action": "login"}}'
    """
    )
    print("=" * 80 + "\n")

    uvicorn.run(app, host="0.0.0.0", port=8000)
