"""
FastAPI integration for ManualTrigger.
Provides routes and dependency injection for triggering events via HTTP API.
"""

from collections.abc import Sequence
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.scheduler.infrastructure.manual_trigger import ManualTrigger


class TriggerPayload(BaseModel):
    """Request payload for triggering events."""

    data: dict[str, Any] = {}


class TriggerResponse(BaseModel):
    """Response after triggering an event."""

    success: bool
    message: str
    trigger_count: int


class TriggerRouter:
    """
    FastAPI router for ManualTrigger integration.

    Example usage:
        ```python
        from fastapi import FastAPI
        from app.scheduler.api import TriggerRouter

        app = FastAPI()

        # Create and register trigger
        manual_trigger = create_manual_trigger(event_factory)
        await scheduler.register_trigger("webhook", manual_trigger, "webhooks.incoming")
        await scheduler.start()

        # Add API routes
        trigger_router = TriggerRouter(manual_trigger, prefix="/webhooks")
        app.include_router(trigger_router.router)

        # Now you can POST to /webhooks/trigger
        ```
    """

    def __init__(
        self,
        trigger: ManualTrigger[Any],
        prefix: str = "",
        tags: Sequence[str] | None = None,
    ):
        """
        Initialize the router.

        Args:
            trigger: The ManualTrigger instance to integrate.
            prefix: URL prefix for the routes (e.g., "/webhooks").
            tags: OpenAPI tags for documentation.
        """
        if tags is None:
            tags = ["Triggers"]

        self.trigger = trigger
        self.router = APIRouter(prefix=prefix, tags=list(tags))
        self._setup_routes()

    def _setup_routes(self) -> None:
        """Setup the API routes."""

        @self.router.post(
            "/trigger",
            response_model=TriggerResponse,
            status_code=status.HTTP_200_OK,
            summary="Trigger an event",
            description="Manually trigger an event with optional payload data.",
        )
        async def trigger_event(payload: TriggerPayload) -> TriggerResponse:
            """
            Trigger an event via API call.

            Args:
                payload: The payload data to pass to the event factory.

            Returns:
                Response indicating success and current trigger count.

            Raises:
                HTTPException: If trigger is not running or event creation fails.
            """
            try:
                await self.trigger.trigger(payload.data)
                return TriggerResponse(
                    success=True,
                    message="Event triggered successfully",
                    trigger_count=self.trigger.get_trigger_count(),
                )
            except RuntimeError as e:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=str(e),
                ) from e
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to trigger event: {str(e)}",
                ) from e

        @self.router.get(
            "/status",
            response_model=dict[str, Any],
            summary="Get trigger status",
            description="Get the current status of the trigger.",
        )
        async def get_trigger_status() -> dict[str, Any]:
            """
            Get trigger status.

            Returns:
                Status information including running state and trigger count.
            """
            return {
                "running": self.trigger.is_running(),
                "trigger_count": self.trigger.get_trigger_count(),
            }


def create_trigger_router(
    trigger: ManualTrigger[Any],
    prefix: str = "",
    tags: Sequence[str] | None = None,
) -> APIRouter:
    """
    Convenience function to create a FastAPI router for a ManualTrigger.

    Args:
        trigger: The ManualTrigger instance.
        prefix: URL prefix for the routes.
        tags: OpenAPI tags.

    Returns:
        Configured APIRouter instance.

    Example:
        ```python
        from fastapi import FastAPI

        app = FastAPI()

        # Create trigger
        manual_trigger = create_manual_trigger(event_factory)

        # Create and add router
        router = create_trigger_router(manual_trigger, prefix="/api/triggers")
        app.include_router(router)
        ```
    """
    return TriggerRouter(trigger, prefix, tags).router
