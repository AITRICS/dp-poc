"""Test fixtures for executor tests."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.executor.domain.execution_state import ExecutionState
from app.executor.domain.execution_state_repository_port import (
    ExecutionStateRepositoryPort,
)

if TYPE_CHECKING:
    pass


class InMemoryStateRepository(ExecutionStateRepositoryPort):
    """In-memory implementation of ExecutionStateRepository for testing."""

    def __init__(self) -> None:
        """Initialize in-memory repository."""
        self.states: dict[str, ExecutionState] = {}

    async def save_state(self, run_id: str, state: ExecutionState) -> None:
        """Save execution state to memory.

        Args:
            run_id: Unique identifier for the execution
            state: Execution state to persist
        """
        self.states[run_id] = state

    async def get_state(self, run_id: str) -> ExecutionState | None:
        """Retrieve execution state from memory.

        Args:
            run_id: Unique identifier for the execution

        Returns:
            Execution state if found, None otherwise
        """
        return self.states.get(run_id)

    async def delete_state(self, run_id: str) -> None:
        """Delete execution state from memory.

        Args:
            run_id: Unique identifier for the execution
        """
        self.states.pop(run_id, None)

    async def list_active_runs(self) -> list[str]:
        """List all active run IDs.

        Returns:
            List of run IDs that have active execution state
        """
        return list(self.states.keys())


class TestHelper:
    """Helper class for running event-driven tests."""

    @staticmethod
    async def run_until_complete(
        orchestrator_task: asyncio.Task,
        completion_event: asyncio.Event,
        timeout: float = 10.0,
    ) -> None:
        """Run orchestrator until completion event is set.

        Args:
            orchestrator_task: The orchestrator task
            completion_event: Event that signals completion
            timeout: Maximum time to wait in seconds

        Raises:
            asyncio.TimeoutError: If timeout is exceeded
        """
        try:
            await asyncio.wait_for(completion_event.wait(), timeout=timeout)
        finally:
            # Cancel orchestrator task
            orchestrator_task.cancel()
            try:
                await orchestrator_task
            except asyncio.CancelledError:
                pass

