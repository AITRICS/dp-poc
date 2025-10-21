"""Port for persisting execution state to database."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.executor.domain.execution_state import ExecutionState


class ExecutionStateRepositoryPort(ABC):
    """Abstract port for storing and retrieving execution state.

    This interface defines how execution state can be persisted to and
    retrieved from a database or storage system. Implementations can use
    various backends (SQL, NoSQL, in-memory, etc.).
    """

    @abstractmethod
    async def save_state(self, run_id: str, state: ExecutionState) -> None:
        """Save execution state to storage.

        Args:
            run_id: Unique identifier for the execution
            state: Execution state to persist
        """
        raise NotImplementedError

    @abstractmethod
    async def get_state(self, run_id: str) -> ExecutionState | None:
        """Retrieve execution state from storage.

        Args:
            run_id: Unique identifier for the execution

        Returns:
            Execution state if found, None otherwise
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_state(self, run_id: str) -> None:
        """Delete execution state from storage.

        Args:
            run_id: Unique identifier for the execution
        """
        raise NotImplementedError

    @abstractmethod
    async def list_active_runs(self) -> list[str]:
        """List all active run IDs.

        Returns:
            List of run IDs that have active execution state
        """
        raise NotImplementedError

