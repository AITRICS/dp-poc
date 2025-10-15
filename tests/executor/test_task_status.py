"""Tests for TaskStatus enum."""

from app.executor.domain.task_status import TaskStatus


class TestTaskStatus:
    """Test cases for TaskStatus enum."""

    def test_all_statuses_exist(self) -> None:
        """Test that all expected statuses are defined."""
        assert TaskStatus.PENDING
        assert TaskStatus.RUNNING
        assert TaskStatus.SUCCESS
        assert TaskStatus.FAILED
        assert TaskStatus.TIMEOUT
        assert TaskStatus.CANCELLED

    def test_is_terminal_for_success(self) -> None:
        """Test that SUCCESS is a terminal status."""
        assert TaskStatus.SUCCESS.is_terminal()

    def test_is_terminal_for_failed(self) -> None:
        """Test that FAILED is a terminal status."""
        assert TaskStatus.FAILED.is_terminal()

    def test_is_terminal_for_timeout(self) -> None:
        """Test that TIMEOUT is a terminal status."""
        assert TaskStatus.TIMEOUT.is_terminal()

    def test_is_terminal_for_cancelled(self) -> None:
        """Test that CANCELLED is a terminal status."""
        assert TaskStatus.CANCELLED.is_terminal()

    def test_is_not_terminal_for_pending(self) -> None:
        """Test that PENDING is not a terminal status."""
        assert not TaskStatus.PENDING.is_terminal()

    def test_is_not_terminal_for_running(self) -> None:
        """Test that RUNNING is not a terminal status."""
        assert not TaskStatus.RUNNING.is_terminal()

    def test_is_successful_only_for_success(self) -> None:
        """Test that only SUCCESS is considered successful."""
        assert TaskStatus.SUCCESS.is_successful()
        assert not TaskStatus.FAILED.is_successful()
        assert not TaskStatus.TIMEOUT.is_successful()
        assert not TaskStatus.CANCELLED.is_successful()
        assert not TaskStatus.PENDING.is_successful()
        assert not TaskStatus.RUNNING.is_successful()

    def test_status_names(self) -> None:
        """Test that status names are correct."""
        assert TaskStatus.PENDING.name == "PENDING"
        assert TaskStatus.RUNNING.name == "RUNNING"
        assert TaskStatus.SUCCESS.name == "SUCCESS"
        assert TaskStatus.FAILED.name == "FAILED"
        assert TaskStatus.TIMEOUT.name == "TIMEOUT"
        assert TaskStatus.CANCELLED.name == "CANCELLED"
