"""Tests for FilesystemIOManager."""

import tempfile
from pathlib import Path

import pytest

from app.io_manager.domain.io_manager_port import IOManagerPort
from app.io_manager.infrastructure.filesystem_io_manager import FilesystemIOManager


class TestFilesystemIOManagerBasic:
    """Test basic I/O Manager operations."""

    def test_init_default_path(self) -> None:
        """Test initialization with default path."""
        io_manager = FilesystemIOManager()
        assert io_manager.base_path == Path("./.dp-poc/runs").expanduser().resolve()

    def test_init_custom_path(self) -> None:
        """Test initialization with custom path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)
            assert io_manager.base_path == Path(tmpdir).resolve()

    def test_implements_port_interface(self) -> None:
        """Test that FilesystemIOManager implements IOManagerPort."""
        io_manager: IOManagerPort = FilesystemIOManager()
        assert isinstance(io_manager, IOManagerPort)


class TestFilesystemIOManagerSaveLoad:
    """Test save and load operations."""

    def test_save_and_load_simple_data(self) -> None:
        """Test saving and loading simple data types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save integer
            location = io_manager.save("task_a", "run_001", "result_1", 42)
            assert location.endswith("result_1.pkl")

            # Load integer
            result = io_manager.load("task_a", "run_001", "result_1")
            assert result == 42

    def test_save_and_load_complex_data(self) -> None:
        """Test saving and loading complex data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Complex data
            data = {
                "values": [1, 2, 3, 4, 5],
                "metadata": {"count": 5, "sum": 15},
                "nested": {"deep": {"value": "test"}},
            }

            # Save and load
            io_manager.save("task_b", "run_002", "result_2", data)
            result = io_manager.load("task_b", "run_002", "result_2")

            assert result == data
            assert result["nested"]["deep"]["value"] == "test"

    def test_save_creates_directories(self) -> None:
        """Test that save creates necessary directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save to non-existent path
            io_manager.save("new_task", "new_run", "result_1", "data")

            # Check directories were created
            expected_path = Path(tmpdir) / "new_run" / "new_task" / "result_1.pkl"
            assert expected_path.exists()
            assert expected_path.is_file()

    def test_save_overwrites_existing(self) -> None:
        """Test that save overwrites existing data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save initial data
            io_manager.save("task_c", "run_003", "result_1", "old_data")

            # Overwrite
            io_manager.save("task_c", "run_003", "result_1", "new_data")

            # Load should return new data
            result = io_manager.load("task_c", "run_003", "result_1")
            assert result == "new_data"


class TestFilesystemIOManagerExists:
    """Test exists operation."""

    def test_exists_returns_true_for_existing(self) -> None:
        """Test exists returns True for existing results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            io_manager.save("task_d", "run_004", "result_1", "data")

            assert io_manager.exists("task_d", "run_004", "result_1")

    def test_exists_returns_false_for_non_existing(self) -> None:
        """Test exists returns False for non-existing results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            assert not io_manager.exists("task_x", "run_999", "result_999")

    def test_load_non_existing_raises_error(self) -> None:
        """Test loading non-existing result raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            with pytest.raises(FileNotFoundError, match="Result not found"):
                io_manager.load("task_x", "run_999", "result_999")


class TestFilesystemIOManagerDelete:
    """Test delete operation."""

    def test_delete_existing_result(self) -> None:
        """Test deleting existing result."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save and verify
            io_manager.save("task_e", "run_005", "result_1", "data")
            assert io_manager.exists("task_e", "run_005", "result_1")

            # Delete
            result = io_manager.delete("task_e", "run_005", "result_1")
            assert result is True

            # Verify deleted
            assert not io_manager.exists("task_e", "run_005", "result_1")

    def test_delete_non_existing_returns_false(self) -> None:
        """Test deleting non-existing result returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            result = io_manager.delete("task_x", "run_999", "result_999")
            assert result is False


class TestFilesystemIOManagerListResults:
    """Test list_results operation."""

    def test_list_results_for_task(self) -> None:
        """Test listing results for a specific task."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save multiple results for same task
            io_manager.save("task_f", "run_006", "result_1", "data1")
            io_manager.save("task_f", "run_006", "result_2", "data2")
            io_manager.save("task_f", "run_006", "result_3", "data3")

            # List results
            results = io_manager.list_results("run_006", task_name="task_f")
            assert sorted(results) == ["result_1", "result_2", "result_3"]

    def test_list_results_for_run(self) -> None:
        """Test listing all results for a run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save results for multiple tasks
            io_manager.save("task_g", "run_007", "result_1", "data1")
            io_manager.save("task_g", "run_007", "result_2", "data2")
            io_manager.save("task_h", "run_007", "result_1", "data3")

            # List all results
            results = io_manager.list_results("run_007")
            assert sorted(results) == ["result_1", "result_1", "result_2"]

    def test_list_results_empty_for_non_existing(self) -> None:
        """Test listing results returns empty for non-existing run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            results = io_manager.list_results("run_999")
            assert results == []


class TestFilesystemIOManagerClearRun:
    """Test clear_run operation."""

    def test_clear_run_deletes_all_results(self) -> None:
        """Test clearing run deletes all results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save multiple results
            io_manager.save("task_i", "run_008", "result_1", "data1")
            io_manager.save("task_i", "run_008", "result_2", "data2")
            io_manager.save("task_j", "run_008", "result_1", "data3")

            # Clear run
            count = io_manager.clear_run("run_008")
            assert count == 3

            # Verify all deleted
            assert not io_manager.exists("task_i", "run_008", "result_1")
            assert not io_manager.exists("task_i", "run_008", "result_2")
            assert not io_manager.exists("task_j", "run_008", "result_1")

    def test_clear_run_returns_zero_for_non_existing(self) -> None:
        """Test clearing non-existing run returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            count = io_manager.clear_run("run_999")
            assert count == 0


class TestFilesystemIOManagerMultipleRuns:
    """Test handling multiple runs."""

    def test_isolated_runs(self) -> None:
        """Test that different runs are isolated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save same task/result_id in different runs
            io_manager.save("task_k", "run_010", "result_1", "data_run_010")
            io_manager.save("task_k", "run_011", "result_1", "data_run_011")

            # Load from different runs
            data_010 = io_manager.load("task_k", "run_010", "result_1")
            data_011 = io_manager.load("task_k", "run_011", "result_1")

            assert data_010 == "data_run_010"
            assert data_011 == "data_run_011"

    def test_clear_run_does_not_affect_other_runs(self) -> None:
        """Test clearing one run doesn't affect others."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Save in two runs
            io_manager.save("task_l", "run_012", "result_1", "data_012")
            io_manager.save("task_l", "run_013", "result_1", "data_013")

            # Clear one run
            io_manager.clear_run("run_012")

            # Verify run_012 deleted but run_013 still exists
            assert not io_manager.exists("task_l", "run_012", "result_1")
            assert io_manager.exists("task_l", "run_013", "result_1")


class TestFilesystemIOManagerStreamOutput:
    """Test handling stream output (multiple results from same task)."""

    def test_multiple_results_from_same_task(self) -> None:
        """Test storing multiple results from same task (stream output)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            io_manager = FilesystemIOManager(base_path=tmpdir)

            # Simulate stream output: multiple results from same task
            io_manager.save("generator_task", "run_014", "result_0", 0)
            io_manager.save("generator_task", "run_014", "result_1", 1)
            io_manager.save("generator_task", "run_014", "result_2", 2)

            # List all results
            results = io_manager.list_results("run_014", task_name="generator_task")
            assert sorted(results) == ["result_0", "result_1", "result_2"]

            # Load each result
            assert io_manager.load("generator_task", "run_014", "result_0") == 0
            assert io_manager.load("generator_task", "run_014", "result_1") == 1
            assert io_manager.load("generator_task", "run_014", "result_2") == 2
