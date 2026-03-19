"""Tests for StorageRegistry.get() file path resolution.

Verifies fix for: LocalStore being rooted at a file path instead of its parent directory,
and .resolve() causing symlink mismatches on macOS (/var vs /private/var).
"""

import tempfile
from pathlib import Path

from sqlspec import SQLSpec


class TestStorageRegistryFilePathResolution:
    """Test that StorageRegistry.get() correctly roots LocalStore at the parent directory."""

    def test_load_single_file_by_str_path(self) -> None:
        """Loading a single SQL file by its string path should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "hello.sql"
            sql_file.write_text("-- name: hello_world\nSELECT 1;\n")

            s = SQLSpec()
            s.load_sql_files(str(sql_file))

            result = s.get_sql("hello_world")
            assert result is not None
            assert "SELECT 1" in str(result)

    def test_load_single_file_by_path_object(self) -> None:
        """Loading a single SQL file by Path object should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "hello.sql"
            sql_file.write_text("-- name: hello_world\nSELECT 1;\n")

            s = SQLSpec()
            s.load_sql_files(sql_file)

            result = s.get_sql("hello_world")
            assert result is not None
            assert "SELECT 1" in str(result)

    def test_load_from_directory(self) -> None:
        """Loading SQL files from a directory should work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "hello.sql"
            sql_file.write_text("-- name: hello_world\nSELECT 1;\n")

            s = SQLSpec()
            s.load_sql_files(tmpdir)

            result = s.get_sql("hello_world")
            assert result is not None
            assert "SELECT 1" in str(result)

    def test_load_multiple_files_from_directory(self) -> None:
        """Loading multiple SQL files from a directory should find all queries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "a.sql").write_text("-- name: query_a\nSELECT 'a';\n")
            Path(tmpdir, "b.sql").write_text("-- name: query_b\nSELECT 'b';\n")

            s = SQLSpec()
            s.load_sql_files(tmpdir)

            assert s.get_sql("query_a") is not None
            assert s.get_sql("query_b") is not None

    def test_file_and_directory_share_same_backend(self) -> None:
        """A file path and its parent directory should resolve to the same backend store."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_file = Path(tmpdir) / "hello.sql"
            sql_file.write_text("-- name: hello_world\nSELECT 1;\n")

            s = SQLSpec()
            loader = s._ensure_sql_loader()

            backend_from_file = loader.storage_registry.get(str(sql_file))
            backend_from_dir = loader.storage_registry.get(tmpdir)

            assert backend_from_file is backend_from_dir
