"""Tests for unified storage mixins."""

from typing import Any
from unittest.mock import MagicMock, Mock

from sqlspec.driver.mixins._unified_storage import SyncStorageMixin


class MockDriver(SyncStorageMixin):
    """Mock driver for testing unified storage mixin."""

    def __init__(self) -> None:
        self._connection = MagicMock()
        self.config = MagicMock()

    def execute(self, sql: Any) -> Any:
        """Mock execute method."""
        result = MagicMock()
        result.data = [["test", "data"], ["more", "data"]]
        result.columns = ["col1", "col2"]
        result.rowcount = 2
        return result


class TestSyncStorageMixin:
    """Test the unified sync storage mixin."""

    def test_mixin_instantiation(self) -> None:
        """Test that the mixin can be instantiated."""
        driver = MockDriver()
        assert isinstance(driver, SyncStorageMixin)

    def test_has_native_capability_detection(self) -> None:
        """Test native capability detection logic."""
        driver = MockDriver()
        driver.__class__.__name__ = "DuckDBDriver"

        # Native capabilities now require explicit implementation
        assert not driver._has_native_capability("parquet", "s3://bucket/file.parquet", "parquet")
        assert not driver._has_native_capability("export", "https://example.com/data.csv", "csv")

        # Non-DuckDB driver should not have native capabilities either
        driver.__class__.__name__ = "SqliteDriver"
        assert not driver._has_native_capability("parquet", "s3://bucket/file.parquet", "parquet")

    def test_uri_detection(self) -> None:
        """Test URI vs relative path detection."""
        driver = MockDriver()

        # These should be detected as URIs
        assert driver._is_uri("s3://bucket/file.parquet")
        assert driver._is_uri("gs://bucket/data.csv")
        assert driver._is_uri("file:///tmp/data.json")
        assert driver._is_uri("http://example.com/data.csv")
        assert driver._is_uri("/absolute/path/file.txt")
        assert driver._is_uri("C:\\windows\\path\\file.txt")

        # These should be detected as relative paths
        assert not driver._is_uri("relative/path/file.csv")
        assert not driver._is_uri("just_a_filename.parquet")
        assert not driver._is_uri("../parent/file.json")

    def test_format_detection(self) -> None:
        """Test file format detection from URIs."""
        driver = MockDriver()

        assert driver._detect_format("s3://bucket/data.parquet") == "parquet"
        assert driver._detect_format("gs://bucket/file.csv") == "csv"
        assert driver._detect_format("file:///tmp/data.json") == "json"
        assert driver._detect_format("http://example.com/data.jsonl") == "jsonl"
        assert driver._detect_format("data.pq") == "parquet"
        assert driver._detect_format("unknown.xyz") == "csv"  # Default

    def test_pyarrow_requirement_check(self) -> None:
        """Test that PyArrow requirement is checked properly."""
        driver = MockDriver()

        # This should not raise if pyarrow is installed
        try:
            driver._ensure_pyarrow_installed()
        except Exception as e:
            # Only acceptable if pyarrow is truly not installed
            assert "pyarrow" in str(e).lower()

    def test_rows_to_arrow_table_conversion(self) -> None:
        """Test conversion of rows to Arrow table."""
        driver = MockDriver()

        # Test with dict rows
        dict_rows = [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 2}]
        columns = ["col1", "col2"]

        try:
            table = driver._rows_to_arrow_table(dict_rows, columns)
            assert table.num_rows == 2
            assert table.num_columns == 2
        except Exception as e:
            # Only acceptable if pyarrow is not installed
            assert "pyarrow" in str(e).lower()

    def test_export_to_storage_format_detection(self) -> None:
        """Test that export_to_storage detects format correctly."""
        driver = MockDriver()

        # Mock the native capability and export method
        driver._has_native_capability = Mock(return_value=True)
        driver._export_native = Mock(return_value=10)

        # Should auto-detect parquet format
        result = driver.export_to_storage("SELECT * FROM table", "s3://bucket/data.parquet")

        # Verify format was detected and native export was called
        driver._export_native.assert_called_once()
        call_args = driver._export_native.call_args
        assert call_args[0][2] == "parquet"  # format argument
        assert result == 10

    def test_import_from_storage_format_detection(self) -> None:
        """Test that import_from_storage detects format correctly."""
        driver = MockDriver()

        # Mock the native capability and import method
        driver._has_native_capability = Mock(return_value=True)
        driver._import_native = Mock(return_value=5)

        # Should auto-detect CSV format
        result = driver.import_from_storage("file:///tmp/data.csv", "target_table")

        # Verify format was detected and native import was called
        driver._import_native.assert_called_once()
        call_args = driver._import_native.call_args
        assert call_args[0][2] == "csv"  # format argument
        assert result == 5


class TestStorageMixinBase:
    """Test the base storage mixin functionality."""

    def test_format_detection_comprehensive(self) -> None:
        """Test comprehensive format detection."""
        driver = MockDriver()

        test_cases = [
            ("data.csv", "csv"),
            ("data.tsv", "csv"),
            ("data.txt", "csv"),
            ("data.parquet", "parquet"),
            ("data.pq", "parquet"),
            ("data.json", "json"),
            ("data.jsonl", "jsonl"),
            ("data.ndjson", "jsonl"),
            ("data.unknown", "csv"),  # Default fallback
        ]

        for filename, expected_format in test_cases:
            assert driver._detect_format(filename) == expected_format

    def test_uri_scheme_detection(self) -> None:
        """Test URI scheme detection for different cloud providers."""
        driver = MockDriver()

        cloud_uris = [
            "s3://bucket/path/file.parquet",
            "gs://bucket/path/file.csv",
            "gcs://bucket/path/file.json",
            "az://container/path/file.txt",
            "azure://container/path/file.parquet",
            "abfs://container/path/file.csv",
            "abfss://container/path/file.json",
            "file:///local/path/file.txt",
            "http://example.com/file.csv",
            "https://secure.example.com/data.parquet",
        ]

        for uri in cloud_uris:
            assert driver._is_uri(uri), f"Should detect {uri} as URI"

        relative_paths = [
            "relative/path/file.csv",
            "just_filename.txt",
            "../parent/file.json",
            "./current/file.parquet",
        ]

        for path in relative_paths:
            assert not driver._is_uri(path), f"Should detect {path} as relative path"
