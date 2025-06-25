"""Unit tests for driver mixins.

Tests the mixin classes that provide additional functionality for database drivers,
including SQL translation, unified storage operations, type coercion, and result utilities.
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlglot import exp, parse_one

from sqlspec.driver.mixins import (
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.exceptions import MissingDependencyError, SQLConversionError
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


# Test SQLTranslatorMixin
class MockDriverWithTranslator(SQLTranslatorMixin):
    """Mock driver class with SQL translator mixin."""

    def __init__(self, dialect: str = "sqlite") -> None:
        self.dialect = dialect


@pytest.mark.parametrize(
    "input_sql,from_dialect,to_dialect,expected_contains",
    [
        # Basic translation
        ("SELECT * FROM users", "sqlite", "postgres", "SELECT"),
        # Different quote styles
        ('SELECT "name" FROM users', "sqlite", "mysql", "SELECT"),
        # Function differences
        ("SELECT SUBSTR(name, 1, 5) FROM users", "sqlite", "postgres", "SUBSTRING"),
        # Keep same dialect
        ("SELECT * FROM users", "sqlite", "sqlite", "SELECT * FROM users"),
    ],
    ids=["basic", "quotes", "functions", "same_dialect"],
)
def test_sql_translator_convert_to_dialect(
    input_sql: str, from_dialect: str, to_dialect: str, expected_contains: str
) -> None:
    """Test SQL dialect conversion."""
    driver = MockDriverWithTranslator(from_dialect)

    # Create SQL object
    statement = SQL(input_sql, dialect=from_dialect)

    result = driver.convert_to_dialect(statement, to_dialect=to_dialect, pretty=False)
    assert expected_contains in result


def test_sql_translator_with_expression_object() -> None:
    """Test conversion with sqlglot Expression object."""
    driver = MockDriverWithTranslator("sqlite")
    expression = parse_one("SELECT * FROM users", dialect="sqlite")

    result = driver.convert_to_dialect(expression, to_dialect="postgres")
    assert "SELECT" in result
    assert "FROM users" in result


def test_sql_translator_with_string() -> None:
    """Test conversion with raw SQL string."""
    driver = MockDriverWithTranslator("sqlite")

    result = driver.convert_to_dialect("SELECT * FROM users", to_dialect="mysql")
    assert "SELECT" in result


@pytest.mark.parametrize(
    "statement,error_match",
    [
        ("INVALID SQL SYNTAX !!!", "Failed to parse SQL statement"),
        (123, "Failed to parse SQL statement"),  # Invalid type
    ],
)
def test_sql_translator_error_handling(statement: Any, error_match: str) -> None:
    """Test error handling in SQL translation."""
    driver = MockDriverWithTranslator("sqlite")

    with pytest.raises(SQLConversionError, match=error_match):
        driver.convert_to_dialect(statement, to_dialect="postgres")


def test_sql_translator_conversion_failure() -> None:
    """Test handling of conversion failures."""
    driver = MockDriverWithTranslator("sqlite")
    expression = Mock(spec=exp.Expression)
    expression.sql.side_effect = Exception("Conversion error")

    with pytest.raises(SQLConversionError, match="Failed to convert SQL expression"):
        driver.convert_to_dialect(expression, to_dialect="postgres")


# Test Storage Mixins
class MockDriverWithStorage(SyncStorageMixin):
    """Mock driver class with sync storage mixin."""

    def __init__(self) -> None:
        self.config = MagicMock()
        self._connection = MagicMock()


class MockAsyncDriverWithStorage(AsyncStorageMixin):
    """Mock async driver class with async storage mixin."""

    def __init__(self) -> None:
        self.config = MagicMock()
        self._connection = MagicMock()


@pytest.mark.parametrize(
    "path,expected",
    [
        ("s3://bucket/key", True),
        ("gs://bucket/object", True),
        ("gcs://bucket/object", True),
        ("az://container/blob", True),
        ("azure://container/blob", True),
        ("abfs://container/path", True),
        ("abfss://container/path", True),
        ("file:///absolute/path", True),
        ("http://example.com/file", True),
        ("https://example.com/file", True),
        ("/absolute/path", True),
        ("C:\\Windows\\Path", True),
        ("D:\\Data\\file.csv", True),
        ("relative/path", False),
        ("just_a_file.txt", False),
        ("./relative/path", False),
        ("../parent/path", False),
    ],
    ids=[
        "s3_uri",
        "gs_uri",
        "gcs_uri",
        "az_uri",
        "azure_uri",
        "abfs_uri",
        "abfss_uri",
        "file_uri",
        "http_uri",
        "https_uri",
        "absolute_path",
        "windows_path_c",
        "windows_path_d",
        "relative_path",
        "simple_file",
        "dot_relative",
        "parent_relative",
    ],
)
def test_storage_is_uri_detection(path: str, expected: bool) -> None:
    """Test URI detection logic."""
    driver = MockDriverWithStorage()
    assert driver._is_uri(path) == expected


@pytest.mark.parametrize(
    "uri,expected_format",
    [
        ("s3://bucket/data.csv", "csv"),
        ("s3://bucket/data.tsv", "csv"),
        ("s3://bucket/data.txt", "csv"),
        ("s3://bucket/data.parquet", "parquet"),
        ("s3://bucket/data.pq", "parquet"),
        ("s3://bucket/data.json", "json"),
        ("s3://bucket/data.jsonl", "jsonl"),
        ("s3://bucket/data.ndjson", "jsonl"),
        ("s3://bucket/data.unknown", "csv"),  # Default
        ("s3://bucket/data", "csv"),  # No extension
        ("file:///path/to/DATA.CSV", "csv"),  # Case insensitive
    ],
    ids=["csv", "tsv", "txt", "parquet", "pq", "json", "jsonl", "ndjson", "unknown_ext", "no_ext", "uppercase_ext"],
)
def test_storage_format_detection(uri: str, expected_format: str) -> None:
    """Test file format detection from URI."""
    driver = MockDriverWithStorage()
    assert driver._detect_format(uri) == expected_format


def test_storage_ensure_pyarrow_installed_success() -> None:
    """Test pyarrow installation check when installed."""
    driver = MockDriverWithStorage()

    with patch("sqlspec.typing.PYARROW_INSTALLED", True):
        # Should not raise
        driver._ensure_pyarrow_installed()


def test_storage_ensure_pyarrow_installed_missing() -> None:
    """Test pyarrow installation check when missing."""
    driver = MockDriverWithStorage()

    with patch("sqlspec.typing.PYARROW_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="pyarrow is required"):
            driver._ensure_pyarrow_installed()


def test_storage_get_backend() -> None:
    """Test storage backend retrieval."""
    driver = MockDriverWithStorage()
    mock_backend = Mock()

    with patch("sqlspec.driver.mixins._storage.storage_registry.get", return_value=mock_backend) as mock_get:
        backend = driver._get_storage_backend("s3://bucket/key")
        assert backend == mock_backend
        mock_get.assert_called_once_with("s3://bucket/key")


# Test TypeCoercionMixin
class MockDriverWithTypeCoercion(TypeCoercionMixin):
    """Mock driver class with type coercion mixin."""

    def __init__(self) -> None:
        self.config = MagicMock()


def test_type_coercion_mixin_import() -> None:
    """Test that TypeCoercionMixin can be imported and instantiated."""
    driver = MockDriverWithTypeCoercion()
    assert hasattr(driver, "config")


# Test ToSchemaMixin
class MockDriverWithToSchema(ToSchemaMixin):
    """Mock driver class with to-schema mixin."""

    def __init__(self) -> None:
        self.config = MagicMock()


def test_to_schema_mixin_import() -> None:
    """Test that ToSchemaMixin can be imported and instantiated."""
    driver = MockDriverWithToSchema()
    assert hasattr(driver, "config")


# Test multiple mixin inheritance
class MockDriverWithMultipleMixins(SQLTranslatorMixin, SyncStorageMixin, TypeCoercionMixin, ToSchemaMixin):
    """Mock driver with multiple mixins."""

    def __init__(self) -> None:
        self.dialect = "sqlite"
        self.config = MagicMock()
        self._connection = MagicMock()


def test_multiple_mixin_inheritance() -> None:
    """Test that a driver can inherit from multiple mixins without conflicts."""
    driver = MockDriverWithMultipleMixins()

    # From SQLTranslatorMixin
    assert hasattr(driver, "convert_to_dialect")
    assert callable(driver.convert_to_dialect)

    # From SyncStorageMixin
    assert hasattr(driver, "_is_uri")
    assert hasattr(driver, "_detect_format")
    assert hasattr(driver, "_get_storage_backend")
    assert hasattr(driver, "_ensure_pyarrow_installed")

    # From TypeCoercionMixin
    assert hasattr(driver, "config")

    # From ToSchemaMixin
    assert hasattr(driver, "config")

    # Test that methods work
    assert driver._is_uri("s3://bucket/key") is True
    assert driver._detect_format("file.parquet") == "parquet"


@pytest.mark.parametrize("mixin_class", [SyncStorageMixin, AsyncStorageMixin])
def test_storage_mixin_slots(mixin_class: type) -> None:
    """Test that storage mixins define __slots__ to avoid dict creation."""
    assert hasattr(mixin_class, "__slots__")
    assert mixin_class.__slots__ == ()


def test_async_storage_mixin_has_async_methods() -> None:
    """Test that AsyncStorageMixin has async method signatures."""
    driver = MockAsyncDriverWithStorage()

    # Check that the mixin provides storage functionality
    assert hasattr(driver, "_is_uri")
    assert hasattr(driver, "_detect_format")
    assert hasattr(driver, "_get_storage_backend")


# Test method resolution order
def test_mixin_mro() -> None:
    """Test method resolution order with multiple mixins."""
    mro = MockDriverWithMultipleMixins.__mro__

    # Verify MRO includes all mixins
    assert SQLTranslatorMixin in mro
    assert SyncStorageMixin in mro
    assert TypeCoercionMixin in mro
    assert ToSchemaMixin in mro

    # Verify mixins come before object
    mixin_indices = [mro.index(m) for m in [SQLTranslatorMixin, SyncStorageMixin, TypeCoercionMixin, ToSchemaMixin]]
    object_index = mro.index(object)
    assert all(idx < object_index for idx in mixin_indices)


# Test that mixins don't interfere with each other
def test_mixin_isolation() -> None:
    """Test that mixins don't interfere with each other's functionality."""
    driver = MockDriverWithMultipleMixins()

    # Set up mocks
    mock_expression = Mock(spec=exp.Expression)
    mock_expression.sql.return_value = "SELECT * FROM users"

    # Test SQL translation doesn't affect storage
    result = driver.convert_to_dialect(mock_expression, to_dialect="postgres")
    assert "SELECT" in result

    # Storage methods still work
    assert driver._is_uri("s3://bucket/key") is True
    assert driver._detect_format("data.json") == "json"


# Edge cases
def test_storage_mixin_windows_path_edge_cases() -> None:
    """Test Windows path detection edge cases."""
    driver = MockDriverWithStorage()

    # Valid Windows paths
    assert driver._is_uri("C:\\") is True  # Minimum length (3 chars)
    assert driver._is_uri("C:\\a") is True  # Longer path
    assert driver._is_uri("Z:\\Path\\To\\File") is True

    # Invalid Windows paths
    assert driver._is_uri("1:\\path") is True  # Number drive (still detected as path)
    assert driver._is_uri(":\\path") is False  # Missing drive letter
    assert driver._is_uri("C:path") is False  # Missing backslash, doesn't match pattern


def test_storage_format_detection_edge_cases() -> None:
    """Test format detection edge cases."""
    driver = MockDriverWithStorage()

    # Multiple dots
    assert driver._detect_format("data.backup.parquet") == "parquet"
    assert driver._detect_format("data.tar.gz") == "csv"  # Unknown defaults to CSV

    # Query parameters
    assert driver._detect_format("s3://bucket/data.json?version=123") == "json"

    # Fragment
    assert driver._detect_format("http://example.com/data.csv#section") == "csv"
