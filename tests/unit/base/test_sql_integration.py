# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for SQLSpec SQL loading integration.

Tests the integration of SQLFileLoader functionality into the SQLSpec base class,
ensuring that all SQL loading methods work correctly and don't interfere with
existing database configuration functionality.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sqlspec.base import SQLSpec
from sqlspec.core import SQL
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.loader import SQLFileLoader

pytestmark = pytest.mark.xdist_group("base")


def test_init_without_loader() -> None:
    """Test SQLSpec initialization without a loader."""
    sql_spec = SQLSpec()
    assert sql_spec._sql_loader is None


def test_init_with_loader() -> None:
    """Test SQLSpec initialization with a provided loader."""
    loader = SQLFileLoader()
    sql_spec = SQLSpec(loader=loader)
    assert sql_spec._sql_loader is loader


def test_lazy_loader_initialization() -> None:
    """Test that loader is created lazily when first needed."""
    sql_spec = SQLSpec()
    assert sql_spec._sql_loader is None

    # Trigger lazy initialization by calling a method that needs the loader
    sql_spec.add_named_sql("test", "SELECT 1")
    assert isinstance(sql_spec._sql_loader, SQLFileLoader)


def test_add_named_sql() -> None:
    """Test adding a named SQL query directly."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("test_query", "SELECT 1 AS result")

    assert sql_spec.has_sql_query("test_query")
    sql_obj = sql_spec.get_sql("test_query")
    assert isinstance(sql_obj, SQL)
    assert "SELECT 1 AS result" in sql_obj.sql


def test_add_named_sql_with_dialect() -> None:
    """Test adding a named SQL query with dialect."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("postgres_query", "SELECT ARRAY_AGG(name) FROM users", dialect="postgres")

    assert sql_spec.has_sql_query("postgres_query")
    sql_obj = sql_spec.get_sql("postgres_query")
    assert isinstance(sql_obj, SQL)


def test_get_sql_not_found() -> None:
    """Test getting a SQL query that doesn't exist."""
    sql_spec = SQLSpec()

    with pytest.raises(SQLFileNotFoundError):
        sql_spec.get_sql("nonexistent_query")


def test_list_sql_queries_empty() -> None:
    """Test listing queries when none are loaded."""
    sql_spec = SQLSpec()
    assert sql_spec.list_sql_queries() == []


def test_list_sql_queries_with_queries() -> None:
    """Test listing queries after adding some."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("query_a", "SELECT 1")
    sql_spec.add_named_sql("query_b", "SELECT 2")

    queries = sql_spec.list_sql_queries()
    assert sorted(queries) == ["query_a", "query_b"]


def test_has_sql_query_empty() -> None:
    """Test checking for query existence when none are loaded."""
    sql_spec = SQLSpec()
    assert not sql_spec.has_sql_query("any_query")


def test_has_sql_query_with_queries() -> None:
    """Test checking for query existence after adding some."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("existing_query", "SELECT 1")

    assert sql_spec.has_sql_query("existing_query")
    assert not sql_spec.has_sql_query("nonexistent_query")


def test_clear_sql_cache_no_loader() -> None:
    """Test clearing cache when no loader exists."""
    sql_spec = SQLSpec()

    sql_spec.clear_sql_cache()


def test_clear_sql_cache_with_loader() -> None:
    """Test clearing cache with existing loader."""
    sql_spec = SQLSpec()
    sql_spec.add_named_sql("test_query", "SELECT 1")

    assert sql_spec.has_sql_query("test_query")

    sql_spec.clear_sql_cache()

    assert not sql_spec.has_sql_query("test_query")


def test_reload_sql_files_no_loader() -> None:
    """Test reloading files when no loader exists."""
    sql_spec = SQLSpec()

    sql_spec.reload_sql_files()


def test_reload_sql_files_with_loader() -> None:
    """Test reloading files with existing loader."""
    sql_spec = SQLSpec()
    sql_spec.add_named_sql("test_query", "SELECT 1")

    assert sql_spec.has_sql_query("test_query")

    sql_spec.reload_sql_files()

    assert not sql_spec.has_sql_query("test_query")


def test_get_sql_files_empty() -> None:
    """Test getting file list when none are loaded."""
    sql_spec = SQLSpec()
    assert sql_spec.get_sql_files() == []


def test_load_sql_files() -> None:
    """Test loading SQL files from a directory."""
    sql_spec = SQLSpec()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        tf.write("""
-- name: test_query
SELECT id, name FROM users WHERE active = true;

-- name: count_users
SELECT COUNT(*) as total FROM users;
""")
        tf.flush()
        temp_path = Path(tf.name)

    try:
        sql_spec.load_sql_files(temp_path)

        queries = sql_spec.list_sql_queries()
        assert "test_query" in queries
        assert "count_users" in queries

        test_sql = sql_spec.get_sql("test_query")
        assert isinstance(test_sql, SQL)
        assert "SELECT id, name FROM users" in test_sql.sql

    finally:
        temp_path.unlink()


def test_provided_loader_is_used() -> None:
    """Test that a provided loader is used instead of creating a new one."""

    mock_loader = Mock(spec=SQLFileLoader)
    mock_loader.list_queries.return_value = ["mock_query"]
    mock_loader.has_query.return_value = True

    sql_spec = SQLSpec(loader=mock_loader)

    queries = sql_spec.list_sql_queries()
    assert queries == ["mock_query"]
    mock_loader.list_queries.assert_called_once()

    has_query = sql_spec.has_sql_query("test")
    assert has_query is True
    mock_loader.has_query.assert_called_once_with("test")


def test_sql_integration_with_existing_functionality() -> None:
    """Test that SQL loading doesn't interfere with existing SQLSpec functionality."""
    from sqlspec.adapters.sqlite import SqliteConfig

    sql_spec = SQLSpec()

    config = SqliteConfig(connection_config={"database": ":memory:"})
    returned_config = sql_spec.add_config(config)

    sql_spec.add_named_sql("get_users", "SELECT * FROM users")
    sql_spec.add_named_sql("count_users", "SELECT COUNT(*) FROM users")

    # Config instance IS the handle - add_config returns the same instance
    assert returned_config is config

    assert sql_spec.has_sql_query("get_users")
    sql_obj = sql_spec.get_sql("get_users")
    assert isinstance(sql_obj, SQL)

    with sql_spec.provide_session(config) as session:
        assert hasattr(session, "execute")


def test_sql_loader_cleanup_on_cache_clear() -> None:
    """Test proper cleanup when clearing SQL cache."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("query1", "SELECT 1")
    sql_spec.add_named_sql("query2", "SELECT 2")

    assert sql_spec._sql_loader is not None
    assert len(sql_spec.list_sql_queries()) == 2

    sql_spec.clear_sql_cache()

    assert sql_spec._sql_loader is not None
    assert len(sql_spec.list_sql_queries()) == 0


@patch("sqlspec.base.logger")
def test_logging_integration(mock_logger: Mock) -> None:
    """Test that SQL operations are properly logged."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("test_query", "SELECT 1")
    mock_logger.debug.assert_called_with("Added named SQL: %s", "test_query")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tf:
        tf.write("-- name: file_query\nSELECT 1;")
        tf.flush()
        temp_path = Path(tf.name)

    try:
        sql_spec.load_sql_files(temp_path)
        mock_logger.debug.assert_called_with("Loaded SQL files: %s", (temp_path,))
    finally:
        temp_path.unlink()

    sql_spec.clear_sql_cache()
    mock_logger.debug.assert_called_with("Cleared SQL cache")


def test_backwards_compatibility() -> None:
    """Test that existing SQLSpec usage patterns still work."""

    from sqlspec.adapters.sqlite import SqliteConfig

    sql_spec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sql_spec.add_config(config)

    with sql_spec.provide_session(config) as session:
        assert hasattr(session, "execute")

    original_cache_config = sql_spec.get_cache_config()
    assert original_cache_config is not None

    sql_spec.add_named_sql("new_query", "SELECT 1")
    assert sql_spec.has_sql_query("new_query")


def test_error_propagation() -> None:
    """Test that SQL loader errors are properly propagated."""
    sql_spec = SQLSpec()

    with pytest.raises(ValueError, match="already exists"):
        sql_spec.add_named_sql("duplicate", "SELECT 1")
        sql_spec.add_named_sql("duplicate", "SELECT 2")


def test_name_normalization_consistency() -> None:
    """Test that name normalization works consistently."""
    sql_spec = SQLSpec()

    sql_spec.add_named_sql("user-profile-query", "SELECT * FROM user_profiles")

    assert sql_spec.has_sql_query("user_profile_query")

    sql_obj = sql_spec.get_sql("user_profile_query")
    assert isinstance(sql_obj, SQL)
