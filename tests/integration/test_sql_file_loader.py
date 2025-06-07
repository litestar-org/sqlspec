"""Integration tests for SQL file loader."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.loader import SQLFileLoader
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


@pytest.fixture
def temp_sql_files() -> Generator[Path, None, None]:
    """Create temporary SQL files with aiosql-style named queries."""
    with tempfile.TemporaryDirectory() as temp_dir:
        sql_dir = Path(temp_dir)

        # Create SQL file with named queries
        users_sql = sql_dir / "users.sql"
        users_sql.write_text(
            """
-- name: get_user_by_id
-- Get a single user by their ID
SELECT id, name, email FROM users WHERE id = :user_id;

-- name: list_users
-- List users with limit
SELECT id, name, email FROM users ORDER BY name LIMIT :limit;

-- name: create_user
-- Create a new user
INSERT INTO users (name, email) VALUES (:name, :email);
""".strip()
        )

        # Create subdirectory with more files
        queries_dir = sql_dir / "queries"
        queries_dir.mkdir()

        stats_sql = queries_dir / "stats.sql"
        stats_sql.write_text(
            """
-- name: count_users
-- Count total users
SELECT COUNT(*) as total FROM users;

-- name: user_stats
-- Get user statistics
SELECT COUNT(*) as user_count, MAX(created_at) as last_signup FROM users;
""".strip()
        )

        yield sql_dir


class TestSQLFileLoaderIntegration:
    """Integration tests for SQLFileLoader with real filesystem."""

    def test_load_sql_file_from_filesystem(self, temp_sql_files: Path) -> None:
        """Test loading a SQL file from the filesystem."""
        loader = SQLFileLoader()
        users_file = temp_sql_files / "users.sql"

        loader.load_sql(users_file)

        # Test getting a SQL object from loaded queries
        sql_obj = loader.get_sql("get_user_by_id", user_id=123)

        assert isinstance(sql_obj, SQL)
        assert "SELECT id, name, email FROM users WHERE id = :user_id" in sql_obj.to_sql()

    def test_load_directory_with_namespacing(self, temp_sql_files: Path) -> None:
        """Test loading a directory with automatic namespacing."""
        loader = SQLFileLoader()

        # Load entire directory
        loader.load_sql(temp_sql_files)

        # Check queries were loaded with proper namespacing
        available_queries = loader.list_queries()

        # Root-level queries (no namespace)
        assert "get_user_by_id" in available_queries
        assert "list_users" in available_queries
        assert "create_user" in available_queries

        # Namespaced queries from subdirectory
        assert "queries.count_users" in available_queries
        assert "queries.user_stats" in available_queries

    def test_get_sql_with_parameters(self, temp_sql_files: Path) -> None:
        """Test getting SQL objects with parameters."""
        loader = SQLFileLoader()
        loader.load_sql(temp_sql_files / "users.sql")

        # Get SQL with parameters using the parameters argument
        sql_obj = loader.get_sql("list_users", parameters={"limit": 10})

        assert isinstance(sql_obj, SQL)
        # Parameters should be available
        assert sql_obj._raw_parameters == {"limit": 10}

        # Also test with kwargs
        sql_obj2 = loader.get_sql("list_users", parameters={"limit": 20})
        assert sql_obj2._raw_parameters == {"limit": 20}

    def test_query_not_found_error(self, temp_sql_files: Path) -> None:
        """Test error when query not found."""
        loader = SQLFileLoader()
        loader.load_sql(temp_sql_files / "users.sql")

        with pytest.raises(SQLFileNotFoundError) as exc_info:
            loader.get_sql("nonexistent_query")

        assert "Query 'nonexistent_query' not found" in str(exc_info.value)

    def test_add_named_sql_directly(self, temp_sql_files: Path) -> None:
        """Test adding named SQL queries directly."""
        loader = SQLFileLoader()

        # Add a query directly
        loader.add_named_sql("health_check", "SELECT 'OK' as status")

        # Should be able to get it
        sql_obj = loader.get_sql("health_check")
        assert isinstance(sql_obj, SQL)
        # Check that the original raw SQL is available
        raw_text = loader.get_query_text("health_check")
        assert "SELECT 'OK' as status" in raw_text

    def test_duplicate_query_name_error(self, temp_sql_files: Path) -> None:
        """Test error when adding duplicate query names."""
        loader = SQLFileLoader()
        loader.add_named_sql("test_query", "SELECT 1")

        with pytest.raises(ValueError) as exc_info:
            loader.add_named_sql("test_query", "SELECT 2")

        assert "Query name 'test_query' already exists" in str(exc_info.value)

    def test_get_file_methods(self, temp_sql_files: Path) -> None:
        """Test file retrieval methods."""
        loader = SQLFileLoader()
        users_file = temp_sql_files / "users.sql"
        loader.load_sql(users_file)

        # Test get_file
        sql_file = loader.get_file(str(users_file))
        assert sql_file is not None
        assert sql_file.path == str(users_file)
        assert "get_user_by_id" in sql_file.content

        # Test get_file_for_query
        query_file = loader.get_file_for_query("get_user_by_id")
        assert query_file is not None
        assert query_file.path == str(users_file)

    def test_has_query(self, temp_sql_files: Path) -> None:
        """Test query existence checking."""
        loader = SQLFileLoader()
        loader.load_sql(temp_sql_files / "users.sql")

        assert loader.has_query("get_user_by_id") is True
        assert loader.has_query("nonexistent") is False

    def test_clear_cache(self, temp_sql_files: Path) -> None:
        """Test clearing the cache."""
        loader = SQLFileLoader()
        loader.load_sql(temp_sql_files / "users.sql")

        assert len(loader.list_queries()) > 0
        assert len(loader.list_files()) > 0

        loader.clear_cache()

        assert len(loader.list_queries()) == 0
        assert len(loader.list_files()) == 0

    def test_get_query_text(self, temp_sql_files: Path) -> None:
        """Test getting raw SQL text."""
        loader = SQLFileLoader()
        loader.load_sql(temp_sql_files / "users.sql")

        query_text = loader.get_query_text("get_user_by_id")
        assert "SELECT id, name, email FROM users WHERE id = :user_id" in query_text


class TestStorageBackendIntegration:
    """Test SQL file loader with different storage backends."""

    def test_load_from_uri_path(self, temp_sql_files: Path) -> None:
        """Test loading SQL files using URI path."""
        loader = SQLFileLoader()

        # Create a file with named queries for URI loading
        test_file = temp_sql_files / "uri_test.sql"
        test_file.write_text(
            """
-- name: test_query
SELECT 'URI test' as message;
""".strip()
        )

        # Load using file:// URI
        loader.load_sql(f"file://{test_file}")

        # Should be able to get the query
        sql_obj = loader.get_sql("test_query")
        assert isinstance(sql_obj, SQL)
        # Check the raw query text instead
        raw_text = loader.get_query_text("test_query")
        assert "SELECT 'URI test' as message" in raw_text

    def test_mixed_local_and_uri_loading(self, temp_sql_files: Path) -> None:
        """Test loading both local files and URIs."""
        loader = SQLFileLoader()

        # Load local file
        users_file = temp_sql_files / "users.sql"
        loader.load_sql(users_file)

        # Create another file for URI loading
        uri_file = temp_sql_files / "uri_queries.sql"
        uri_file.write_text(
            """
-- name: health_check
SELECT 'OK' as status;

-- name: version_info
SELECT '1.0.0' as version;
""".strip()
        )

        # Load using URI
        loader.load_sql(f"file://{uri_file}")

        # Should have queries from both sources
        queries = loader.list_queries()
        assert "get_user_by_id" in queries  # From local file
        assert "health_check" in queries  # From URI file
        assert "version_info" in queries  # From URI file
