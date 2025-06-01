"""Unit tests for AiosqlLoader with enhanced features."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.extensions.aiosql.loader import AiosqlLoader, SqlFileParseError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import AiosqlSQLOperationType


@pytest.fixture
def sample_sql_content() -> str:
    """Sample SQL content for testing."""
    return """
-- name: get_users^
-- Get all users from the database
SELECT id, name, email FROM users WHERE active = TRUE

-- name: get_user_by_id$
-- Get a single user by ID
SELECT id, name, email FROM users WHERE id = :user_id

-- name: create_user<!
-- Create a new user and return the created record
INSERT INTO users (name, email) VALUES (:name, :email) RETURNING id, name, email

-- name: update_user!
-- Update user information
UPDATE users SET name = :name, email = :email WHERE id = :user_id

-- name: delete_users_batch*!
-- Delete multiple users
DELETE FROM users WHERE id = ANY(:user_ids)

-- name: create_tables#
-- Create database schema
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    active BOOLEAN DEFAULT TRUE
)
"""


@pytest.fixture
def temp_sql_file(sample_sql_content: str) -> "Generator[Path, None, None]":
    """Create a temporary SQL file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sample_sql_content)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture(autouse=True)
def clear_singleton_cache() -> Generator[None, None, None]:
    """Clear singleton cache before each test."""
    AiosqlLoader._file_cache.clear()
    yield
    AiosqlLoader._file_cache.clear()


class TestAiosqlLoader:
    """Test AiosqlLoader functionality."""

    @patch("sqlspec.typing.AIOSQL_INSTALLED", False)
    def test_aiosql_loader_missing_dependency_error(self) -> None:
        """Test that MissingDependencyError is raised when aiosql is not installed."""
        with pytest.raises(MissingDependencyError, match="aiosql"):
            AiosqlLoader("test.sql")

    def test_aiosql_loader_initialization(self, temp_sql_file: Path) -> None:
        """Test AiosqlLoader initialization."""
        loader = AiosqlLoader(temp_sql_file)

        assert loader.sql_path == temp_sql_file.resolve()
        assert isinstance(loader.config, SQLConfig)
        assert len(loader.query_names) > 0

    def test_aiosql_loader_singleton_behavior(self, temp_sql_file: Path) -> None:
        """Test singleton behavior of AiosqlLoader."""
        loader1 = AiosqlLoader(temp_sql_file)
        loader2 = AiosqlLoader(temp_sql_file)

        # Same instance due to singleton pattern
        assert loader1 is loader2

    def test_aiosql_loader_invalid_path_security(self) -> None:
        """Test security validation of file paths."""
        suspicious_paths = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "~/secret_file",
            "$HOME/file",
        ]

        for path in suspicious_paths:
            with pytest.raises(SqlFileParseError, match="Potentially unsafe SQL file path"):
                AiosqlLoader(path)

    def test_aiosql_loader_nonexistent_file(self) -> None:
        """Test error handling for nonexistent files."""
        with pytest.raises(SqlFileParseError, match="SQL file not found"):
            AiosqlLoader("nonexistent_file.sql")

    def test_aiosql_loader_directory_instead_of_file(self, tmp_path: Path) -> None:
        """Test error handling when path is directory instead of file."""
        with pytest.raises(SqlFileParseError, match="Path is not a file"):
            AiosqlLoader(tmp_path)

    def test_aiosql_loader_parse_queries(self, temp_sql_file: Path) -> None:
        """Test parsing of SQL queries from file."""
        loader = AiosqlLoader(temp_sql_file)

        # Check that all expected queries are parsed
        expected_queries = {
            "get_users": AiosqlSQLOperationType.SELECT_ONE,
            "get_user_by_id": AiosqlSQLOperationType.SELECT_VALUE,
            "create_user": AiosqlSQLOperationType.INSERT_RETURNING,
            "update_user": AiosqlSQLOperationType.INSERT_UPDATE_DELETE,
            "delete_users_batch": AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY,
            "create_tables": AiosqlSQLOperationType.SCRIPT,
        }

        for query_name, expected_type in expected_queries.items():
            assert loader.has_query(query_name)
            operation_type = loader.get_operation_type(query_name)
            assert operation_type == expected_type

    def test_aiosql_loader_get_sql_basic(self, temp_sql_file: Path) -> None:
        """Test getting SQL object from loader."""
        config = SQLConfig(strict_mode=False)
        loader = AiosqlLoader(temp_sql_file, config=config)
        sql_obj = loader.get_sql("get_users")

        assert "SELECT id, name, email FROM users" in sql_obj.sql
        assert "WHERE active = TRUE" in sql_obj.sql

    def test_aiosql_loader_get_sql_with_filters(self, temp_sql_file: Path) -> None:
        """Test getting SQL object with filters."""
        from sqlspec.statement.filters import LimitOffsetFilter

        config = SQLConfig(strict_mode=False)
        loader = AiosqlLoader(temp_sql_file, config=config)
        sql_obj = loader.get_sql("get_users", LimitOffsetFilter(10, 0))

        assert "SELECT id, name, email FROM users" in sql_obj.sql
        assert "LIMIT" in sql_obj.sql

    def test_aiosql_loader_convenience_methods(self, temp_sql_file: Path) -> None:
        """Test convenience methods for different operation types."""
        config = SQLConfig(strict_mode=False)
        loader = AiosqlLoader(temp_sql_file, config=config)

        # Test SELECT methods
        select_sql = loader.get_select_sql("get_users")
        assert "SELECT" in select_sql.sql

        # Test INSERT methods
        insert_sql = loader.get_insert_sql("create_user")
        assert "INSERT" in insert_sql.sql

        # Test UPDATE methods
        update_sql = loader.get_update_sql("update_user")
        assert "UPDATE" in update_sql.sql

        # Test DELETE methods - update_user is actually an UPDATE/DELETE operation type
        delete_sql = loader.get_delete_sql("update_user")
        assert "UPDATE" in delete_sql.sql

        # Test SCRIPT methods
        script_sql = loader.get_script_sql("create_tables")
        assert "CREATE TABLE" in script_sql.sql

    def test_aiosql_loader_convenience_method_validation(self, temp_sql_file: Path) -> None:
        """Test that convenience methods validate operation types."""
        config = SQLConfig(strict_mode=False)
        loader = AiosqlLoader(temp_sql_file, config=config)

        # Try to get SELECT operation as INSERT - should fail
        with pytest.raises(SqlFileParseError, match="not an INSERT operation"):
            loader.get_insert_sql("get_users")

        # Try to get INSERT operation as SELECT - should fail
        with pytest.raises(SqlFileParseError, match="not a SELECT operation"):
            loader.get_select_sql("create_user")

    def test_aiosql_loader_get_nonexistent_query(self, temp_sql_file: Path) -> None:
        """Test error when getting nonexistent query."""
        loader = AiosqlLoader(temp_sql_file)

        with pytest.raises(SqlFileParseError, match="Query 'nonexistent' not found"):
            loader.get_sql("nonexistent")

    def test_aiosql_loader_dictionary_access(self, temp_sql_file: Path) -> None:
        """Test dictionary-like access to raw SQL."""
        loader = AiosqlLoader(temp_sql_file)

        # Test __contains__
        assert "get_users" in loader
        assert "nonexistent" not in loader

        # Test __getitem__ - returns raw SQL
        raw_sql = loader["get_users"]
        assert "SELECT id, name, email FROM users" in raw_sql

        # Test __len__
        assert len(loader) > 0

    def test_aiosql_loader_get_raw_sql(self, temp_sql_file: Path) -> None:
        """Test getting raw SQL text."""
        loader = AiosqlLoader(temp_sql_file)
        raw_sql = loader.get_raw_sql("get_users")

        assert "SELECT id, name, email FROM users" in raw_sql
        assert "WHERE active = TRUE" in raw_sql

    def test_aiosql_loader_operation_type_mapping(self) -> None:
        """Test mapping of aiosql operation suffixes to types."""
        test_cases = [
            ("^", AiosqlSQLOperationType.SELECT_ONE),
            ("$", AiosqlSQLOperationType.SELECT_VALUE),
            ("!", AiosqlSQLOperationType.INSERT_UPDATE_DELETE),
            ("<!", AiosqlSQLOperationType.INSERT_RETURNING),
            ("*!", AiosqlSQLOperationType.INSERT_UPDATE_DELETE_MANY),
            ("#", AiosqlSQLOperationType.SCRIPT),
            ("", AiosqlSQLOperationType.SELECT),
            ("unknown", AiosqlSQLOperationType.SELECT),  # Default
        ]

        for suffix, expected_type in test_cases:
            result = AiosqlLoader._map_operation_type(suffix)
            assert result == expected_type

    def test_aiosql_loader_enhanced_query_pattern_matching(self) -> None:
        """Test enhanced query pattern matching with parameters."""
        sql_with_params = """
-- name: get_user_by_id(user_id)^
SELECT * FROM users WHERE id = :user_id

-- name: create_user(name, email)<!
INSERT INTO users (name, email) VALUES (:name, :email) RETURNING *

-- name: get_all_users
SELECT * FROM users
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql_with_params)
            temp_path = Path(f.name)

        try:
            loader = AiosqlLoader(temp_path)

            # All queries should be parsed correctly
            assert "get_user_by_id" in loader
            assert "create_user" in loader
            assert "get_all_users" in loader

            # Check operation types
            assert loader.get_operation_type("get_user_by_id") == AiosqlSQLOperationType.SELECT_ONE
            assert loader.get_operation_type("create_user") == AiosqlSQLOperationType.INSERT_RETURNING
            assert loader.get_operation_type("get_all_users") == AiosqlSQLOperationType.SELECT

        finally:
            temp_path.unlink()

    def test_aiosql_loader_empty_file_error(self) -> None:
        """Test error handling for empty SQL files."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("")  # Empty file
            temp_path = Path(f.name)

        try:
            with pytest.raises(SqlFileParseError, match="No valid aiosql queries found"):
                AiosqlLoader(temp_path)
        finally:
            temp_path.unlink()

    def test_aiosql_loader_repr(self, temp_sql_file: Path) -> None:
        """Test string representation of AiosqlLoader."""
        loader = AiosqlLoader(temp_sql_file)
        repr_str = repr(loader)

        assert "AiosqlLoader" in repr_str
        assert "queries=" in repr_str

    def test_aiosql_loader_merge_sql_method(self, temp_sql_file: Path) -> None:
        """Test get_merge_sql method with actual MERGE query."""
        # Create a SQL file with a MERGE query
        merge_sql_content = """
-- name: upsert_user!
MERGE INTO users AS target
USING (VALUES (:id, :name, :email)) AS source (id, name, email)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name, email = source.email
WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (source.id, source.name, source.email)

-- name: regular_update!
UPDATE users SET name = :name WHERE id = :id
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(merge_sql_content)
            temp_path = Path(f.name)

        try:
            config = SQLConfig(strict_mode=False)
            loader = AiosqlLoader(temp_path, config=config)

            # Test successful MERGE operation
            merge_sql = loader.get_merge_sql("upsert_user")
            assert "MERGE INTO users" in merge_sql.sql
            assert "WHEN MATCHED" in merge_sql.sql

            # Test that non-MERGE query fails
            with pytest.raises(SqlFileParseError, match="does not contain MERGE statement"):
                loader.get_merge_sql("regular_update")

        finally:
            temp_path.unlink()
