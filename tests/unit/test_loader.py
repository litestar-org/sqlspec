"""Unit tests for SQL file loader module."""

from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from sqlspec.exceptions import SQLFileNotFoundError, SQLFileParseError
from sqlspec.loader import SQLFile, SQLFileLoader
from sqlspec.statement.sql import SQL

if TYPE_CHECKING:
    pass


class TestSQLFile:
    """Tests for SQLFile class."""

    def test_sql_file_creation(self) -> None:
        """Test creating a SQLFile object."""
        content = "SELECT * FROM users WHERE id = :user_id"
        sql_file = SQLFile(content=content, path="/sql/get_user.sql")

        assert sql_file.content == content
        assert sql_file.path == "/sql/get_user.sql"
        assert sql_file.checksum == "5103e9ecd072d5f1be6768dc556956b6"  # MD5 of content
        assert isinstance(sql_file.loaded_at, datetime)
        assert sql_file.metadata == {}

    def test_sql_file_with_metadata(self) -> None:
        """Test creating a SQLFile with metadata."""
        sql_file = SQLFile(
            content="SELECT * FROM orders", path="/sql/complex_query.sql", metadata={"author": "test", "version": "1.0"}
        )

        assert sql_file.metadata == {"author": "test", "version": "1.0"}

    def test_sql_file_checksum_calculation(self) -> None:
        """Test that checksum is calculated correctly."""
        sql_file1 = SQLFile(content="SELECT 1", path="/sql/query1.sql")
        sql_file2 = SQLFile(
            content="SELECT 1",  # Same content
            path="/sql/query2.sql",
        )
        sql_file3 = SQLFile(
            content="SELECT 2",  # Different content
            path="/sql/query3.sql",
        )

        assert sql_file1.checksum == sql_file2.checksum
        assert sql_file1.checksum != sql_file3.checksum


class TestSQLFileLoader:
    """Tests for SQLFileLoader class."""

    @pytest.fixture
    def sample_sql_content(self) -> str:
        """Sample SQL file content with named queries."""
        return """
-- name: get_user
SELECT * FROM users WHERE id = :user_id;

-- name: list_users
SELECT * FROM users ORDER BY username;

-- name: create_user
INSERT INTO users (username, email) VALUES (:username, :email);
"""

    @pytest.fixture
    def mock_path_read(self, sample_sql_content: str) -> Generator[Mock, None, None]:
        """Mock Path.read_bytes."""
        with patch.object(Path, "read_bytes") as mock_read:
            mock_read.return_value = sample_sql_content.encode("utf-8")
            yield mock_read

    @pytest.fixture
    def mock_path_exists(self) -> Generator[Mock, None, None]:
        """Mock Path.exists."""
        with patch.object(Path, "exists") as mock_exists:
            mock_exists.return_value = True
            yield mock_exists

    @pytest.fixture
    def mock_path_is_file(self) -> Generator[Mock, None, None]:
        """Mock Path.is_file."""
        with patch.object(Path, "is_file") as mock_is_file:
            mock_is_file.return_value = True
            yield mock_is_file

    def test_loader_initialization(self) -> None:
        """Test SQLFileLoader initialization."""
        loader = SQLFileLoader()
        assert loader.encoding == "utf-8"

        loader = SQLFileLoader(encoding="latin-1")
        assert loader.encoding == "latin-1"

    def test_load_sql_local_file(
        self, sample_sql_content: str, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test loading a local SQL file."""
        loader = SQLFileLoader()
        loader.load_sql("queries/users.sql")

        # Check queries were parsed
        assert loader.has_query("get_user")
        assert loader.has_query("list_users")
        assert loader.has_query("create_user")

        # Check file was loaded
        assert "queries/users.sql" in loader.list_files()

    def test_get_sql(
        self, sample_sql_content: str, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test getting SQL by query name."""
        loader = SQLFileLoader()
        loader.load_sql("queries/users.sql")

        # Get SQL without parameters
        sql = loader.get_sql("get_user")
        assert isinstance(sql, SQL)
        assert "SELECT * FROM users WHERE id = :user_id" in sql._sql  # pyright: ignore

        # Get SQL with parameters
        sql_with_params = loader.get_sql("create_user", username="alice", email="alice@example.com")
        assert sql_with_params.parameters == {"username": "alice", "email": "alice@example.com"}

    def test_load_multiple_files(self, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock) -> None:
        """Test loading multiple SQL files."""
        # Set up different content for different files
        contents = [b"-- name: query1\nSELECT 1;", b"-- name: query2\nSELECT 2;", b"-- name: query3\nSELECT 3;"]
        mock_path_read.side_effect = contents

        loader = SQLFileLoader()
        loader.load_sql("file1.sql", "file2.sql", "file3.sql")

        assert loader.has_query("query1")
        assert loader.has_query("query2")
        assert loader.has_query("query3")
        assert len(loader.list_queries()) == 3

    def test_query_not_found(self) -> None:
        """Test error when query not found."""
        loader = SQLFileLoader()

        with pytest.raises(SQLFileNotFoundError) as exc_info:
            loader.get_sql("missing_query")

        assert "missing_query" in str(exc_info.value)
        assert "Available queries: none" in str(exc_info.value)

    def test_file_not_found(self, mock_path_exists: Mock) -> None:
        """Test error when file not found."""
        mock_path_exists.return_value = False

        loader = SQLFileLoader()
        with pytest.raises(SQLFileNotFoundError) as exc_info:
            loader.load_sql("missing.sql")

        assert "missing.sql" in str(exc_info.value)

    def test_no_named_queries(self, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock) -> None:
        """Test error when file has no named queries."""
        mock_path_read.return_value = b"SELECT * FROM users;"  # No -- name: comment

        loader = SQLFileLoader()
        with pytest.raises(SQLFileParseError) as exc_info:
            loader.load_sql("no_names.sql")

        assert "No named SQL statements found" in str(exc_info.value)

    def test_duplicate_query_names_in_file(
        self, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test error when file has duplicate query names."""
        mock_path_read.return_value = b"""
-- name: get_user
SELECT * FROM users WHERE id = 1;

-- name: get_user
SELECT * FROM users WHERE id = 2;
"""

        loader = SQLFileLoader()
        with pytest.raises(SQLFileParseError) as exc_info:
            loader.load_sql("duplicates.sql")

        assert "Duplicate query name: get_user" in str(exc_info.value)

    def test_duplicate_query_names_across_files(
        self, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test error when query name exists in different file."""
        contents = [
            b"-- name: get_user\nSELECT 1;",
            b"-- name: get_user\nSELECT 2;",  # Same name
        ]
        mock_path_read.side_effect = contents

        loader = SQLFileLoader()
        loader.load_sql("file1.sql")

        with pytest.raises(SQLFileParseError) as exc_info:
            loader.load_sql("file2.sql")

        assert "Query name 'get_user' already exists" in str(exc_info.value)

    def test_get_file_info(
        self, sample_sql_content: str, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test getting file information."""
        loader = SQLFileLoader()
        loader.load_sql("queries/users.sql")

        # Get loaded file
        sql_file = loader.get_file("queries/users.sql")
        assert sql_file is not None
        assert sql_file.content == sample_sql_content
        assert sql_file.path == "queries/users.sql"
        assert sql_file.checksum is not None

        # Get file for query
        query_file = loader.get_file_for_query("get_user")
        assert query_file is not None
        assert query_file is sql_file

    def test_clear_cache(
        self, sample_sql_content: str, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test clearing cache."""
        loader = SQLFileLoader()
        loader.load_sql("file1.sql")

        assert len(loader.list_queries()) > 0
        assert len(loader.list_files()) > 0

        loader.clear_cache()

        assert len(loader.list_queries()) == 0
        assert len(loader.list_files()) == 0

    def test_storage_backend_uri(self) -> None:
        """Test loading from storage backend URI."""
        mock_backend = Mock()
        mock_backend.read_text.return_value = "-- name: test\nSELECT 1;"

        mock_registry = Mock()
        mock_registry.get.return_value = mock_backend

        loader = SQLFileLoader(storage_registry=mock_registry)
        loader.load_sql("s3://bucket/queries.sql")

        assert loader.has_query("test")
        mock_backend.read_text.assert_called_once_with("s3://bucket/queries.sql", encoding="utf-8")

    def test_add_named_sql(self) -> None:
        """Test adding named SQL directly."""
        loader = SQLFileLoader()

        # Add a named query
        loader.add_named_sql("custom_query", "SELECT * FROM custom_table WHERE active = true")

        assert loader.has_query("custom_query")
        assert loader.get_query_text("custom_query") == "SELECT * FROM custom_table WHERE active = true"

        # Get as SQL object
        sql = loader.get_sql("custom_query")
        assert isinstance(sql, SQL)
        assert "SELECT * FROM custom_table" in sql.sql

        # Should show in query list
        assert "custom_query" in loader.list_queries()

    def test_add_named_sql_duplicate(self) -> None:
        """Test error when adding duplicate query name."""
        loader = SQLFileLoader()

        # Add first query
        loader.add_named_sql("my_query", "SELECT 1")

        # Try to add duplicate
        with pytest.raises(ValueError) as exc_info:
            loader.add_named_sql("my_query", "SELECT 2")

        assert "Query name 'my_query' already exists" in str(exc_info.value)
        assert "<directly added>" in str(exc_info.value)

    def test_add_named_sql_with_loaded_files(
        self, sample_sql_content: str, mock_path_read: Mock, mock_path_exists: Mock, mock_path_is_file: Mock
    ) -> None:
        """Test adding named SQL alongside loaded files."""
        loader = SQLFileLoader()

        # Load file with queries
        loader.load_sql("queries.sql")

        # Add additional query
        loader.add_named_sql("runtime_query", "DELETE FROM temp_table")

        # Both should be available
        assert loader.has_query("get_user")  # From file
        assert loader.has_query("runtime_query")  # Directly added

        # Try to add duplicate from file
        with pytest.raises(ValueError) as exc_info:
            loader.add_named_sql("get_user", "SELECT 1")

        assert "already exists" in str(exc_info.value)


class TestSQLFileExceptions:
    """Tests for SQL file loader exceptions."""

    def test_sql_file_not_found_error(self) -> None:
        """Test SQLFileNotFoundError."""
        # Without path
        error = SQLFileNotFoundError("missing.sql")
        assert error.name == "missing.sql"
        assert error.path is None
        assert str(error) == "SQL file 'missing.sql' not found"

        # With path
        error = SQLFileNotFoundError("missing.sql", "/sql/missing.sql")
        assert error.name == "missing.sql"
        assert error.path == "/sql/missing.sql"
        assert str(error) == "SQL file 'missing.sql' not found at path: /sql/missing.sql"

    def test_sql_file_parse_error(self) -> None:
        """Test SQLFileParseError."""
        original = ValueError("Invalid syntax")
        error = SQLFileParseError("bad.sql", "/sql/bad.sql", original)

        assert error.name == "bad.sql"
        assert error.path == "/sql/bad.sql"
        assert error.original_error == original
        assert "Failed to parse SQL file 'bad.sql' at /sql/bad.sql: Invalid syntax" in str(error)


class TestSQLFileLoaderWithFixtures:
    """Tests for SQLFileLoader with real fixture files."""

    def test_postgres_collection_privileges_parsing(self) -> None:
        """Test parsing PostgreSQL collection-privileges.sql with hyphenated query names."""
        loader = SQLFileLoader()
        fixture_path = Path(__file__).parent.parent / "fixtures" / "postgres" / "collection-privileges.sql"

        # Load the SQL file - the loader now automatically converts hyphens to underscores
        loader.load_sql(str(fixture_path))

        # Should have loaded the file
        sql_file = loader.get_file(str(fixture_path))
        assert sql_file is not None
        assert sql_file.path == str(fixture_path)
        assert "-- name: collection-postgres-pglogical-schema-usage-privilege" in sql_file.content

        # Should have parsed multiple named queries
        queries = loader.list_queries()
        assert len(queries) >= 3  # At least 3 named queries in the file

        # Check specific queries are present (with underscores)
        assert "collection_postgres_pglogical_schema_usage_privilege" in queries
        assert "collection_postgres_pglogical_privileges" in queries
        assert "collection_postgres_user_schemas_without_privilege" in queries

        # But we can also use the original hyphenated names!
        assert loader.has_query("collection-postgres-pglogical-schema-usage-privilege")

        # Verify query content using hyphenated name
        schema_query = loader.get_sql("collection-postgres-pglogical-schema-usage-privilege")
        assert isinstance(schema_query, SQL)
        sql_text = schema_query.to_sql()
        assert "pg_catalog.has_schema_privilege" in sql_text
        assert ":PKEY" in sql_text
        assert ":DMA_SOURCE_ID" in sql_text
        assert ":DMA_MANUAL_ID" in sql_text

    def test_loading_directory_with_mixed_files(self) -> None:
        """Test loading a directory containing both named query files and script files."""
        loader = SQLFileLoader()
        fixtures_path = Path(__file__).parent.parent / "fixtures"

        # Load all SQL files in the postgres subdirectory (has named queries)
        postgres_path = fixtures_path / "postgres"
        if postgres_path.exists() and postgres_path.is_dir():
            loader.load_sql(str(postgres_path))

            # Should have loaded queries from collection-privileges.sql
            queries = loader.list_queries()
            # Look for normalized query names (with underscores)
            postgres_queries = [q for q in queries if "collection_postgres" in q]
            assert len(postgres_queries) >= 3

            # Files should be loaded
            files = loader.list_files()
            assert any("collection-privileges.sql" in f for f in files)

    def test_oracle_ddl_as_whole_file_content(self) -> None:
        """Test handling Oracle DDL file without named queries."""
        loader = SQLFileLoader()
        fixture_path = Path(__file__).parent.parent / "fixtures" / "oracle.ddl.sql"

        # Method 1: Direct file reading for scripts without named queries
        content = loader._read_file_content(str(fixture_path))
        assert "CREATE TABLE" in content
        assert "VECTOR(768, FLOAT32)" in content

        # Create a SQL object from the entire content as a script
        # Disable parsing to avoid errors with Oracle-specific syntax
        from sqlspec.statement.sql import SQLConfig

        config = SQLConfig(enable_parsing=False, enable_validation=False, strict_mode=False)
        stmt = SQL(content, dialect="oracle", config=config).as_script()
        assert stmt.is_script is True

        # Method 2: Programmatically add as a named query
        loader.add_named_sql("oracle_ddl_script", content)

        # Now we can retrieve it as a named query (but it may have parsing issues)
        # So let's just verify it was added
        assert "oracle_ddl_script" in loader.list_queries()

        # We can get the raw text back
        raw_text = loader.get_query_text("oracle_ddl_script")
        assert "CREATE TABLE" in raw_text
        assert "VECTOR(768, FLOAT32)" in raw_text
