"""Integration tests for SQLite connection management."""

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteConnectionConfig
from sqlspec.statement.result import SQLResult


@pytest.mark.xdist_group("sqlite")
def test_sqlite_basic_connection() -> None:
    """Test basic SQLite connection functionality."""
    config = SqliteConfig(connection_config=SqliteConnectionConfig(database=":memory:"))

    # Test direct connection
    with config.provide_connection() as conn:
        assert conn is not None
        # Test basic connection properties
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        # SQLite returns sqlite3.Row when row_factory is set
        assert result[0] == 1
        cursor.close()

    # Test session management
    with config.provide_session() as session:
        assert session is not None
        # Test basic query through session
        result = session.execute("SELECT 1 AS test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("sqlite")
def test_sqlite_file_database_connection() -> None:
    """Test SQLite file database connection."""
    import os
    import tempfile

    # Create temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    try:
        config = SqliteConfig(connection_config=SqliteConnectionConfig(database=db_path))

        with config.provide_session() as session:
            # Create a table and insert data
            session.execute_script("""
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)

            insert_result = session.execute("INSERT INTO test_table (name) VALUES (?)", ("test_name",))
            assert isinstance(insert_result, SQLResult)
            assert insert_result.rows_affected == 1

            # Verify data persists
            select_result = session.execute("SELECT name FROM test_table")
            assert isinstance(select_result, SQLResult)
            assert select_result.data is not None
            assert len(select_result.data) == 1
            assert select_result.data[0]["name"] == "test_name"

    finally:
        # Clean up temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.mark.xdist_group("sqlite")
def test_sqlite_connection_configuration() -> None:
    """Test SQLite connection with various configuration options."""
    # Test with timeout
    config = SqliteConfig(
        connection_config=SqliteConnectionConfig(
            database=":memory:",
            timeout=30.0,
            check_same_thread=False,
        )
    )

    with config.provide_session() as session:
        result = session.execute("SELECT 1 AS configured")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["configured"] == 1


@pytest.mark.xdist_group("sqlite")
def test_sqlite_isolation_levels() -> None:
    """Test SQLite with different isolation levels."""
    for isolation_level in [None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE"]:
        config = SqliteConfig(
            connection_config=SqliteConnectionConfig(
                database=":memory:",
                isolation_level=isolation_level,
            )
        )

        with config.provide_session() as session:
            result = session.execute("SELECT 1 AS isolation_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["isolation_test"] == 1
