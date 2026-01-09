# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Integration tests for SQLite connection pooling."""

from pathlib import Path

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.sqlite.core import build_connection_config
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("sqlite")


def test_shared_memory_pooling(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test that shared memory databases allow pooling."""
    config = sqlite_config_shared_memory

    assert config.connection_config["pool_min_size"] == 2
    assert config.connection_config["pool_max_size"] == 5

    with config.provide_session() as session1:
        session1.execute_script("""
            CREATE TABLE shared_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO shared_test (value) VALUES ('shared_data');
        """)
        session1.commit()

    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM shared_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "shared_data"

    config.close_pool()


def test_regular_memory_auto_conversion(sqlite_config_regular_memory: SqliteConfig) -> None:
    """Test that regular memory databases are auto-converted to shared memory with pooling enabled."""
    config = sqlite_config_regular_memory

    assert config.connection_config["pool_min_size"] == 5
    assert config.connection_config["pool_max_size"] == 10

    connection_config = build_connection_config(config.connection_config)
    db_uri = connection_config["database"]
    assert db_uri.startswith("file:memory_") and "cache=private" in db_uri
    assert connection_config["uri"] is True

    with config.provide_session() as session1:
        session1.execute_script("""
            CREATE TABLE auto_shared_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO auto_shared_test (value) VALUES ('auto_converted_data');
        """)
        session1.commit()

    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM auto_shared_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "auto_converted_data"

    config.close_pool()


def test_file_database_pooling_enabled(sqlite_temp_file_config: SqliteConfig) -> None:
    """Test that file-based databases allow pooling."""
    config = sqlite_temp_file_config

    assert config.connection_config["pool_min_size"] == 3
    assert config.connection_config["pool_max_size"] == 8

    with config.provide_session() as session1:
        session1.execute_script("""
            CREATE TABLE pool_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO pool_test (value) VALUES ('test_data');
        """)
        session1.commit()

    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM pool_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "test_data"

    config.close_pool()


def test_pool_session_isolation(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test that sessions from the pool share thread-local connections as expected.

    Note: SQLite uses thread-local connections, so multiple sessions in the same thread
    share the same underlying connection. This test verifies that behavior works correctly.
    """
    config = sqlite_config_shared_memory

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE isolation_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO isolation_test (value) VALUES ('base_data');
            """)
            session.commit()

        with config.provide_session() as session1, config.provide_session() as session2:
            assert session1.connection is session2.connection

            session1.execute("INSERT INTO isolation_test (value) VALUES (?)", ("session1_data",))

            result = session2.execute("SELECT COUNT(*) as count FROM isolation_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 2

            session2.execute("UPDATE isolation_test SET value = ? WHERE value = ?", ("updated_data", "session1_data"))

            result = session1.execute("SELECT value FROM isolation_test WHERE value = ?", ("updated_data",))
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "updated_data"

    finally:
        config.close_pool()


def test_pool_error_handling(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test pool behavior with errors and exceptions."""
    config = sqlite_config_shared_memory

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE error_test (
                    id INTEGER PRIMARY KEY,
                    unique_value TEXT UNIQUE
                );
            """)
            session.commit()

        with config.provide_session() as session:
            session.execute("INSERT INTO error_test (unique_value) VALUES (?)", ("unique1",))
            session.commit()

            with pytest.raises(Exception):
                session.execute("INSERT INTO error_test (unique_value) VALUES (?)", ("unique1",))

            result = session.execute("SELECT COUNT(*) as count FROM error_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

        with config.provide_session() as session:
            result = session.execute("SELECT COUNT(*) as count FROM error_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

    finally:
        config.close_pool()


def test_pool_transaction_rollback(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test transaction rollback behavior with pooled connections."""
    config = sqlite_config_shared_memory

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE transaction_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO transaction_test (value) VALUES ('initial_data');
            """)
            session.commit()

        with config.provide_session() as session:
            session.execute("INSERT INTO transaction_test (value) VALUES (?)", ("uncommitted_data",))

            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 2

            session.rollback()

            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

        with config.provide_session() as session:
            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

    finally:
        config.close_pool()


def test_config_with_connection_config_parameter(tmp_path: Path) -> None:
    """Test that SqliteConfig correctly accepts connection_config parameter."""

    db_path = tmp_path / "test.sqlite"
    connection_config = {"database": str(db_path), "timeout": 10.0, "check_same_thread": False}

    config = SqliteConfig(connection_config=connection_config)

    try:
        connection_config = build_connection_config(config.connection_config)
        assert connection_config["database"] == str(db_path)
        assert connection_config["timeout"] == 10.0
        assert connection_config["check_same_thread"] is False

        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        with config.provide_session() as session:
            result = session.execute("SELECT 1 as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == 1

    finally:
        config._close_pool()


def test_config_memory_database_conversion() -> None:
    """Test that :memory: databases are converted to shared memory."""

    config = SqliteConfig(connection_config={"database": ":memory:"})

    try:
        db_uri = config.connection_config["database"]
        assert db_uri.startswith("file:memory_") and "cache=private" in db_uri
        assert config.connection_config["uri"] is True

        with config.provide_session() as session:
            result = session.execute("SELECT 'memory_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "memory_test"

    finally:
        config._close_pool()


def test_config_default_database() -> None:
    """Test that default database is shared memory."""

    config = SqliteConfig()

    try:
        db_uri = config.connection_config["database"]
        assert db_uri.startswith("file:memory_") and "cache=private" in db_uri
        assert config.connection_config["uri"] is True

        with config.provide_session() as session:
            result = session.execute("SELECT 'default_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "default_test"

    finally:
        config._close_pool()
