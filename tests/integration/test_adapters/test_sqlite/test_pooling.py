"""Integration tests for SQLite connection pooling with CORE_ROUND_3 architecture."""

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("sqlite")
def test_shared_memory_pooling(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test that shared memory databases allow pooling."""
    config = sqlite_config_shared_memory

    # Verify pooling configuration
    assert config.pool_config["pool_min_size"] == 2
    assert config.pool_config["pool_max_size"] == 5

    # Test that multiple connections can access the same data
    with config.provide_session() as session1:
        # Create table in first session
        session1.execute_script("""
            CREATE TABLE shared_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO shared_test (value) VALUES ('shared_data');
        """)
        session1.commit()  # Commit to release locks

    # Get data from another session in the pool
    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM shared_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "shared_data"

    # Clean up
    config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_regular_memory_auto_conversion(sqlite_config_regular_memory: SqliteConfig) -> None:
    """Test that regular memory databases are auto-converted to shared memory with pooling enabled."""
    config = sqlite_config_regular_memory

    # Verify pooling configuration
    assert config.pool_config["pool_min_size"] == 5
    assert config.pool_config["pool_max_size"] == 10

    # Verify database was auto-converted to shared memory
    assert config._get_connection_config_dict()["database"] == "file::memory:?cache=shared"  # pyright: ignore[reportAttributeAccessIssue]
    assert config._get_connection_config_dict()["uri"] is True  # pyright: ignore[reportAttributeAccessIssue]

    # Test that multiple connections can access the same data (like shared memory test)
    with config.provide_session() as session1:
        # Create table in first session
        session1.execute_script("""
            CREATE TABLE auto_shared_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO auto_shared_test (value) VALUES ('auto_converted_data');
        """)
        session1.commit()  # Commit to release locks

    # Get data from another session in the pool
    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM auto_shared_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "auto_converted_data"

    # Clean up
    config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_file_database_pooling_enabled(sqlite_temp_file_config: SqliteConfig) -> None:
    """Test that file-based databases allow pooling."""
    config = sqlite_temp_file_config

    # Verify pooling configuration
    assert config.pool_config["pool_min_size"] == 3
    assert config.pool_config["pool_max_size"] == 8

    # Test that multiple connections work
    with config.provide_session() as session1:
        session1.execute_script("""
            CREATE TABLE pool_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO pool_test (value) VALUES ('test_data');
        """)
        session1.commit()  # Commit to persist data

    # Data persists across connections
    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM pool_test WHERE id = 1")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["value"] == "test_data"

    # Clean up
    config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_pool_session_isolation(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test that sessions from the pool share thread-local connections as expected.

    Note: SQLite uses thread-local connections, so multiple sessions in the same thread
    share the same underlying connection. This test verifies that behavior works correctly.
    """
    config = sqlite_config_shared_memory

    try:
        # Create base table
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE isolation_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO isolation_test (value) VALUES ('base_data');
            """)
            session.commit()

        # Test that multiple sessions share the same thread-local connection
        with config.provide_session() as session1, config.provide_session() as session2:
            # Verify sessions share the same connection (SQLite thread-local behavior)
            assert session1.connection is session2.connection

            # Session 1 inserts data (will be immediately visible to session2 since same connection)
            session1.execute("INSERT INTO isolation_test (value) VALUES (?)", ("session1_data",))

            # Session 2 should see the data since they share the same connection
            result = session2.execute("SELECT COUNT(*) as count FROM isolation_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 2  # base_data + session1_data

            # Both sessions can modify the same data since they share the connection
            session2.execute("UPDATE isolation_test SET value = ? WHERE value = ?", ("updated_data", "session1_data"))

            # Verify the update is visible from session1
            result = session1.execute("SELECT value FROM isolation_test WHERE value = ?", ("updated_data",))
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert len(result.data) == 1
            assert result.data[0]["value"] == "updated_data"

    finally:
        config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_pool_error_handling(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test pool behavior with errors and exceptions."""
    config = sqlite_config_shared_memory

    try:
        # Create test table
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE error_test (
                    id INTEGER PRIMARY KEY,
                    unique_value TEXT UNIQUE
                );
            """)
            session.commit()

        # Test that errors don't break the pool
        with config.provide_session() as session:
            # Insert initial data
            session.execute("INSERT INTO error_test (unique_value) VALUES (?)", ("unique1",))
            session.commit()

            # Try to insert duplicate (should fail)
            with pytest.raises(Exception):  # sqlite3.IntegrityError
                session.execute("INSERT INTO error_test (unique_value) VALUES (?)", ("unique1",))

            # Session should still be usable after error
            result = session.execute("SELECT COUNT(*) as count FROM error_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

        # Pool should still work after error in previous session
        with config.provide_session() as session:
            result = session.execute("SELECT COUNT(*) as count FROM error_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

    finally:
        config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_pool_transaction_rollback(sqlite_config_shared_memory: SqliteConfig) -> None:
    """Test transaction rollback behavior with pooled connections."""
    config = sqlite_config_shared_memory

    try:
        # Create test table
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE transaction_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                );
                INSERT INTO transaction_test (value) VALUES ('initial_data');
            """)
            session.commit()

        # Test rollback behavior
        with config.provide_session() as session:
            # Insert data but don't commit
            session.execute("INSERT INTO transaction_test (value) VALUES (?)", ("uncommitted_data",))

            # Verify data is visible within the same session
            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 2

            # Rollback the transaction
            session.rollback()

            # Verify data was rolled back
            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

        # Verify rollback persisted across sessions
        with config.provide_session() as session:
            result = session.execute("SELECT COUNT(*) as count FROM transaction_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1

    finally:
        config.close_pool()


@pytest.mark.xdist_group("sqlite")
def test_config_with_pool_config_parameter() -> None:
    """Test that SqliteConfig correctly accepts pool_config parameter."""

    # Define connection parameters using SqliteConnectionParams type
    pool_config = {"database": "test.sqlite", "timeout": 10.0, "check_same_thread": False}

    # Create config with pool_config
    config = SqliteConfig(pool_config=pool_config)

    try:
        # Verify the configuration was set correctly
        connection_config = config._get_connection_config_dict()
        assert connection_config["database"] == "test.sqlite"
        assert connection_config["timeout"] == 10.0
        assert connection_config["check_same_thread"] is False

        # Test that pool-related parameters are excluded from connection config
        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        # Verify the connection works
        with config.provide_session() as session:
            result = session.execute("SELECT 1 as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == 1

    finally:
        config._close_pool()


@pytest.mark.xdist_group("sqlite")
def test_config_memory_database_conversion() -> None:
    """Test that :memory: databases are converted to shared memory."""

    # Test with explicit :memory:
    config = SqliteConfig(pool_config={"database": ":memory:"})

    try:
        # Should be converted to shared memory
        assert config.pool_config["database"] == "file::memory:?cache=shared"
        assert config.pool_config["uri"] is True

        # Verify it works
        with config.provide_session() as session:
            result = session.execute("SELECT 'memory_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "memory_test"

    finally:
        config._close_pool()


@pytest.mark.xdist_group("sqlite")
def test_config_default_database() -> None:
    """Test that default database is shared memory."""

    # Test with no database specified
    config = SqliteConfig()

    try:
        # Should default to shared memory
        assert config.pool_config["database"] == "file::memory:?cache=shared"
        assert config.pool_config["uri"] is True

        # Verify it works
        with config.provide_session() as session:
            result = session.execute("SELECT 'default_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "default_test"

    finally:
        config._close_pool()
