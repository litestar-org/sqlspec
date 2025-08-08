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
    """Test that sessions from the pool maintain proper isolation."""
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

        # Test concurrent access with different sessions
        with config.provide_session() as session1, config.provide_session() as session2:
            # Session 1 inserts data
            session1.execute("INSERT INTO isolation_test (value) VALUES (?)", ("session1_data",))

            # Session 2 should not see uncommitted data from session 1
            result = session2.execute("SELECT COUNT(*) as count FROM isolation_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 1  # Only base_data

            # After session1 commits, session2 should see the data
            session1.commit()

            result = session2.execute("SELECT COUNT(*) as count FROM isolation_test")
            assert isinstance(result, SQLResult)
            assert result.data is not None
            assert result.data[0]["count"] == 2  # base_data + session1_data

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
