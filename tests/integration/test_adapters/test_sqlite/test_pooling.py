"""Integration tests for SQLite connection pooling."""

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig


@pytest.mark.xdist_group("sqlite")
def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""
    # Create config with shared memory database
    config = SqliteConfig(
        pool_config={"database": "file::memory:?cache=shared", "uri": True, "pool_min_size": 2, "pool_max_size": 5}
    )

    # Verify pooling is not disabled
    assert config.min_pool_size == 2
    assert config.max_pool_size == 5

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
        data = result.get_data()
        assert len(data) == 1
        assert data[0]["value"] == "shared_data"


@pytest.mark.xdist_group("sqlite")
def test_regular_memory_auto_conversion() -> None:
    """Test that regular memory databases are auto-converted to shared memory with pooling enabled."""
    # Create config with regular memory database
    config = SqliteConfig(pool_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    # Verify pooling is enabled with requested sizes
    assert config.min_pool_size == 5
    assert config.max_pool_size == 10

    # Verify database was auto-converted to shared memory
    assert config.connection_config["database"] == "file::memory:?cache=shared"
    assert config.connection_config["uri"] is True

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
        data = result.get_data()
        assert len(data) == 1
        assert data[0]["value"] == "auto_converted_data"


@pytest.mark.xdist_group("sqlite")
def test_file_database_pooling_enabled() -> None:
    """Test that file-based databases allow pooling."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        # Create config with file database
        config = SqliteConfig(pool_config={"database": db_path, "pool_min_size": 3, "pool_max_size": 8})

        # Verify pooling is enabled
        assert config.min_pool_size == 3
        assert config.max_pool_size == 8

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
            data = result.get_data()
            assert len(data) == 1
            assert data[0]["value"] == "test_data"
    finally:
        import os

        try:
            os.unlink(db_path)
        except Exception:
            pass
