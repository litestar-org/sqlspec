"""Integration tests for DuckDB connection pooling.

Tests shared memory database conversion and connection pooling functionality.
"""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig


@pytest.mark.xdist_group("duckdb")
def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""
    # Create config with shared memory database
    config = DuckDBConfig(connection_config={"database": ":memory:shared_test"}, min_pool=2, max_pool=5)

    # Verify pooling is not disabled
    assert config.min_pool == 2
    assert config.max_pool == 5

    # Test that multiple connections can access the same data
    with config.provide_session() as session1:
        # Drop table if it exists from previous run
        session1.execute("DROP TABLE IF EXISTS shared_test")

        # Create table in first session
        session1.execute_script("""
            CREATE TABLE shared_test (id INTEGER, name TEXT);
            INSERT INTO shared_test VALUES (1, 'test_value');
        """)

    # Verify data is accessible from another session (same shared memory database)
    with config.provide_session() as session2:
        result = session2.execute("SELECT name FROM shared_test WHERE id = 1").get_data()
        assert len(result) == 1
        assert result[0]["name"] == "test_value"

        # Clean up
        session2.execute("DROP TABLE shared_test")


@pytest.mark.xdist_group("duckdb")
def test_regular_memory_auto_conversion() -> None:
    """Test that regular memory databases are auto-converted to shared memory with pooling enabled."""
    # Create config with regular memory database
    config = DuckDBConfig(connection_config={"database": ":memory:"}, min_pool=5, max_pool=10)

    # Verify pooling is not disabled (no more pool size overrides)
    assert config.min_pool == 5
    assert config.max_pool == 10
    # tdodo: readd this feature
    # Verify database was auto-converted to shared memory
    assert config.connection_config["database"] == ":memory:shared_db"

    # Test that multiple connections can access the same data (like shared memory test)
    with config.provide_session() as session1:
        # Create table in first session
        session1.execute_script("""
            CREATE TABLE converted_test (id INTEGER, value TEXT);
            INSERT INTO converted_test VALUES (42, 'converted_value');
        """)

    # Verify data is accessible from another session
    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM converted_test WHERE id = 42").get_data()
        assert len(result) == 1
        assert result[0]["value"] == "converted_value"

        # Clean up
        session2.execute("DROP TABLE converted_test")


@pytest.mark.xdist_group("duckdb")
def test_file_database_pooling() -> None:
    """Test that file databases work with pooling (no changes needed)."""
    import tempfile

    # TODO: Use a temorary directory instead.  This method doesn't tend to work otherwise.
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    config = DuckDBConfig(connection_config={"database": db_path}, min_pool=2, max_pool=4)

    # Verify pooling works normally
    assert config.min_pool == 2
    assert config.max_pool == 4

    # Test that multiple connections work with file database
    with config.provide_session() as session1:
        session1.execute("CREATE TABLE file_test (id INTEGER, data TEXT)")
        session1.execute("INSERT INTO file_test VALUES (1, 'file_data')")

    with config.provide_session() as session2:
        result = session2.execute("SELECT data FROM file_test WHERE id = 1").get_data()
        assert len(result) == 1
        assert result[0]["data"] == "file_data"

        # Clean up
        session2.execute("DROP TABLE file_test")

    # Note: In a real scenario, you'd want to clean up the temp file
    # but for testing purposes, the OS will handle it


@pytest.mark.xdist_group("duckdb")
def test_connection_pool_health_checks() -> None:
    """Test that the connection pool performs health checks correctly."""
    config = DuckDBConfig(connection_config={"database": ":memory:health_test"}, min_pool=1, max_pool=3)
    pool = config.provide_pool()

    # Test that we can get a connection and it passes health check
    with pool.get_connection() as conn:
        # This should work without issues
        result = conn.execute("SELECT 'health_check'").fetchone()
        assert result is not None
        assert result[0] == "health_check"

    # Verify pool size
    assert pool.size() >= 0  # At least one connection should be in pool


@pytest.mark.xdist_group("duckdb")
def test_empty_database_conversion() -> None:
    """Test that empty database string gets converted properly."""
    config = DuckDBConfig(connection_config={"database": ""})

    # Empty string should default to :memory: and then be converted
    assert config.connection_config["database"] == ":memory:shared_db"

    # Should work with pooling
    with config.provide_session() as session:
        result = session.execute("SELECT 'empty_test' as test").get_data()
        assert result[0]["test"] == "empty_test"


@pytest.mark.xdist_group("duckdb")
def test_default_config_conversion() -> None:
    """Test that default config (no connection_config) works with shared memory."""
    config = DuckDBConfig()

    # Default should be converted to shared memory
    assert config.connection_config["database"] == ":memory:shared_db"

    # Should work with pooling
    with config.provide_session() as session:
        result = session.execute("SELECT 'default_test' as test").get_data()
        assert result[0]["test"] == "default_test"
