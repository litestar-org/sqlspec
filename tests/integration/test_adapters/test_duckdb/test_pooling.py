"""Integration tests for DuckDB connection pooling.

Tests shared memory database conversion and connection pooling functionality.
"""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig

pytestmark = pytest.mark.xdist_group("duckdb")


def test_shared_memory_pooling() -> None:
    """Test that shared memory databases allow pooling."""

    config = DuckDBConfig(connection_config={"database": ":memory:shared_test", "pool_min_size": 2, "pool_max_size": 5})

    assert config.connection_config["pool_min_size"] == 2
    assert config.connection_config["pool_max_size"] == 5

    with config.provide_session() as session1:
        session1.execute("DROP TABLE IF EXISTS shared_test")

        session1.execute_script("""
            CREATE TABLE shared_test (id INTEGER, name TEXT);
            INSERT INTO shared_test VALUES (1, 'test_value');
        """)

    with config.provide_session() as session2:
        result = session2.execute("SELECT name FROM shared_test WHERE id = 1").get_data()
        assert len(result) == 1
        assert result[0]["name"] == "test_value"

        session2.execute("DROP TABLE shared_test")


def test_regular_memory_auto_conversion() -> None:
    """Test that regular memory databases are auto-converted to shared memory with pooling enabled."""

    config = DuckDBConfig(connection_config={"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})

    assert config.connection_config["pool_min_size"] == 5
    assert config.connection_config["pool_max_size"] == 10

    database = config.connection_config["database"]
    assert database == ":memory:shared_db"

    with config.provide_session() as session1:
        session1.execute_script("""
            CREATE TABLE converted_test (id INTEGER, value TEXT);
            INSERT INTO converted_test VALUES (42, 'converted_value');
        """)

    with config.provide_session() as session2:
        result = session2.execute("SELECT value FROM converted_test WHERE id = 42").get_data()
        assert len(result) == 1
        assert result[0]["value"] == "converted_value"

        session2.execute("DROP TABLE converted_test")


def test_file_database_pooling() -> None:
    """Test that file databases work with pooling (no changes needed)."""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp_file:
        db_path = tmp_file.name

    config = DuckDBConfig(connection_config={"database": db_path, "pool_min_size": 2, "pool_max_size": 4})

    assert config.connection_config["pool_min_size"] == 2
    assert config.connection_config["pool_max_size"] == 4

    with config.provide_session() as session1:
        session1.execute("CREATE TABLE file_test (id INTEGER, data TEXT)")
        session1.execute("INSERT INTO file_test VALUES (1, 'file_data')")

    with config.provide_session() as session2:
        result = session2.execute("SELECT data FROM file_test WHERE id = 1").get_data()
        assert len(result) == 1
        assert result[0]["data"] == "file_data"

        session2.execute("DROP TABLE file_test")

    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


def test_connection_pool_health_checks() -> None:
    """Test that the connection pool performs health checks correctly."""
    config = DuckDBConfig(connection_config={"database": ":memory:health_test", "pool_min_size": 1, "pool_max_size": 3})
    pool = config.provide_pool()

    with pool.get_connection() as conn:
        result = conn.execute("SELECT 'health_check'").fetchone()
        assert result is not None
        assert result[0] == "health_check"

    assert pool.size() >= 0


def test_empty_database_conversion() -> None:
    """Test that empty database string gets converted properly."""
    config = DuckDBConfig(connection_config={"database": ""})

    database = config.connection_config["database"]
    assert database.startswith(":memory:")
    assert len(database) == len(":memory:shared_db")

    with config.provide_session() as session:
        result = session.execute("SELECT 'empty_test' as test").get_data()
        assert result[0]["test"] == "empty_test"


def test_default_config_conversion() -> None:
    """Test that default config (no connection_config) works with shared memory."""
    config = DuckDBConfig()

    database = config.connection_config["database"]
    assert database.startswith(":memory:shared_db")
    assert len(database) == len(":memory:shared_db")

    with config.provide_session() as session:
        result = session.execute("SELECT 'default_test' as test").get_data()
        assert result[0]["test"] == "default_test"
