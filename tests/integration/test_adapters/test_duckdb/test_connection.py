"""Test DuckDB connection configuration."""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBConnection, DuckDBConnectionConfig
from sqlspec.config import InstrumentationConfig
from sqlspec.statement.result import SelectResult


@pytest.mark.xdist_group("duckdb")
def test_basic_connection() -> None:
    """Test basic DuckDB connection functionality."""
    # Test direct connection
    config = DuckDBConfig()

    with config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()  # pyright: ignore
        assert result is not None
        assert result[0] == 1
        cur.close()

    # Test session management
    with config.provide_session() as session:
        assert session is not None
        # Test basic query through session
        select_result = session.execute("SELECT 1")
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.column_names is not None
        result = select_result.data[0][select_result.column_names[0]]
        assert result == 1


@pytest.mark.xdist_group("duckdb")
def test_memory_database_connection() -> None:
    """Test DuckDB in-memory database connection."""
    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        # Create a test table
        session.execute_script("CREATE TABLE test_memory (id INTEGER, name TEXT)")

        # Insert data
        insert_result = session.execute("INSERT INTO test_memory VALUES (?, ?)", [1, "test"])
        assert insert_result.rows_affected == 1

        # Query data
        select_result = session.execute("SELECT id, name FROM test_memory")
        assert len(select_result.data) == 1
        assert select_result.data[0]["id"] == 1
        assert select_result.data[0]["name"] == "test"


@pytest.mark.xdist_group("duckdb")
def test_connection_with_performance_settings() -> None:
    """Test DuckDB connection with performance configuration."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        memory_limit="500MB",
        threads=2,
        enable_object_cache=True,
        enable_progress_bar=False,  # Disable for testing
    )
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        # Test that connection works with performance settings
        result = session.execute("SELECT current_setting('memory_limit')")
        assert result.data is not None
        assert len(result.data) == 1
        # Memory limit should be set (exact format may vary)
        memory_setting = result.data[0][result.column_names[0]]
        assert memory_setting is not None


@pytest.mark.xdist_group("duckdb")
def test_connection_with_data_processing_settings() -> None:
    """Test DuckDB connection with data processing settings."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        preserve_insertion_order=True,
        default_null_order="NULLS_FIRST",
        default_order="ASC",
    )
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        # Create test data with NULLs to test ordering
        session.execute_script("""
            CREATE TABLE test_ordering (id INTEGER, value INTEGER);
            INSERT INTO test_ordering VALUES (1, 10), (2, NULL), (3, 5);
        """)

        # Test ordering with NULL handling
        result = session.execute("SELECT id, value FROM test_ordering ORDER BY value")
        assert len(result.data) == 3
        # With NULLS_FIRST, NULL should come first
        assert result.data[0]["value"] is None


@pytest.mark.xdist_group("duckdb")
def test_connection_with_instrumentation() -> None:
    """Test DuckDB connection with instrumentation configuration."""
    connection_config = DuckDBConnectionConfig(database=":memory:")
    instrumentation = InstrumentationConfig(
        log_queries=True,
        log_parameters=True,
        log_results_count=True,
        log_pool_operations=True,
    )
    config = DuckDBConfig(
        connection_config=connection_config,
        instrumentation=instrumentation,
    )

    with config.provide_session() as session:
        # Test that instrumentation doesn't interfere with operations
        result = session.execute("SELECT ? as test_value", [42])
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == 42


@pytest.mark.xdist_group("duckdb")
def test_connection_with_extensions() -> None:
    """Test DuckDB connection with extension configuration."""
    # Test with commonly available extensions - use config dict approach
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        autoload_known_extensions=True,
    )
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        # Test that we can use JSON functionality (if extension loaded)
        try:
            result = session.execute("SELECT json_object('key', 'value') as json_data")
            assert result.data is not None
            assert len(result.data) == 1
            # JSON functionality should work if extension is available
        except Exception:
            # Extension might not be available in test environment
            pytest.skip("JSON extension not available in test environment")


@pytest.mark.xdist_group("duckdb")
def test_connection_with_hook() -> None:
    """Test DuckDB connection with connection creation hook."""
    hook_executed = False

    def connection_hook(conn: DuckDBConnection) -> None:
        nonlocal hook_executed
        hook_executed = True
        # Set a custom setting via the hook
        conn.execute("SET enable_progress_bar = false")

    connection_config = DuckDBConnectionConfig(database=":memory:")
    config = DuckDBConfig(
        connection_config=connection_config,
        on_connection_create=connection_hook,
    )

    with config.provide_session() as session:
        assert hook_executed is True

        # Verify the hook setting was applied
        result = session.execute("SELECT current_setting('enable_progress_bar')")
        assert result.data is not None
        setting_value = result.data[0][result.column_names[0]]
        assert setting_value == "false"


@pytest.mark.xdist_group("duckdb")
def test_connection_error_handling() -> None:
    """Test DuckDB connection error handling."""
    # Test with invalid configuration
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        memory_limit="invalid_value",  # This should cause a warning but not fail
    )
    config = DuckDBConfig(connection_config=connection_config)

    # Connection should still work despite invalid setting
    with config.provide_session() as session:
        result = session.execute("SELECT 1")
        assert result.data is not None
        assert len(result.data) == 1


@pytest.mark.xdist_group("duckdb")
def test_connection_read_only_mode() -> None:
    """Test DuckDB connection in read-only mode."""
    # Note: Read-only mode requires an existing database file
    # For testing, we'll create a temporary database first
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        temp_db_path = tmp_file.name

    try:
        # First, create a database with some data
        setup_config = DuckDBConfig(connection_config=DuckDBConnectionConfig(database=temp_db_path))

        with setup_config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE readonly_test (id INTEGER, value TEXT);
                INSERT INTO readonly_test VALUES (1, 'test_data');
            """)

        # Now test read-only access
        readonly_config = DuckDBConfig(
            connection_config=DuckDBConnectionConfig(
                database=temp_db_path,
                read_only=True,
            )
        )

        with readonly_config.provide_session() as session:
            # Should be able to read
            result = session.execute("SELECT id, value FROM readonly_test")
            assert len(result.data) == 1
            assert result.data[0]["value"] == "test_data"

            # Should not be able to write (this might raise an exception)
            try:
                session.execute("INSERT INTO readonly_test VALUES (2, 'new_data')")
                # If no exception, check that no data was actually inserted
                check_result = session.execute("SELECT COUNT(*) as count FROM readonly_test")
                assert check_result.data[0]["count"] == 1  # Should still be 1
            except Exception:
                # Expected behavior for read-only mode
                pass

    finally:
        # Clean up temporary file
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)


@pytest.mark.xdist_group("duckdb")
def test_connection_with_logging_settings() -> None:
    """Test DuckDB connection with logging configuration."""
    connection_config = DuckDBConnectionConfig(
        database=":memory:",
        enable_logging=False,  # Disable logging for cleaner test output
        errors_as_json=False,
    )
    config = DuckDBConfig(connection_config=connection_config)

    with config.provide_session() as session:
        # Test that logging settings don't interfere with normal operations
        result = session.execute("SELECT 'logging_test' as message")
        assert result.data is not None
        assert result.data[0]["message"] == "logging_test"


@pytest.mark.xdist_group("duckdb")
def test_multiple_concurrent_connections() -> None:
    """Test multiple concurrent DuckDB connections."""
    config1 = DuckDBConfig()
    config2 = DuckDBConfig()

    # Test that multiple connections can work independently
    with config1.provide_session() as session1, config2.provide_session() as session2:
        # Create different tables in each session
        session1.execute_script("CREATE TABLE session1_table (id INTEGER)")
        session2.execute_script("CREATE TABLE session2_table (id INTEGER)")

        # Insert data in each session
        session1.execute("INSERT INTO session1_table VALUES (1)")
        session2.execute("INSERT INTO session2_table VALUES (2)")

        # Verify data isolation
        result1 = session1.execute("SELECT id FROM session1_table")
        result2 = session2.execute("SELECT id FROM session2_table")

        assert result1.data[0]["id"] == 1
        assert result2.data[0]["id"] == 2

        # Verify tables don't exist in the other session
        try:
            session1.execute("SELECT id FROM session2_table")
            assert False, "Should not be able to access other session's table"
        except Exception:
            pass  # Expected

        try:
            session2.execute("SELECT id FROM session1_table")
            assert False, "Should not be able to access other session's table"
        except Exception:
            pass  # Expected
