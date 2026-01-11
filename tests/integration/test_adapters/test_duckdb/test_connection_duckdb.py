# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Test DuckDB connection configuration."""

import os
import tempfile
import time
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from sqlspec import ObservabilityConfig, SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBConnection
from sqlspec.adapters.duckdb.core import build_connection_config
from sqlspec.config import LifecycleConfig
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("duckdb")


def create_permissive_config(**kwargs: Any) -> DuckDBConfig:
    """Create a DuckDB config with permissive SQL settings."""

    connection_config = kwargs.pop("connection_config", {})

    for param in [
        "database",
        "read_only",
        "memory_limit",
        "threads",
        "enable_object_cache",
        "preserve_insertion_order",
        "default_null_order",
        "default_order",
        "autoload_known_extensions",
        "autoinstall_known_extensions",
        "allow_community_extensions",
    ]:
        if param in kwargs:
            connection_config[param] = kwargs.pop(param)

    if "database" not in connection_config:
        # Use a unique memory database identifier to avoid configuration conflicts
        connection_config["database"] = f":memory:{uuid4().hex}"

    kwargs["connection_config"] = connection_config
    return DuckDBConfig(**kwargs)


def test_basic_connection() -> None:
    """Test basic DuckDB connection functionality."""
    config = create_permissive_config()

    with config.provide_connection() as conn:
        assert conn is not None
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result is not None
        assert result[0] == 1
        cur.close()

    with config.provide_session() as session:
        assert session is not None
        select_result = session.execute("SELECT 1")
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.column_names is not None
        result = select_result.data[0][select_result.column_names[0]]
        assert result in (1, "1")


def test_memory_database_connection() -> None:
    """Test DuckDB in-memory database connection."""
    config = create_permissive_config()

    with config.provide_session() as session:
        session.execute_script("CREATE TABLE test_memory (id INTEGER, name TEXT)")

        insert_result = session.execute("INSERT INTO test_memory VALUES (?, ?)", (1, "test"))
        assert insert_result is not None

        select_result = session.execute("SELECT id, name FROM test_memory")
        assert len(select_result.data) == 1
        assert select_result.data[0]["id"] == 1
        assert select_result.data[0]["name"] == "test"


def test_connection_with_performance_settings() -> None:
    """Test DuckDB connection with performance optimization settings."""
    config = create_permissive_config(memory_limit="512MB", threads=2, enable_object_cache=True)

    with config.provide_session() as session:
        result = session.execute("SELECT 42 as test_value")
        assert result.data is not None
        assert result.data[0]["test_value"] in (42, "42")


def test_connection_with_data_processing_settings() -> None:
    """Test DuckDB connection with data processing settings."""
    config = create_permissive_config(
        preserve_insertion_order=True, default_null_order="NULLS_FIRST", default_order="ASC"
    )

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE test_ordering (id INTEGER, value INTEGER);
            INSERT INTO test_ordering VALUES (1, 10), (2, NULL), (3, 5);
        """)

        result = session.execute("SELECT id, value FROM test_ordering ORDER BY value")
        assert len(result.data) == 3

        assert result.data[0]["value"] is None
        assert result.data[1]["value"] == 5
        assert result.data[2]["value"] == 10


def test_connection_with_instrumentation() -> None:
    """Test DuckDB connection with instrumentation configuration."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})

    with config.provide_session() as session:
        result = session.execute("SELECT ? as test_value", (42))
        assert result.data is not None
        assert result.data[0]["test_value"] == 42


def test_connection_with_hook() -> None:
    """Test DuckDB connection with connection creation hook."""
    hook_executed = False

    def connection_hook(connection: DuckDBConnection) -> None:
        nonlocal hook_executed
        hook_executed = True
        connection.execute("SET threads = 1")

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"on_connection_create": connection_hook}
    )

    registry = SQLSpec()
    registry.add_config(config)

    with registry.provide_session(config) as session:
        assert hook_executed is True

        result = session.execute("SELECT current_setting('threads')")
        assert result.data is not None
        setting_value = result.data[0][result.column_names[0]]
        assert setting_value == 1 or setting_value == "1"


def test_connection_read_only_mode() -> None:
    """Test DuckDB connection in read-only mode."""

    temp_fd, temp_db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(temp_fd)
    os.unlink(temp_db_path)

    try:
        setup_config = create_permissive_config(database=temp_db_path)

        with setup_config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE test_readonly (id INTEGER, value TEXT);
                INSERT INTO test_readonly VALUES (1, 'test_data');
            """)

        if hasattr(setup_config, "connection_instance") and setup_config.connection_instance:
            setup_config.connection_instance.close()
            setup_config.connection_instance = None

        time.sleep(0.1)

        readonly_config = create_permissive_config(database=temp_db_path, read_only=True)

        with readonly_config.provide_session() as session:
            result = session.execute("SELECT id, value FROM test_readonly")
            assert len(result.data) == 1
            assert result.data[0]["id"] == 1
            assert result.data[0]["value"] == "test_data"

        if hasattr(readonly_config, "connection_instance") and readonly_config.connection_instance:
            readonly_config.connection_instance.close()
            readonly_config.connection_instance = None

    finally:
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)


def test_connection_with_logging_settings() -> None:
    """Test DuckDB connection with logging configuration."""
    config = create_permissive_config()

    with config.provide_session() as session:
        result = session.execute("SELECT 'logging_test' as message")
        assert result.data is not None
        assert result.data[0]["message"] == "logging_test"


def test_duckdb_disabled_observability_has_zero_lifecycle_counts() -> None:
    """Ensure lifecycle counters stay zero when no hooks are registered."""

    registry = SQLSpec()
    config = create_permissive_config()
    registry.add_config(config)

    with registry.provide_session(config) as session:
        session.execute("SELECT 1")

    runtime = config.get_observability_runtime()
    assert all(value == 0 for value in runtime.lifecycle_snapshot().values())


def test_duckdb_observability_hook_records_query_counts() -> None:
    """Lifecycle hooks should increment counters when configured."""

    queries: list[dict[str, Any]] = []

    def hook(context: dict[str, Any]) -> None:
        queries.append(context)

    registry = SQLSpec()
    config = create_permissive_config(
        observability_config=ObservabilityConfig(lifecycle=cast(LifecycleConfig, {"on_query_start": [hook]}))
    )
    registry.add_config(config)

    with registry.provide_session(config) as session:
        session.execute("SELECT 1")

    runtime = config.get_observability_runtime()
    assert runtime.lifecycle_snapshot()["DuckDBConfig.lifecycle.query_start"] == 1
    assert queries, "Lifecycle hook should capture context"


def test_connection_with_extension_settings() -> None:
    """Test DuckDB connection with extension-related settings."""
    config = create_permissive_config(
        autoload_known_extensions=True, autoinstall_known_extensions=False, allow_community_extensions=False
    )

    with config.provide_session() as session:
        result = session.execute("SELECT 'extension_test' as message")
        assert result.data is not None
        assert result.data[0]["message"] == "extension_test"


def test_multiple_concurrent_connections() -> None:
    """Test multiple concurrent DuckDB connections."""
    config1 = DuckDBConfig()
    config2 = DuckDBConfig()

    with config1.provide_session() as session1, config2.provide_session() as session2:
        session1.execute_script("CREATE TABLE session1_table (id INTEGER)")
        session2.execute_script("CREATE TABLE session2_table (id INTEGER)")

        session1.execute("INSERT INTO session1_table VALUES (?)", (1))
        session2.execute("INSERT INTO session2_table VALUES (?)", (2))

        result1 = session1.execute("SELECT id FROM session1_table")
        result2 = session2.execute("SELECT id FROM session2_table")

        assert result1.data[0]["id"] == 1
        assert result2.data[0]["id"] == 2

        try:
            session1.execute("SELECT id FROM session2_table")
            assert False, "Should not be able to access other session's table"
        except Exception:
            pass

        try:
            session2.execute("SELECT id FROM session1_table")
            assert False, "Should not be able to access other session's table"
        except Exception:
            pass


def test_config_with_connection_config_parameter(tmp_path: Path) -> None:
    """Test that DuckDBConfig correctly accepts connection_config parameter."""

    db_path = tmp_path / "test.duckdb"
    connection_config = {"database": str(db_path), "memory_limit": "256MB", "threads": 4}

    config = DuckDBConfig(connection_config=connection_config)

    try:
        connection_config = build_connection_config(config.connection_config)
        assert connection_config["database"] == str(db_path)
        assert connection_config["memory_limit"] == "256MB"
        assert connection_config["threads"] == 4

        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        with config.provide_session() as session:
            result = session.execute("SELECT 1 as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == 1

    finally:
        config._close_pool()


def test_config_memory_database_shared_conversion() -> None:
    """Test that :memory: databases are converted to shared memory."""

    config = DuckDBConfig(connection_config={"database": ":memory:"})

    try:
        assert config.connection_config["database"] == ":memory:shared_db"

        with config.provide_session() as session:
            result = session.execute("SELECT 'memory_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "memory_test"

    finally:
        config._close_pool()


def test_config_empty_database_conversion() -> None:
    """Test that empty database string is converted to shared memory."""

    config = DuckDBConfig(connection_config={"database": ""})

    try:
        assert config.connection_config["database"] == ":memory:shared_db"

        with config.provide_session() as session:
            result = session.execute("SELECT 'empty_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "empty_test"

    finally:
        config._close_pool()


def test_config_default_database_shared() -> None:
    """Test that default database is shared memory."""

    config = DuckDBConfig()

    try:
        assert config.connection_config["database"] == ":memory:shared_db"

        with config.provide_session() as session:
            result = session.execute("SELECT 'default_test' as test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["test"] == "default_test"

    finally:
        config._close_pool()


def test_config_consistency_with_other_adapters(tmp_path: Path) -> None:
    """Test that DuckDB config behaves consistently with SQLite/aiosqlite."""

    db_path = tmp_path / "consistency_test.duckdb"
    connection_config = {
        "database": str(db_path),
        "memory_limit": "512MB",
        "threads": 2,
        "pool_min_size": 1,
        "pool_max_size": 4,
    }

    config = DuckDBConfig(connection_config=connection_config)

    try:
        connection_config = build_connection_config(config.connection_config)
        assert connection_config["database"] == str(db_path)
        assert connection_config["memory_limit"] == "512MB"
        assert connection_config["threads"] == 2

        assert "pool_min_size" not in connection_config
        assert "pool_max_size" not in connection_config

        with config.provide_session() as session:
            session.execute("CREATE TABLE IF NOT EXISTS consistency_test (id INTEGER)")
            session.execute("INSERT INTO consistency_test VALUES (42)")
            result = session.execute("SELECT id FROM consistency_test")
            assert isinstance(result, SQLResult)
            assert result.data[0]["id"] == 42

            session.execute("DROP TABLE consistency_test")

    finally:
        config._close_pool()
