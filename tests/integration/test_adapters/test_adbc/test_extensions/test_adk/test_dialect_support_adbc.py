"""Tests for ADBC ADK store dialect-specific DDL generation."""

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


def test_detect_dialect_postgresql() -> None:
    """Test PostgreSQL dialect detection."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)
    assert store._dialect == "postgresql"  # pyright: ignore[reportPrivateUsage]


def test_detect_dialect_sqlite() -> None:
    """Test SQLite dialect detection."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    assert store._dialect == "sqlite"  # pyright: ignore[reportPrivateUsage]


def test_detect_dialect_duckdb() -> None:
    """Test DuckDB dialect detection."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": ":memory:"})
    store = AdbcADKStore(config)
    assert store._dialect == "duckdb"  # pyright: ignore[reportPrivateUsage]


def test_detect_dialect_snowflake() -> None:
    """Test Snowflake dialect detection."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake", "uri": "snowflake://test"})
    store = AdbcADKStore(config)
    assert store._dialect == "snowflake"  # pyright: ignore[reportPrivateUsage]


def test_detect_dialect_generic_unknown() -> None:
    """Test generic dialect fallback for unknown driver."""
    config = AdbcConfig(connection_config={"driver_name": "unknown_driver", "uri": ":memory:"})
    store = AdbcADKStore(config)
    assert store._dialect == "generic"  # pyright: ignore[reportPrivateUsage]


def test_postgresql_sessions_ddl_contains_jsonb() -> None:
    """Test PostgreSQL DDL uses JSONB type."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_sessions_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in ddl
    assert "TIMESTAMPTZ" in ddl
    assert "'{}'::jsonb" in ddl


def test_sqlite_sessions_ddl_contains_text() -> None:
    """Test SQLite DDL uses TEXT type."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "REAL" in ddl


def test_duckdb_sessions_ddl_contains_json() -> None:
    """Test DuckDB DDL uses JSON type."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_sessions_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "JSON" in ddl
    assert "TIMESTAMP" in ddl


def test_snowflake_sessions_ddl_contains_variant() -> None:
    """Test Snowflake DDL uses VARIANT type."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake", "uri": "snowflake://test"})
    store = AdbcADKStore(config)
    ddl = store._get_sessions_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "VARIANT" in ddl
    assert "TIMESTAMP_TZ" in ddl


def test_generic_sessions_ddl_contains_text() -> None:
    """Test generic DDL uses TEXT type."""
    config = AdbcConfig(connection_config={"driver_name": "unknown", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_sessions_ddl_generic()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "TIMESTAMP" in ddl


def test_postgresql_events_ddl_contains_jsonb() -> None:
    """Test PostgreSQL events DDL uses JSONB for content fields."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_events_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in ddl
    assert "BYTEA" in ddl
    assert "BOOLEAN" in ddl


def test_sqlite_events_ddl_contains_text_and_integer() -> None:
    """Test SQLite events DDL uses TEXT for JSON and INTEGER for booleans."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_events_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "BLOB" in ddl
    assert "INTEGER" in ddl


def test_duckdb_events_ddl_contains_json_and_boolean() -> None:
    """Test DuckDB events DDL uses JSON and BOOLEAN types."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._get_events_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "JSON" in ddl
    assert "BOOLEAN" in ddl


def test_snowflake_events_ddl_contains_variant() -> None:
    """Test Snowflake events DDL uses VARIANT for content."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake", "uri": "snowflake://test"})
    store = AdbcADKStore(config)
    ddl = store._get_events_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "VARIANT" in ddl
    assert "BINARY" in ddl


def test_ddl_dispatch_uses_correct_dialect() -> None:
    """Test that DDL dispatch selects correct dialect method."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)

    sessions_ddl = store._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in sessions_ddl

    events_ddl = store._get_create_events_table_sql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in events_ddl


def test_owner_id_column_included_in_sessions_ddl() -> None:
    """Test owner ID column is included in sessions DDL."""
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._get_sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "tenant_id INTEGER NOT NULL" in ddl


def test_owner_id_column_not_included_when_none() -> None:
    """Test owner ID column is not included when None."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)

    ddl = store._get_sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "tenant_id" not in ddl


def test_owner_id_column_postgresql() -> None:
    """Test owner ID column works with PostgreSQL dialect."""
    config = AdbcConfig(
        connection_config={"driver_name": "postgresql", "uri": ":memory:"},
        extension_config={
            "adk": {"owner_id_column": "organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE"}
        },
    )
    store = AdbcADKStore(config)

    ddl = store._get_sessions_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "organization_id UUID REFERENCES organizations(id)" in ddl


def test_owner_id_column_duckdb() -> None:
    """Test owner ID column works with DuckDB dialect."""
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": "workspace_id VARCHAR(128) NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._get_sessions_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "workspace_id VARCHAR(128) NOT NULL" in ddl


def test_owner_id_column_snowflake() -> None:
    """Test owner ID column works with Snowflake dialect."""
    config = AdbcConfig(
        connection_config={"driver_name": "snowflake", "uri": "snowflake://test"},
        extension_config={"adk": {"owner_id_column": "account_id VARCHAR NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._get_sessions_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "account_id VARCHAR NOT NULL" in ddl
