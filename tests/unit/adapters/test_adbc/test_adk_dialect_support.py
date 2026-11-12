"""Tests for ADBC ADK store dialect-specific DDL generation."""

import logging
import struct
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from adbc_driver_manager import AdbcStatusCode, IntegrityError, NotSupportedError

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKConfig, AdbcADKMemoryStore, AdbcADKStore
from sqlspec.adapters.adbc.adk.store import (
    _adbc_atomic_connection,
    _oracle_store_sql,
    _postgresql_store_sql,
    decode_adbc_pgvector,
    format_pgvector_literal,
)
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import AdbcStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, TransactionError
from sqlspec.extensions.adk import StoredMemory
from sqlspec.extensions.adk.migrations.reconcile import reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter

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
    ddl = store._sessions_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in ddl
    assert "TIMESTAMPTZ" in ddl
    assert "'{}'::jsonb" in ddl


def test_sqlite_sessions_ddl_contains_text() -> None:
    """Test SQLite DDL uses TEXT type."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "REAL" in ddl


def test_duckdb_sessions_ddl_contains_json() -> None:
    """Test DuckDB DDL uses JSON type."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._sessions_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "JSON" in ddl
    assert "TIMESTAMP" in ddl


def test_snowflake_sessions_ddl_contains_variant() -> None:
    """Test Snowflake DDL uses VARIANT type."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake", "uri": "snowflake://test"})
    store = AdbcADKStore(config)
    ddl = store._sessions_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "VARIANT" in ddl
    assert "TIMESTAMP_TZ" in ddl


def test_generic_sessions_ddl_contains_text() -> None:
    """Test generic DDL uses TEXT type."""
    config = AdbcConfig(connection_config={"driver_name": "unknown", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._sessions_ddl_generic()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "TIMESTAMP" in ddl


def test_postgresql_events_ddl_uses_jsonb() -> None:
    """Test PostgreSQL events DDL uses JSONB for event_data."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._events_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in ddl
    assert "event_data" in ddl
    assert "session_id" in ddl
    assert "invocation_id" in ddl
    assert "timestamp" in ddl.lower()


def test_sqlite_events_ddl_uses_text() -> None:
    """Test SQLite events DDL uses TEXT for event_data."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._events_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "TEXT" in ddl
    assert "event_data" in ddl
    assert "session_id" in ddl
    assert "REAL" in ddl


def test_duckdb_events_ddl_uses_json() -> None:
    """Test DuckDB events DDL uses JSON type for event_data."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": ":memory:"})
    store = AdbcADKStore(config)
    ddl = store._events_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "JSON" in ddl
    assert "event_data" in ddl


def test_snowflake_events_ddl_uses_variant() -> None:
    """Test Snowflake events DDL uses VARIANT for event_data."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake", "uri": "snowflake://test"})
    store = AdbcADKStore(config)
    ddl = store._events_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "VARIANT" in ddl
    assert "event_data" in ddl


def test_ddl_dispatch_uses_correct_dialect() -> None:
    """Test that DDL dispatch selects correct dialect method."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    store = AdbcADKStore(config)

    sessions_ddl = store._sessions_table_ddl()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in sessions_ddl

    events_ddl = store._events_table_ddl()  # pyright: ignore[reportPrivateUsage]
    assert "JSONB" in events_ddl
    assert "event_data" in events_ddl


def test_owner_id_column_included_in_sessions_ddl() -> None:
    """Test owner ID column is included in sessions DDL."""
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
    assert "tenant_id INTEGER NOT NULL" in ddl


def test_owner_id_column_not_included_when_none() -> None:
    """Test owner ID column is not included when None."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)

    ddl = store._sessions_ddl_sqlite()  # pyright: ignore[reportPrivateUsage]
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

    ddl = store._sessions_ddl_postgresql()  # pyright: ignore[reportPrivateUsage]
    assert "organization_id UUID REFERENCES organizations(id)" in ddl


def test_owner_id_column_duckdb() -> None:
    """Test owner ID column works with DuckDB dialect."""
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": "workspace_id VARCHAR(128) NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._sessions_ddl_duckdb()  # pyright: ignore[reportPrivateUsage]
    assert "workspace_id VARCHAR(128) NOT NULL" in ddl


def test_owner_id_column_snowflake() -> None:
    """Test owner ID column works with Snowflake dialect."""
    config = AdbcConfig(
        connection_config={"driver_name": "snowflake", "uri": "snowflake://test"},
        extension_config={"adk": {"owner_id_column": "account_id VARCHAR NOT NULL"}},
    )
    store = AdbcADKStore(config)

    ddl = store._sessions_ddl_snowflake()  # pyright: ignore[reportPrivateUsage]
    assert "account_id VARCHAR NOT NULL" in ddl


@pytest.mark.parametrize("driver_name", ["postgresql", "sqlite", "duckdb", "snowflake"])
def test_limit_offset_dialects_bind_a_row_limited_page(driver_name: str) -> None:
    """PostgreSQL, SQLite, DuckDB, and Snowflake page with bound LIMIT/OFFSET."""
    store = _store_for(driver_name)

    sql, params = store._session_list_query("app", "u1", "create_time", "ASC", 10, 20)  # pyright: ignore[reportPrivateUsage]

    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC LIMIT ? OFFSET ?"
    )
    assert params == ("app", "u1", 10, 20)


def test_generic_dialect_pages_with_standard_offset_fetch() -> None:
    """The generic branch uses SQL:2008 row-limiting with the offset bound first."""
    store = _store_for("unknown_driver")

    sql, params = store._session_list_query("app", None, "update_time", "DESC", 10, 0)  # pyright: ignore[reportPrivateUsage]

    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = ? "
        "ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    )
    assert params == ("app", 0, 10)


@pytest.mark.parametrize("driver_name", ["postgresql", "sqlite", "duckdb", "snowflake", "unknown_driver"])
def test_every_dialect_omits_pagination_for_an_unbounded_listing(driver_name: str) -> None:
    """Without a limit no dialect emits a row-limiting clause."""
    store = _store_for(driver_name)

    sql, params = store._session_list_query("app", None, "update_time", "DESC", None, 0)  # pyright: ignore[reportPrivateUsage]

    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_list_sessions_zero_limit_returns_empty_without_a_query() -> None:
    """A zero limit short-circuits before any database work."""
    store = _store_for("sqlite")

    assert store.list_sessions("app", limit=0) == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, object]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store = _store_for("sqlite")

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)  # type: ignore[arg-type]


def test_memory_store_detects_postgresql_from_uri_and_formats_placeholders() -> None:
    """AdbcADKMemoryStore resolves dialect from URI and formats PostgreSQL placeholders."""
    config = AdbcConfig(connection_config={"uri": "postgresql://localhost/testdb"})
    store = AdbcADKMemoryStore(config)

    assert store.dialect == "postgresql"
    formatted = store._format_sql('DELETE FROM "memory?" WHERE session_id = ? AND app_name = ?')
    assert formatted == 'DELETE FROM "memory?" WHERE session_id = $1 AND app_name = $2'
    assert store._json_placeholder() == "?::jsonb"


@pytest.mark.parametrize("store_type", [AdbcADKStore, AdbcADKMemoryStore])
@pytest.mark.parametrize("driver_name", ["postgresql", "sqlite"])
def test_store_placeholder_formatting_preserves_sql_text(store_type: type, driver_name: str) -> None:
    config = AdbcConfig(connection_config={"driver_name": driver_name})
    store = store_type(config)
    sql = 'SELECT \'?\', "field?", $$?$$, $tag$?$tag$ FROM "table?" /* ? */ WHERE id = ?::int -- ?\nAND name = ?'
    expected = sql.replace("id = ?::int", "id = $1::int").replace("name = ?", "name = $2")
    assert store._format_sql(sql) == (expected if driver_name == "postgresql" else sql)


def test_store_placeholder_formatting_reuses_cached_sql() -> None:
    _postgresql_store_sql.cache_clear()
    sql = "SELECT ?::jsonb /* cached store query */"
    assert _postgresql_store_sql(sql) == "SELECT $1::jsonb /* cached store query */"
    assert _postgresql_store_sql(sql) == "SELECT $1::jsonb /* cached store query */"
    assert _postgresql_store_sql.cache_info().hits == 1


def test_detect_dialect_bigquery_and_ddl() -> None:
    """AdbcADKStore and AdbcADKMemoryStore should detect BigQuery and generate BigQuery DDL."""
    config = AdbcConfig(connection_config={"driver_name": "bigquery"})
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)

    assert store._dialect == "bigquery"  # pyright: ignore[reportPrivateUsage]
    assert mem_store.dialect == "bigquery"
    assert "STRING" in store._sessions_table_ddl()  # pyright: ignore[reportPrivateUsage]
    assert "JSON" in store._events_table_ddl()  # pyright: ignore[reportPrivateUsage]
    assert "STRING" in mem_store._memory_table_ddl()  # pyright: ignore[reportPrivateUsage]


def test_sqlite_adk_store_encodes_and_decodes_timestamps() -> None:
    """AdbcADKStore on SQLite encodes timestamps as floats and decodes them to UTC datetimes."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)

    encoded = store._encode_timestamp(now)  # pyright: ignore[reportPrivateUsage]
    assert isinstance(encoded, float)
    decoded = store._decode_timestamp(encoded)  # pyright: ignore[reportPrivateUsage]
    assert decoded == now


def test_adbc_litestar_store_handles_pg_extensions_and_bigquery_snowflake(monkeypatch: Any) -> None:
    """AdbcStore should recognize Postgres extension dialects, omit indexes on BigQuery/Snowflake, and use select_one_or_none."""
    config = AdbcConfig(connection_config={"driver_name": "bigquery"})
    store = AdbcStore(config)
    store._dialect = "bigquery"  # pyright: ignore[reportPrivateUsage]

    ddl = store._table_ddl()  # pyright: ignore[reportPrivateUsage]
    assert "CREATE INDEX" not in ddl
    assert store._drop_table_sql() == [f"DROP TABLE IF EXISTS {store._table_name}"]  # pyright: ignore[reportPrivateUsage]

    store._dialect = "pgvector"  # pyright: ignore[reportPrivateUsage]
    assert store._get_param_placeholder(1) == "$1"  # pyright: ignore[reportPrivateUsage]
    assert "BYTEA" in store._table_ddl()  # pyright: ignore[reportPrivateUsage]

    mock_driver = MagicMock()
    mock_driver.select_one_or_none.return_value = None
    mock_session = MagicMock()
    mock_session.__enter__.return_value = mock_driver
    mock_session.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_session", lambda *_a, **_k: mock_session)

    assert store._expires_in("missing") is None  # pyright: ignore[reportPrivateUsage]
    mock_driver.select_one_or_none.assert_called_once()


def test_adbc_events_store_delegates_dialect_resolution() -> None:
    """AdbcEventQueueStore should resolve dialects via resolve_dialect_from_config and preserve flightsql."""
    bq_config = AdbcConfig(connection_config={"driver_name": "bigquery"}, extension_config={"events": {}})
    assert AdbcEventQueueStore(bq_config).dialect == "bigquery"

    gizmo_sqlite_config = AdbcConfig(
        connection_config={"uri": "grpc+tls://localhost:31337", "gizmosql_backend": "sqlite"},
        extension_config={"events": {}},
    )
    assert AdbcEventQueueStore(gizmo_sqlite_config).dialect == "sqlite"

    so_config = AdbcConfig(
        connection_config={"driver_name": "/opt/lib/libadbc_driver_postgresql.so"}, extension_config={"events": {}}
    )
    assert AdbcEventQueueStore(so_config).dialect == "postgres"

    flight_config = AdbcConfig(
        connection_config={"driver_name": "flightsql", "uri": "grpc://localhost:8815"}, extension_config={"events": {}}
    )
    assert AdbcEventQueueStore(flight_config).dialect == "flightsql"


@pytest.mark.parametrize(
    ("driver_name", "expected_dialect"),
    [
        ("oracle", "oracle"),
        ("oracledb", "oracle"),
        ("db2", "db2"),
        ("ibmdb2", "db2"),
        ("mssql", "mssql"),
        ("sqlserver", "mssql"),
        ("tsql", "mssql"),
    ],
)
def test_detect_enterprise_dialects_in_adk_stores(driver_name: str, expected_dialect: str) -> None:
    """AdbcADKStore and AdbcADKMemoryStore detect oracle, db2, and mssql dialects."""
    config = AdbcConfig(connection_config={"driver_name": driver_name})
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)
    assert store.dialect == expected_dialect
    assert mem_store.dialect == expected_dialect


def test_detect_dialect_fallback_to_statement_config() -> None:
    """AdbcADKStore and AdbcADKMemoryStore fall back to statement_config.dialect when connection_config has no driver."""
    config = AdbcConfig(connection_config={}, statement_config=StatementConfig(dialect="oracle"))
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)
    assert store.dialect == "oracle"
    assert mem_store.dialect == "oracle"


def test_oracle_store_sql_rewrites_qmark_and_preserves_literals_and_comments() -> None:
    """_oracle_store_sql rewrites unquoted ? to :1, :2 while preserving literals and comments."""
    _oracle_store_sql.cache_clear()
    sql = "SELECT '?', \"col?\" FROM adk_session /* ? */ WHERE app_name = ? -- ?\nAND user_id = ?"
    rewritten = _oracle_store_sql(sql)
    assert rewritten == "SELECT '?', \"col?\" FROM adk_session /* ? */ WHERE app_name = :1 -- ?\nAND user_id = :2"
    assert _oracle_store_sql(sql) == rewritten
    assert _oracle_store_sql.cache_info().hits == 1

    config = AdbcConfig(connection_config={"driver_name": "oracle"})
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)
    assert getattr(store, "_format_sql")("SELECT * FROM t WHERE a = ? AND b = ?") == (
        "SELECT * FROM t WHERE a = :1 AND b = :2"
    )
    assert getattr(mem_store, "_format_sql")("SELECT * FROM t WHERE a = ? AND b = ?") == (
        "SELECT * FROM t WHERE a = :1 AND b = :2"
    )


def test_enterprise_adk_ddl_generation() -> None:
    """AdbcADKStore and AdbcADKMemoryStore emit dialect-specific DDL for oracle, db2, and mssql."""
    oracle_store = AdbcADKStore(AdbcConfig(connection_config={"driver_name": "oracle"}))
    oracle_mem = AdbcADKMemoryStore(AdbcConfig(connection_config={"driver_name": "oracle"}))
    assert "EXECUTE IMMEDIATE" in getattr(oracle_store, "_sessions_table_ddl")()
    assert "VARCHAR2(128)" in getattr(oracle_store, "_sessions_table_ddl")()
    assert "CLOB" in getattr(oracle_store, "_events_table_ddl")()
    assert "TIMESTAMP WITH TIME ZONE" in getattr(oracle_store, "_app_states_table_ddl")()
    assert "EXECUTE IMMEDIATE" in getattr(oracle_mem, "_memory_table_ddl")()
    assert "SQLCODE != -942" in getattr(oracle_store, "_drop_tables_sql")()[0]

    db2_store = AdbcADKStore(AdbcConfig(connection_config={"driver_name": "db2"}))
    db2_mem = AdbcADKMemoryStore(AdbcConfig(connection_config={"driver_name": "db2"}))
    assert "CLOB(1M)" in getattr(db2_store, "_sessions_table_ddl")()
    assert "WITH DEFAULT CURRENT TIMESTAMP" in getattr(db2_store, "_sessions_table_ddl")()
    assert "CLOB(1M)" in getattr(db2_store, "_events_table_ddl")()
    assert '"KEY"' in getattr(db2_store, "_metadata_table_ddl")()
    assert "CLOB(1M)" in getattr(db2_mem, "_memory_table_ddl")()

    mssql_store = AdbcADKStore(AdbcConfig(connection_config={"driver_name": "mssql"}))
    mssql_mem = AdbcADKMemoryStore(AdbcConfig(connection_config={"driver_name": "mssql"}))
    assert "IF OBJECT_ID" in getattr(mssql_store, "_sessions_table_ddl")()
    assert "NVARCHAR(MAX)" in getattr(mssql_store, "_sessions_table_ddl")()
    assert "SYSUTCDATETIME()" in getattr(mssql_store, "_sessions_table_ddl")()
    assert "[key]" in getattr(mssql_store, "_metadata_table_ddl")()
    assert "IF OBJECT_ID" in getattr(mssql_mem, "_memory_table_ddl")()


@pytest.mark.parametrize("driver_name", ["oracle", "db2", "mssql"])
def test_enterprise_dialects_page_sessions_with_offset_fetch(driver_name: str) -> None:
    """Oracle, Db2, and MSSQL use OFFSET ? ROWS FETCH NEXT ? ROWS ONLY with offset bound first."""
    store = _store_for(driver_name)
    sql, params = getattr(store, "_session_list_query")("app", "u1", "update_time", "DESC", 15, 5)
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = ? AND user_id = ? "
        "ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    )
    assert params == ("app", "u1", 5, 15)


@pytest.mark.parametrize("driver_name", ["oracle", "db2", "mssql"])
def test_enterprise_dialects_create_tables_uses_safe_index_creation(driver_name: str, monkeypatch: Any) -> None:
    """_create_tables never emits CREATE INDEX IF NOT EXISTS for oracle, db2, or mssql."""
    executed_sql: list[str] = []
    mock_cursor = MagicMock()

    def _capture_execute(sql: str, params: Any = None) -> None:
        executed_sql.append(_normalized(sql))

    mock_cursor.execute.side_effect = _capture_execute
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": driver_name}, extension_config={"adk": {"enable_memory": True}}
    )
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)
    store.create_tables()
    mem_store.create_tables()

    assert executed_sql
    assert all("CREATE INDEX IF NOT EXISTS" not in stmt for stmt in executed_sql)
    if driver_name == "oracle":
        assert any("SQLCODE NOT IN (-955, -1408)" in stmt for stmt in executed_sql)
    elif driver_name == "db2":
        assert any("SYSCAT.INDEXES" in stmt for stmt in executed_sql)
    elif driver_name == "mssql":
        assert any("sys.indexes" in stmt for stmt in executed_sql)


@pytest.mark.parametrize("driver_name", ["oracle", "db2", "mssql"])
def test_enterprise_memory_search_entries_uses_dialect_grammar(driver_name: str, monkeypatch: Any) -> None:
    """AdbcADKMemoryStore._search_entries uses POSSTR for Db2 and OFFSET/FETCH for enterprise dialects."""
    captured: list[tuple[str, Any]] = []
    mock_cursor = MagicMock()

    def _capture_execute(sql: str, params: Any = None) -> None:
        captured.append((_normalized(sql), params))

    mock_cursor.execute.side_effect = _capture_execute
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": driver_name}, extension_config={"adk": {"enable_memory": True}}
    )
    mem_store = AdbcADKMemoryStore(config)
    mem_store.search_entries("needle", "app1", "user1", limit=5)

    assert len(captured) == 1
    sql, params = captured[0]
    assert "LIMIT" not in sql
    if driver_name == "db2":
        assert "POSSTR(LOWER(content_text), LOWER(?)) > 0" in sql
        assert "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY" in sql
        assert params == ("app1", "user1", "needle", 5)
    elif driver_name == "oracle":
        assert "content_text LIKE :3" in sql
        assert "OFFSET 0 ROWS FETCH NEXT :4 ROWS ONLY" in sql
        assert params == ("app1", "user1", "%needle%", 5)
    elif driver_name == "mssql":
        assert "content_text LIKE ?" in sql
        assert "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY" in sql
        assert params == ("app1", "user1", "%needle%", 5)


def test_adbc_adk_config_accepts_vector_and_bm25_options() -> None:
    """AdbcADKConfig validates vector, BM25, and ScaNN configuration keys."""
    adk_cfg: AdbcADKConfig = {
        "enable_memory": True,
        "memory_use_fts": True,
        "enable_bm25": True,
        "vector_dimensions": 3,
        "vector_index_type": "scann",
        "scann_num_leaves": 64,
        "scann_quantizer": "SQ8",
    }
    config = AdbcConfig(connection_config={"driver_name": "postgresql"}, extension_config={"adk": adk_cfg})
    mem_store = AdbcADKMemoryStore(config)
    assert mem_store._enable_bm25 is True
    assert mem_store._vector_dimensions == 3
    assert mem_store._vector_index_type == "scann"
    assert mem_store._scann_num_leaves == 64
    assert mem_store._scann_quantizer == "SQ8"


def test_format_pgvector_literal_and_decode_adbc_pgvector() -> None:
    """format_pgvector_literal and decode_adbc_pgvector round-trip text and binary pgvector values."""
    assert format_pgvector_literal([0.1, 0.25, -0.5]) == "[0.1,0.25,-0.5]"
    assert format_pgvector_literal((1.0, 2.0)) == "[1.0,2.0]"

    assert decode_adbc_pgvector(None) is None
    assert decode_adbc_pgvector("") == []
    assert decode_adbc_pgvector("[]") == []
    assert decode_adbc_pgvector("[0.1, 0.25, -0.5]") == [0.1, 0.25, -0.5]
    assert decode_adbc_pgvector(b"[0.5, -1.5]") == [0.5, -1.5]
    assert decode_adbc_pgvector((0.25, 0.75)) == [0.25, 0.75]

    binary_payload = struct.pack(">hh3f", 3, 0, 0.5, -0.25, 1.25)
    assert decode_adbc_pgvector(binary_payload) == [0.5, -0.25, 1.25]


def test_postgresql_adbc_memory_ddl_and_vector_indexes(monkeypatch: Any) -> None:
    """PostgreSQL AdbcADKMemoryStore emits VECTOR(N) DDL and vector/BM25/FTS indexes when extensions are enabled."""
    executed_sql: list[str] = []
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = lambda sql, params=None: executed_sql.append(_normalized(sql))
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": "postgresql"},
        extension_config={
            "adk": {
                "enable_memory": True,
                "memory_use_fts": True,
                "enable_bm25": True,
                "vector_dimensions": 3,
                "vector_index_type": "scann",
                "scann_num_leaves": 50,
                "scann_quantizer": "SQ8",
            }
        },
    )
    config._pgvector_available = True
    config._pg_textsearch_available = True

    mem_store = AdbcADKMemoryStore(config)
    ddl = getattr(mem_store, "_memory_table_ddl")()
    assert "embedding VECTOR(3)" in ddl

    mem_store.create_tables()
    assert any("USING GIN (to_tsvector('english', content_text))" in stmt for stmt in executed_sql)
    assert any("USING bm25 (content_text) WITH (text_config='english')" in stmt for stmt in executed_sql)
    assert any("USING scann (embedding) WITH (num_leaves = 50, quantizer = 'SQ8')" in stmt for stmt in executed_sql)


def test_postgresql_adbc_memory_insert_and_vector_hybrid_search(monkeypatch: Any) -> None:
    """PostgreSQL AdbcADKMemoryStore binds $N::text::vector on insert/search and decodes 13-column rows."""
    captured: list[tuple[str, Any]] = []
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    sample_row = (
        "mem-1",
        "sess-1",
        "app1",
        "user1",
        "user",
        "evt-1",
        "user",
        now,
        '{"parts": [{"text": "hello"}]}',
        "hello",
        '{"topic": "greeting"}',
        now,
        "[0.1,0.2,0.3]",
    )
    mock_cursor = MagicMock()

    def _capture_execute(sql: str, params: Any = None) -> None:
        captured.append((_normalized(sql), params))

    mock_cursor.execute.side_effect = _capture_execute
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.return_value = [sample_row]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": "postgresql"},
        extension_config={
            "adk": {"enable_memory": True, "memory_use_fts": True, "enable_bm25": True, "vector_dimensions": 3}
        },
    )
    config._pgvector_available = True
    config._pg_textsearch_available = True

    mem_store = AdbcADKMemoryStore(config)
    inserted = mem_store.insert_memory_entries([
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app1",
            "user_id": "user1",
            "scope": "user",
            "event_id": "evt-1",
            "author": "user",
            "timestamp": now,
            "embedding": (0.1, 0.2, 0.3),
            "content_json": {"parts": [{"text": "hello"}]},
            "content_text": "hello",
            "metadata_json": {"topic": "greeting"},
            "inserted_at": now,
        }
    ])
    assert inserted == 1
    insert_sql, insert_params = captured[-1]
    assert "$9::text::vector" in insert_sql
    assert insert_params[8] == "[0.1,0.2,0.3]"

    captured.clear()
    vec_rows = mem_store.search_entries("", "app1", "user1", limit=5, embedding=(0.1, 0.2, 0.3))
    assert len(vec_rows) == 1
    assert vec_rows[0]["embedding"] == [0.1, 0.2, 0.3]
    vec_sql, vec_params = captured[0]
    assert "embedding::text AS embedding" in vec_sql
    assert "embedding <=> $3::text::vector ASC" in vec_sql
    assert vec_params == ("app1", "user1", "[0.1,0.2,0.3]", 5)

    captured.clear()
    hybrid_rows = mem_store.search_entries("hello", "app1", "user1", limit=5, embedding=[0.1, 0.2, 0.3])
    assert len(hybrid_rows) == 1
    assert hybrid_rows[0]["embedding"] == [0.1, 0.2, 0.3]
    hybrid_sql, hybrid_params = captured[0]
    assert "WITH vector_matches AS" in hybrid_sql
    assert "m.embedding::text AS embedding" in hybrid_sql
    assert "embedding <=> $1::text::vector" in hybrid_sql
    assert "content_text <@> $5" in hybrid_sql
    assert hybrid_params == ("[0.1,0.2,0.3]", "app1", "user1", 50, "hello", "app1", "user1", 50, 5)


@pytest.mark.parametrize("driver_name", ["sqlite", "duckdb"])
def test_non_postgresql_adbc_memory_rejects_vector_operations(driver_name: str, monkeypatch: Any) -> None:
    """Non-PostgreSQL AdbcADKMemoryStore raises ImproperConfigurationError when embeddings are supplied."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": driver_name, "uri": ":memory:"},
        extension_config={"adk": {"enable_memory": True}},
    )
    mem_store = AdbcADKMemoryStore(config)
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)

    with pytest.raises(ImproperConfigurationError, match="Vector embeddings in AdbcADKMemoryStore are only supported"):
        mem_store.search_entries("query", "app1", "user1", embedding=[0.1, 0.2])

    with pytest.raises(ImproperConfigurationError, match="Vector embeddings in AdbcADKMemoryStore are only supported"):
        mem_store.insert_memory_entries([
            {
                "id": "mem-1",
                "session_id": "sess-1",
                "app_name": "app1",
                "user_id": "user1",
                "scope": "user",
                "event_id": "evt-1",
                "author": "user",
                "timestamp": now,
                "embedding": [0.1, 0.2],
                "content_json": {"parts": [{"text": "hello"}]},
                "content_text": "hello",
                "metadata_json": None,
                "inserted_at": now,
            }
        ])


def _store_for(driver_name: str) -> AdbcADKStore:
    return AdbcADKStore(AdbcConfig(connection_config={"driver_name": driver_name, "uri": ":memory:"}))


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def test_oracle_adk_memory_store_pre_queries_existing_event_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """AdbcADKMemoryStore pre-queries existing event_ids on non-returning dialects."""
    executed_statements: list[str] = []
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("evt-existing",)]

    def record_execute(sql: str, _params: Any = None) -> None:
        executed_statements.append(sql)

    mock_cursor.execute.side_effect = record_execute
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_conn
    mock_ctx.__exit__.return_value = None
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: mock_ctx)

    config = AdbcConfig(
        connection_config={"driver_name": "oracle", "uri": ":memory:"},
        extension_config={"adk": {"enable_memory": True}},
    )
    mem_store = AdbcADKMemoryStore(config)
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)

    count = mem_store.insert_memory_entries([
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app1",
            "user_id": "user1",
            "scope": "user",
            "event_id": "evt-existing",
            "author": "user",
            "timestamp": now,
            "content_json": {"parts": [{"text": "hello"}]},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        },
        {
            "id": "mem-2",
            "session_id": "sess-1",
            "app_name": "app1",
            "user_id": "user1",
            "scope": "user",
            "event_id": "evt-new",
            "author": "user",
            "timestamp": now,
            "content_json": {"parts": [{"text": "world"}]},
            "content_text": "world",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        },
    ])

    assert any("SELECT event_id FROM" in s for s in executed_statements)
    assert count == 1


@pytest.mark.parametrize("driver_name", ["oracle", "db2"])
def test_adbc_memory_batch_deduplicates_fresh_event_ids(driver_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated fresh event IDs keep the first entry and preserve the rest of the batch."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = conn
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: context)
    store = AdbcADKMemoryStore(AdbcConfig(connection_config={"driver_name": driver_name}))
    now = datetime.now(UTC)
    entry: StoredMemory = {
        "id": "mem-1",
        "session_id": "sess-1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "evt-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }

    count = store.insert_memory_entries([
        entry,
        {**entry, "id": "mem-duplicate"},
        {**entry, "id": "mem-2", "event_id": "evt-2"},
    ])

    inserts = [call.args[1] for call in cursor.execute.call_args_list if "INSERT INTO" in call.args[0]]
    assert count == 2
    assert [params[0] for params in inserts] == ["mem-1", "mem-2"]
    conn.commit.assert_called_once()


@pytest.mark.parametrize("driver_name", ["oracle", "db2"])
@pytest.mark.parametrize(
    ("error_message", "event_exists"),
    [
        ("ORA-00001: unique constraint violated", True),
        ("SQL0803N duplicate key", True),
        ("ORA-00001: unique constraint violated", False),
        ("foreign key constraint violated", True),
    ],
)
def test_adbc_memory_insert_only_ignores_confirmed_event_conflicts(
    driver_name: str, error_message: str, event_exists: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate race is idempotent while other integrity errors still roll back."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = ("evt-1",) if event_exists else None
    error = IntegrityError(error_message, status_code=AdbcStatusCode.INTEGRITY)

    def execute(sql: str, _params: object = None) -> None:
        if "INSERT INTO" in sql:
            raise error

    cursor.execute.side_effect = execute
    conn = MagicMock()
    conn.cursor.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = conn
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: context)
    store = AdbcADKMemoryStore(AdbcConfig(connection_config={"driver_name": driver_name}))
    now = datetime.now(UTC)
    entry: StoredMemory = {
        "id": "mem-1",
        "session_id": "sess-1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "evt-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }

    if event_exists and "foreign key" not in error_message:
        assert store.insert_memory_entries([entry]) == 0
        conn.commit.assert_called_once()
        conflict_checks = [call.args[1] for call in cursor.execute.call_args_list if "WHERE event_id =" in call.args[0]]
        assert conflict_checks == [("evt-1",)]
    else:
        with pytest.raises(IntegrityError) as exc_info:
            store.insert_memory_entries([entry])
        assert exc_info.value is error
        conn.rollback.assert_called_once()


def test_adbc_sqlite_fts_statements_match_sqlite_store() -> None:
    """Every normalized ADBC SQLite FTS5 statement appears in SqliteADKMemoryStore DDL."""
    adbc_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )
    sqlite_store = SqliteADKMemoryStore(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )

    fts_statements: dict[str, str] = getattr(adbc_store, "_memory_fts_statements")()
    assert list(fts_statements) == ["adk_memory_fts", "adk_memory_ai", "adk_memory_ad", "adk_memory_au"]

    sqlite_ddl = _normalized(getattr(sqlite_store, "_memory_table_ddl")())
    for stmt in fts_statements.values():
        assert _normalized(stmt) in sqlite_ddl

    non_fts_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": False}},
        )
    )
    assert getattr(non_fts_store, "_memory_fts_statements")() == {}


def test_adbc_flightsql_memory_ddl_has_no_fts() -> None:
    """Remote FlightSQL/GizmoSQL endpoints keep FTS5 disabled even when dialect resolves to sqlite."""
    store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={
                "driver_name": "adbc_driver_flightsql",
                "uri": "grpc://localhost:8815",
                "gizmosql_backend": "sqlite",
            },
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )

    ddl = getattr(store, "_memory_table_ddl")()
    assert isinstance(ddl, str)
    assert "fts5" not in ddl.lower()
    assert getattr(store, "_memory_fts_statements")() == {}
    assert getattr(store, "_drop_memory_table_sql")() == ["DROP TABLE IF EXISTS adk_memory"]


def test_adbc_sqlite_memory_ddl_lists_fts_statements() -> None:
    """Local SQLite with memory_use_fts=True emits base table DDL followed by four FTS5 statements."""
    store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )

    ddl = getattr(store, "_memory_table_ddl")()
    assert isinstance(ddl, list)
    assert len(ddl) == 5
    assert ddl[0].lstrip().startswith("CREATE TABLE")
    assert getattr(store, "_memory_fts_rebuild_statement")() == (
        "INSERT INTO adk_memory_fts(adk_memory_fts) VALUES('rebuild')"
    )


def test_adbc_sqlite_drop_includes_fts_objects() -> None:
    """Local SQLite with memory_use_fts=True drops FTS table and triggers alongside the memory table."""
    store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )

    drops = getattr(store, "_drop_memory_table_sql")()
    assert drops == [
        "DROP TABLE IF EXISTS adk_memory_fts",
        "DROP TABLE IF EXISTS adk_memory",
        "DROP TRIGGER IF EXISTS adk_memory_ai",
        "DROP TRIGGER IF EXISTS adk_memory_ad",
        "DROP TRIGGER IF EXISTS adk_memory_au",
    ]


def test_adbc_sqlite_fts_create_tables_skips_catalog_when_disabled_and_aborts_triggers_on_missing_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disabled FTS skips sqlite_master queries and missing fts5 module aborts before trigger DDL."""
    executed_sql: list[str] = []
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None

    def record_execute(sql: str, _params: object = None) -> None:
        executed_sql.append(_normalized(sql))
        if "USING fts5" in sql:
            msg = "no such module: fts5"
            raise RuntimeError(msg)

    cursor.execute.side_effect = record_execute
    conn = MagicMock()
    conn.cursor.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = conn
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: context)

    non_fts_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": False}},
        )
    )
    non_fts_store.create_tables()
    assert all("sqlite_master" not in stmt for stmt in executed_sql)

    executed_sql.clear()
    fts_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )
    with pytest.raises(RuntimeError, match="no such module: fts5"):
        fts_store.create_tables()
    assert all("CREATE TRIGGER" not in stmt for stmt in executed_sql)


def test_adbc_flightsql_search_keeps_like(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Remote FlightSQL endpoints keep LIKE search and emit the unsupported-FTS warning."""
    captured: list[tuple[str, Any]] = []
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.execute.side_effect = lambda sql, params=None: captured.append((_normalized(sql), params))
    conn = MagicMock()
    conn.cursor.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = conn
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda *_a, **_k: context)

    store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={
                "driver_name": "adbc_driver_flightsql",
                "uri": "grpc://localhost:8815",
                "gizmosql_backend": "sqlite",
            },
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )

    with caplog.at_level(logging.WARNING, logger="sqlspec.adapters.adbc.adk.store"):
        assert store.search_entries("espresso", "app", "user", limit=5) == []

    assert len(captured) == 1
    sql, params = captured[0]
    assert "LIKE ?" in sql
    assert "MATCH" not in sql
    assert params == ("app", "user", "%espresso%", 5)
    assert any("ADBC memory store does not support FTS" in rec.message for rec in caplog.records)


def test_adbc_reconcile_accepts_shared_object_sqlite() -> None:
    """Shared-object SQLite driver configs pass reconcile adapter validation and companion hooks."""
    config = AdbcConfig(
        connection_config={"driver_name": "/opt/libadbc_driver_sqlite.so"},
        statement_config=StatementConfig(dialect="sqlite"),
        extension_config={"adk": {"enable_memory": False}},
    )
    assert validate_builtin_reconcile_adapter(config) == "adbc"

    session_store = AdbcADKStore(config)
    driver = MagicMock()
    session_store.reconcile_companion_objects_sync(driver)
    executed = [_normalized(str(call.args[0])) for call in driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX IF NOT EXISTS idx_adk_session_app_user ON adk_session(app_name, user_id)",
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time ON adk_session(update_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_adk_event_session ON adk_event(session_id, timestamp ASC)",
    ]
    assert driver.commit.call_count == 0

    memory_store = AdbcADKMemoryStore(config)
    memory_driver = MagicMock()
    memory_store.reconcile_companion_objects_sync(memory_driver)
    assert memory_driver.execute.call_count == 0
    assert memory_driver.select.call_count == 0


@pytest.mark.parametrize("driver_name", ["adbc_driver_flightsql", "flightsql", "grpc"])
def test_adbc_reconcile_refuses_flightsql_before_store_hooks(driver_name: str) -> None:
    """ADK reconcile callback rejects unclassified FlightSQL/gRPC configs before touching the driver."""
    config = AdbcConfig(
        connection_config={"driver_name": driver_name, "uri": "grpc://localhost:8815"},
        extension_config={"adk": {"enable_memory": True, "migration_mode": "reconcile"}},
    )
    assert AdbcADKStore(config).dialect == "sqlite"

    driver = MagicMock()
    context = MigrationContext.from_config(config)
    context.driver = driver
    with pytest.raises(MigrationError, match='migration_mode="versioned"'):
        reconcile(context)
    assert driver.execute.call_count == 0


@pytest.mark.parametrize("driver_name", ["adbc_driver_postgresql", "adbc_driver_flightsql"])
def test_adbc_versioned_preparation_keeps_non_sqlite_dialect(driver_name: str) -> None:
    """Versioned prepare_schema_sync remains a no-op across non-SQLite and FlightSQL ADBC drivers."""
    config = AdbcConfig(
        connection_config={"driver_name": driver_name}, extension_config={"adk": {"enable_memory": True}}
    )
    AdbcADKStore(config).prepare_schema_sync(MagicMock())
    AdbcADKMemoryStore(config).prepare_schema_sync(MagicMock())


def test_adbc_memory_companion_hook_skips_catalog_when_fts_disabled_and_propagates_fts5_error() -> None:
    """Memory companion hook skips sqlite_master when FTS is off and propagates missing fts5 errors."""
    non_fts_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": False}},
        )
    )
    expected_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_adk_memory_app_scope_user_time ON adk_memory(app_name, scope, user_id, timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_adk_memory_scope ON adk_memory(app_name, scope)",
        "CREATE INDEX IF NOT EXISTS idx_adk_memory_session ON adk_memory(session_id)",
    ]
    assert getattr(non_fts_store, "_memory_index_statements")() == expected_indexes

    non_fts_driver = MagicMock()
    non_fts_store.reconcile_companion_objects_sync(non_fts_driver)
    assert [str(call.args[0]) for call in non_fts_driver.execute.call_args_list] == expected_indexes
    assert non_fts_driver.select.call_count == 0
    assert non_fts_driver.commit.call_count == 0

    fts_store = AdbcADKMemoryStore(
        AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": ":memory:"},
            extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
        )
    )
    fts_driver = MagicMock()
    fts_driver.select.return_value = []

    def fail_on_fts(stmt: str) -> None:
        if "USING fts5" in stmt:
            msg = "no such module: fts5"
            raise RuntimeError(msg)

    fts_driver.execute.side_effect = fail_on_fts
    with pytest.raises(RuntimeError, match="no such module: fts5"):
        fts_store.reconcile_companion_objects_sync(fts_driver)
    assert all("CREATE TRIGGER" not in str(call.args[0]) for call in fts_driver.execute.call_args_list)
    assert fts_driver.commit.call_count == 0


def test_teradata_adk_schema_targets() -> None:
    config = AdbcConfig(connection_config={"uri": "teradata://user:pass@host/database"})
    store = AdbcADKStore(config)
    targets = store.get_schema_targets_sync(None)
    assert len(targets) == 5
    assert all(str(target.create_statement).startswith("CREATE MULTISET TABLE") for target in targets)


@pytest.mark.parametrize(
    "operation",
    [
        "upsert_app_state",
        "upsert_user_state",
        "set_metadata",
        "delete_session",
        "delete_idle_sessions",
        "append_event_and_update_state",
    ],
)
def test_teradata_adk_refuses_autocommit_only_compound_mutation(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    native = SimpleNamespace(
        get_option=lambda _: "true", set_autocommit=MagicMock(side_effect=NotSupportedError("autocommit only"))
    )
    conn = SimpleNamespace(adbc_connection=native, cursor=MagicMock(), commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))
    store = AdbcADKStore(AdbcConfig(connection_config={"uri": "teradata://user:pass@host/database"}))
    now = datetime.now(UTC)
    event = {
        "id": "event",
        "app_name": "app",
        "user_id": "user",
        "session_id": "session",
        "invocation_id": "invocation",
        "timestamp": now,
        "event_data": {},
    }
    args: dict[str, tuple[Any, ...]] = {
        "upsert_app_state": ("app", {}),
        "upsert_user_state": ("app", "user", {}),
        "set_metadata": ("key", "value"),
        "delete_session": ("app", "user", "session"),
        "delete_idle_sessions": (now, "app"),
        "append_event_and_update_state": (event, "app", "user", "session", {}),
    }
    with pytest.raises(TransactionError, match="autocommit-only"):
        getattr(store, operation)(*args[operation])
    conn.cursor.assert_not_called()


@pytest.mark.parametrize("initial_mode", ["true", "false"])
def test_teradata_flightsql_adk_compound_transaction_restores_mode(
    monkeypatch: pytest.MonkeyPatch, initial_mode: str
) -> None:
    modes: list[bool] = []
    native = SimpleNamespace(
        get_option=lambda _: initial_mode,
        set_autocommit=lambda value: modes.append(value),
        commit=MagicMock(),
        rollback=MagicMock(),
    )
    cursor = MagicMock()
    conn = SimpleNamespace(adbc_connection=native, cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))
    store = AdbcADKStore(AdbcConfig(connection_config={"uri": "grpc+tls://host:443", "adbc_dialect": "teradata"}))
    store.upsert_app_state("app", {"value": 1})
    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args_list[0].args == ("DELETE FROM adk_app_state WHERE app_name = ?", ("app",))
    committer = native.commit if initial_mode == "true" else conn.commit
    committer.assert_called_once()
    assert modes == ([False, True] if initial_mode == "true" else [False])
    cursor.execute.side_effect = RuntimeError("insert failed")
    with pytest.raises(RuntimeError, match="insert failed"):
        store.upsert_user_state("app", "user", {})
    native.rollback.assert_called_once()
    conn.rollback.assert_not_called()
    assert modes[-1] is (initial_mode == "true")


def test_teradata_adk_metadata_queries_quote_reserved_key_column(monkeypatch: pytest.MonkeyPatch) -> None:
    """AdbcADKStore quotes the reserved KEY column on Teradata in metadata SELECT, DELETE, and INSERT."""
    executed_sql: list[tuple[str, tuple[Any, ...]]] = []
    native = SimpleNamespace(get_option=lambda _: "false", set_autocommit=lambda _value: None, rollback=MagicMock())
    cursor = MagicMock()
    cursor.fetchone.return_value = ("1",)
    cursor.execute.side_effect = lambda sql, params: executed_sql.append((sql, tuple(params)))
    conn = SimpleNamespace(adbc_connection=native, cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    store = AdbcADKStore(AdbcConfig(connection_config={"uri": "teradata://user:pass@host/database"}))
    assert store._get_metadata("schema_version") == "1"
    store._set_metadata("schema_version", "2")

    assert executed_sql == [
        ('SELECT value FROM adk_internal_metadata WHERE "KEY" = ?', ("schema_version",)),
        ('DELETE FROM adk_internal_metadata WHERE "KEY" = ?', ("schema_version",)),
        ('INSERT INTO adk_internal_metadata ("KEY", value) VALUES (?, ?)', ("schema_version", "2")),
    ]
    conn.commit.assert_called_once()


@pytest.mark.parametrize("failure_cls", [RuntimeError, KeyboardInterrupt])
def test_adbc_atomic_connection_enables_commit_and_rolls_back_before_restoring_autocommit(
    monkeypatch: pytest.MonkeyPatch, failure_cls: type[BaseException]
) -> None:
    """_adbc_atomic_connection commits on the native connection and rolls back before set_autocommit(True)."""
    call_order: list[str] = []

    class _FakeConnection:
        def __init__(self) -> None:
            self._autocommit = True
            self._commit_supported = False
            self.adbc_connection = SimpleNamespace(
                get_option=lambda _: "true",
                set_autocommit=self._set_autocommit,
                rollback=self._native_rollback,
                commit=self._native_commit,
            )

        def _set_autocommit(self, enabled: bool) -> None:
            call_order.append(f"set_autocommit:{enabled}")

        def _native_rollback(self) -> None:
            call_order.append("native_rollback")

        def _native_commit(self) -> None:
            call_order.append("native_commit")

        def commit(self) -> None:
            if not self._commit_supported:
                call_order.append("commit_noop")
                return
            call_order.append("commit_executed")

        def rollback(self) -> None:
            call_order.append("conn_rollback")

    fake_conn = _FakeConnection()
    config = AdbcConfig(connection_config={"uri": "grpc+tls://host:443", "adbc_dialect": "teradata"})
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(fake_conn))

    with _adbc_atomic_connection(config, "teradata") as conn:
        conn.commit()
    assert fake_conn._commit_supported is False
    assert call_order == ["set_autocommit:False", "native_commit", "set_autocommit:True"]

    call_order.clear()
    with pytest.raises(failure_cls, match="aborted"), _adbc_atomic_connection(config, "teradata"):
        raise failure_cls("aborted")

    assert fake_conn._commit_supported is False
    assert call_order == ["set_autocommit:False", "native_rollback", "set_autocommit:True"]


def test_adbc_atomic_connection_prevents_partial_autocommit_on_real_dbapi_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Real DBAPI connection starting in autocommit=True commits atomically and rolls back partial writes on error."""
    adbc_sqlite = pytest.importorskip("adbc_driver_sqlite.dbapi")
    db_uri = f"file:{tmp_path / 'atomic_teradata.sqlite'}"
    config = AdbcConfig(connection_config={"uri": "grpc+tls://host:443", "adbc_dialect": "teradata"})

    raw_conn = adbc_sqlite.connect(db_uri, autocommit=True)
    try:
        setup_cur = raw_conn.cursor()
        setup_cur.execute('CREATE TABLE kv ("KEY" TEXT PRIMARY KEY, value TEXT NOT NULL)')
        setup_cur.close()
        assert raw_conn._commit_supported is False

        monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(raw_conn))

        with _adbc_atomic_connection(config, "teradata") as conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO kv ("KEY", value) VALUES (?, ?)', ("k1", "initial"))
            cur.close()
            conn.commit()

        assert raw_conn._commit_supported is False
        assert raw_conn.adbc_connection.get_option("adbc.connection.autocommit") == "true"

        with pytest.raises(RuntimeError, match="boom"), _adbc_atomic_connection(config, "teradata") as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM kv WHERE "KEY" = ?', ("k1",))
            cur.close()
            raise RuntimeError("boom")

        assert raw_conn._commit_supported is False
        assert raw_conn.adbc_connection.get_option("adbc.connection.autocommit") == "true"

        verify_cur = raw_conn.cursor()
        verify_cur.execute('SELECT value FROM kv WHERE "KEY" = ?', ("k1",))
        assert verify_cur.fetchone() == ("initial",)
        verify_cur.close()
    finally:
        raw_conn.close()


def test_snowflake_adbc_adk_and_memory_variant_dml_merge_and_ilike(monkeypatch: pytest.MonkeyPatch) -> None:
    """Snowflake ADBC ADK stores emit PARSE_JSON/SELECT/MERGE DML, decode Mapping/bytes, and search via ILIKE."""
    from types import MappingProxyType

    executed: list[tuple[str, tuple[Any, ...]]] = []
    now = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
    cursor = MagicMock()

    def run_sql(sql: str, params: Any = ()) -> None:
        normalized = _normalized(sql)
        executed.append((normalized, tuple(params)))
        if normalized.startswith("SELECT id, app_name, user_id, state"):
            cursor.fetchone.return_value = ("s1", "app", "u1", MappingProxyType({"count": 1}), now, now)
            cursor.fetchall.return_value = [cursor.fetchone.return_value]
        elif normalized.startswith("SELECT e.id, e.session_id"):
            cursor.fetchall.return_value = [("e1", "s1", "inv1", now, bytearray(b'{"text":"hello"}'), "app", "u1")]
        elif normalized.startswith("SELECT event_id FROM adk_memory WHERE event_id IN"):
            cursor.fetchall.return_value = []
        elif normalized.startswith("MERGE INTO adk_memory"):
            cursor.rowcount = 1 if params[0] == "m1" else 0
        elif "content_text ILIKE ?" in normalized:
            cursor.fetchall.return_value = [
                (
                    "m1",
                    "s1",
                    "app",
                    "u1",
                    "user",
                    "e1",
                    "agent",
                    now,
                    MappingProxyType({"parts": ["budget"]}),
                    "Budget Forecast",
                    bytearray(b'{"tag":"q4"}'),
                    now,
                )
            ]

    cursor.execute.side_effect = run_sql
    conn = SimpleNamespace(cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    config = AdbcConfig(
        connection_config={"driver_name": "snowflake"},
        extension_config={"adk": {"enable_memory": True, "owner_id_column": "owner_id VARCHAR(64)"}},
    )
    store = AdbcADKStore(config)
    created = store.create_session("s1", "app", "u1", {"count": 1}, owner_id="org-1")
    assert created["state"] == {"count": 1}
    assert (
        "INSERT INTO adk_session (id, app_name, user_id, owner_id, state, create_time, update_time) "
        "SELECT ?, ?, ?, ?, PARSE_JSON(?), ?, ?"
    ) == executed[0][0]

    executed.clear()
    store.update_session_state("app", "u1", "s1", {"count": 2})
    assert "UPDATE adk_session SET state = PARSE_JSON(?), update_time = ?" in executed[0][0]

    executed.clear()
    event = {
        "id": "e1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv1",
        "timestamp": now,
        "event_data": {"text": "hello"},
    }
    store.append_event(cast("Any", event))
    assert (
        "INSERT INTO adk_event (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
        "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)"
    ) == executed[0][0]

    executed.clear()
    updated = store.append_event_and_update_state(
        cast("Any", event), "app", "u1", "s1", {"count": 3}, app_state={"a": 1}, user_state={"u": 2}
    )
    assert updated["state"] == {"count": 1}
    sql_batch = [stmt for stmt, _ in executed]
    assert "UPDATE adk_session SET state = PARSE_JSON(?), update_time = ?" in sql_batch[0]
    assert "INSERT INTO adk_event" in sql_batch[2] and "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)" in sql_batch[2]
    assert sql_batch[3].startswith("MERGE INTO adk_app_state t USING (SELECT ? AS app_name, PARSE_JSON(?) AS state")
    assert sql_batch[4].startswith(
        "MERGE INTO adk_user_state t USING (SELECT ? AS app_name, ? AS user_id, PARSE_JSON(?) AS state"
    )

    executed.clear()
    store.upsert_app_state("app", {"a": 2})
    store.upsert_user_state("app", "u1", {"u": 3})
    assert executed[0][0].startswith("MERGE INTO adk_app_state t")
    assert executed[1][0].startswith("MERGE INTO adk_user_state t")

    events = store.get_events("app", "u1", "s1")
    assert events[0]["event_data"] == {"text": "hello"}

    executed.clear()
    mem_store = AdbcADKMemoryStore(config)
    entry_one: StoredMemory = {
        "id": "m1",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "e1",
        "author": "agent",
        "timestamp": now,
        "content_json": {"parts": ["budget"]},
        "content_text": "Budget Forecast",
        "metadata_json": {"tag": "q4"},
        "inserted_at": now,
        "embedding": None,
    }
    entry_two: StoredMemory = {**entry_one, "id": "m2", "event_id": "e2"}
    inserted = mem_store.insert_memory_entries([entry_one, entry_two], owner_id="org-1")
    assert inserted == 1
    merge_calls = [stmt for stmt, _ in executed if stmt.startswith("MERGE INTO adk_memory")]
    assert len(merge_calls) == 2
    assert "PARSE_JSON(?) AS content_json" in merge_calls[0]
    assert "PARSE_JSON(?) AS metadata_json" in merge_calls[0]
    assert "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT" in merge_calls[0]

    executed.clear()
    recalled = mem_store.search_entries(" budget ", "app", "u1", limit=5)
    assert len(recalled) == 1
    assert recalled[0]["content_json"] == {"parts": ["budget"]}
    assert recalled[0]["metadata_json"] == {"tag": "q4"}
    search_sql, search_params = executed[0]
    assert "content_text ILIKE ?" in search_sql
    assert search_params == ("app", "u1", "%budget%", 5)
