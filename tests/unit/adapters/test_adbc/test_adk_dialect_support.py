"""Tests for ADBC ADK store dialect-specific DDL generation."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKMemoryStore, AdbcADKStore
from sqlspec.adapters.adbc.adk.store import _oracle_store_sql, _postgresql_store_sql
from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
from sqlspec.adapters.adbc.litestar.store import ADBCStore
from sqlspec.core import StatementConfig

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
    """ADBCStore should recognize Postgres extension dialects, omit indexes on BigQuery/Snowflake, and use select_one_or_none."""
    config = AdbcConfig(connection_config={"driver_name": "bigquery"})
    store = ADBCStore(config)
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
    assert store.dialect() == expected_dialect
    assert mem_store.dialect == expected_dialect


def test_detect_dialect_fallback_to_statement_config() -> None:
    """AdbcADKStore and AdbcADKMemoryStore fall back to statement_config.dialect when connection_config has no driver."""
    config = AdbcConfig(connection_config={}, statement_config=StatementConfig(dialect="oracle"))
    store = AdbcADKStore(config)
    mem_store = AdbcADKMemoryStore(config)
    assert store.dialect() == "oracle"
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


def _store_for(driver_name: str) -> AdbcADKStore:
    return AdbcADKStore(AdbcConfig(connection_config={"driver_name": driver_name, "uri": ":memory:"}))


def _normalized(sql: str) -> str:
    return " ".join(sql.split())
