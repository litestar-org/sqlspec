# pyright: reportPrivateUsage=false
"""Unit tests for BaseEventQueueStore hook methods and DDL generation.

Tests the hook-based pattern introduced for DDL generation standardization:
- _string_type(length): String type syntax (VARCHAR(N), STRING(N), VARCHAR2(N))
- _integer_type(): Integer type syntax (INTEGER, INT64)
- _timestamp_default(): Timestamp default expression (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(6))
- _primary_key_syntax(): Inline PRIMARY KEY clause
- _table_clause(): Additional table options (CLUSTER BY, INMEMORY)
"""

from typing import TYPE_CHECKING

import pytest

from sqlspec.extensions.events import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite import SqliteConfig

    BaseEventQueueStoreBase = BaseEventQueueStore[SqliteConfig]
else:
    BaseEventQueueStoreBase = BaseEventQueueStore


def test_base_store_string_type_default() -> None:
    """Base store _string_type returns VARCHAR(N) format."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._string_type(64) == "VARCHAR(64)"
    assert store._string_type(128) == "VARCHAR(128)"
    assert store._string_type(32) == "VARCHAR(32)"


def test_base_store_integer_type_default() -> None:
    """Base store _integer_type returns INTEGER."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._integer_type() == "INTEGER"


def test_base_store_timestamp_default() -> None:
    """Base store _timestamp_default returns CURRENT_TIMESTAMP."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._timestamp_default() == "CURRENT_TIMESTAMP"


def test_base_store_primary_key_syntax_default() -> None:
    """Base store _primary_key_syntax returns empty string (column-level PK)."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._primary_key_syntax() == ""


def test_base_store_table_clause_default() -> None:
    """Base store _table_clause returns empty string."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._table_clause() == ""


def test_hook_override_string_type() -> None:
    """Subclass can override _string_type for dialect-specific syntax."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class CustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

        def _string_type(self, length: int) -> str:
            return f"STRING({length})"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = CustomStore(config)

    assert store._string_type(64) == "STRING(64)"
    assert "STRING(64)" in store._build_create_table_sql()
    assert "STRING(128)" in store._build_create_table_sql()


def test_hook_override_integer_type() -> None:
    """Subclass can override _integer_type for dialect-specific syntax."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class CustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

        def _integer_type(self) -> str:
            return "INT64"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = CustomStore(config)

    assert store._integer_type() == "INT64"
    assert "INT64 NOT NULL" in store._build_create_table_sql()


def test_hook_override_timestamp_default() -> None:
    """Subclass can override _timestamp_default for dialect-specific syntax."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class CustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "DATETIME(6)"

        def _timestamp_default(self) -> str:
            return "CURRENT_TIMESTAMP(6)"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = CustomStore(config)

    assert store._timestamp_default() == "CURRENT_TIMESTAMP(6)"
    assert "DEFAULT CURRENT_TIMESTAMP(6)" in store._build_create_table_sql()


def test_hook_override_primary_key_syntax() -> None:
    """Subclass can override _primary_key_syntax for table-level PK."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class CustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

        def _primary_key_syntax(self) -> str:
            return " PRIMARY KEY (event_id)"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = CustomStore(config)

    assert store._primary_key_syntax() == " PRIMARY KEY (event_id)"
    ddl = store._build_create_table_sql()
    assert " PRIMARY KEY (event_id)" in ddl
    assert "event_id VARCHAR(64)," in ddl


def test_hook_override_table_clause() -> None:
    """Subclass can override _table_clause for additional options."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class CustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

        def _table_clause(self) -> str:
            return " CLUSTER BY channel, status"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = CustomStore(config)

    assert store._table_clause() == " CLUSTER BY channel, status"
    assert " CLUSTER BY channel, status" in store._build_create_table_sql()


def test_ddl_generation_uses_all_hooks() -> None:
    """DDL generation correctly uses all hook methods together."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class FullCustomStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

        def _string_type(self, length: int) -> str:
            return f"STRING({length})"

        def _integer_type(self) -> str:
            return "INT64"

        def _timestamp_default(self) -> str:
            return "CURRENT_TIMESTAMP()"

        def _primary_key_syntax(self) -> str:
            return " PRIMARY KEY (event_id)"

        def _table_clause(self) -> str:
            return " OPTIONS(description='Event queue')"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = FullCustomStore(config)
    ddl = store._build_create_table_sql()

    assert "STRING(64)" in ddl
    assert "STRING(128)" in ddl
    assert "STRING(32)" in ddl
    assert "INT64 NOT NULL" in ddl
    assert "DEFAULT CURRENT_TIMESTAMP()" in ddl
    assert "PRIMARY KEY (event_id)" in ddl
    assert "OPTIONS(description='Event queue')" in ddl


def test_mysql_timestamp_default_hook() -> None:
    """MySQL store overrides _timestamp_default for CURRENT_TIMESTAMP(6)."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)

    assert store._timestamp_default() == "CURRENT_TIMESTAMP(6)"


def test_mysql_ddl_uses_timestamp_default_hook() -> None:
    """MySQL DDL correctly uses CURRENT_TIMESTAMP(6) for DATETIME(6) columns."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)
    statements = store.create_statements()
    ddl = statements[0]

    assert "DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)" in ddl
    assert "DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP," not in ddl


def test_bigquery_string_type_hook() -> None:
    """BigQuery store overrides _string_type to return STRING (no length)."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)

    assert store._string_type(64) == "STRING"
    assert store._string_type(128) == "STRING"
    assert store._string_type(0) == "STRING"


def test_bigquery_integer_type_hook() -> None:
    """BigQuery store overrides _integer_type to return INT64."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)

    assert store._integer_type() == "INT64"


def test_bigquery_timestamp_default_hook() -> None:
    """BigQuery store overrides _timestamp_default to return CURRENT_TIMESTAMP()."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)

    assert store._timestamp_default() == "CURRENT_TIMESTAMP()"


def test_bigquery_table_clause_hook() -> None:
    """BigQuery store overrides _table_clause for CLUSTER BY."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)

    assert store._table_clause() == " CLUSTER BY channel, status, available_at"


def test_spanner_string_type_hook() -> None:
    """Spanner store overrides _string_type to return STRING(N)."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)

    assert store._string_type(64) == "STRING(64)"
    assert store._string_type(128) == "STRING(128)"
    assert store._string_type(32) == "STRING(32)"


def test_spanner_integer_type_hook() -> None:
    """Spanner store overrides _integer_type to return INT64."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)

    assert store._integer_type() == "INT64"


def test_spanner_primary_key_syntax_hook() -> None:
    """Spanner store overrides _primary_key_syntax for inline PRIMARY KEY."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)

    assert store._primary_key_syntax() == " PRIMARY KEY (event_id)"


def test_spanner_ddl_no_defaults() -> None:
    """Spanner DDL omits DEFAULT clauses (not supported for non-computed columns)."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    ddl = store._build_create_table_sql()

    assert "DEFAULT" not in ddl


def test_oracle_string_type_hook() -> None:
    """Oracle store overrides _string_type to return VARCHAR2(N)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(config)

    assert store._string_type(64) == "VARCHAR2(64)"
    assert store._string_type(128) == "VARCHAR2(128)"


def test_postgresql_stores_use_base_hooks() -> None:
    """PostgreSQL stores (asyncpg, psycopg, psqlpy) use base hook defaults."""
    asyncpg = pytest.importorskip("asyncpg")
    psycopg = pytest.importorskip("psycopg")
    psqlpy = pytest.importorskip("psqlpy")

    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.store import PsqlpyEventQueueStore
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.store import PsycopgSyncEventQueueStore

    _ = asyncpg, psycopg, psqlpy

    stores = [
        AsyncpgEventQueueStore(AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})),
        PsycopgSyncEventQueueStore(PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})),
        PsqlpyEventQueueStore(PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})),
    ]

    for store in stores:
        assert store._string_type(64) == "VARCHAR(64)"
        assert store._integer_type() == "INTEGER"
        assert store._timestamp_default() == "CURRENT_TIMESTAMP"
        assert store._primary_key_syntax() == ""
        assert store._table_clause() == ""


def test_sqlite_stores_use_base_hooks() -> None:
    """SQLite stores (sqlite, aiosqlite) use base hook defaults."""
    aiosqlite = pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    _ = aiosqlite

    stores = [
        SqliteEventQueueStore(SqliteConfig(connection_config={"database": ":memory:"})),
        AiosqliteEventQueueStore(AiosqliteConfig(connection_config={"database": ":memory:"})),
    ]

    for store in stores:
        assert store._string_type(64) == "VARCHAR(64)"
        assert store._integer_type() == "INTEGER"
        assert store._timestamp_default() == "CURRENT_TIMESTAMP"
        assert store._primary_key_syntax() == ""
        assert store._table_clause() == ""


def test_duckdb_store_uses_base_hooks() -> None:
    """DuckDB store uses base hook defaults."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)

    assert store._string_type(64) == "VARCHAR(64)"
    assert store._integer_type() == "INTEGER"
    assert store._timestamp_default() == "CURRENT_TIMESTAMP"
    assert store._primary_key_syntax() == ""
    assert store._table_clause() == ""


def test_ddl_column_primary_key_without_inline_pk() -> None:
    """When _primary_key_syntax is empty, PRIMARY KEY is on the column."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class ColumnPKStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = ColumnPKStore(config)
    ddl = store._build_create_table_sql()

    assert "event_id VARCHAR(64) PRIMARY KEY," in ddl
    assert not ddl.endswith(") PRIMARY KEY (event_id)")


def test_ddl_inline_primary_key_with_override() -> None:
    """When _primary_key_syntax is set, PRIMARY KEY is at table level."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class InlinePKStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

        def _primary_key_syntax(self) -> str:
            return " PRIMARY KEY (event_id)"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = InlinePKStore(config)
    ddl = store._build_create_table_sql()

    assert "event_id VARCHAR(64)," in ddl
    assert "event_id VARCHAR(64) PRIMARY KEY" not in ddl
    assert ") PRIMARY KEY (event_id)" in ddl


def test_ddl_contains_all_required_columns() -> None:
    """DDL includes all required event queue columns."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)
    ddl = store._build_create_table_sql()

    required_columns = [
        "event_id",
        "channel",
        "payload_json",
        "metadata_json",
        "status",
        "available_at",
        "lease_expires_at",
        "attempts",
        "created_at",
        "acknowledged_at",
    ]

    for column in required_columns:
        assert column in ddl, f"Missing column: {column}"


def test_ddl_default_values() -> None:
    """DDL includes default values for status, attempts, available_at, created_at."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)
    ddl = store._build_create_table_sql()

    assert "DEFAULT 'pending'" in ddl
    assert "DEFAULT 0" in ddl
    assert "DEFAULT CURRENT_TIMESTAMP" in ddl


def test_ddl_nullable_columns() -> None:
    """DDL correctly marks optional columns as nullable (no NOT NULL)."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)
    ddl = store._build_create_table_sql()

    assert "metadata_json JSON," in ddl
    assert "lease_expires_at TIMESTAMP," in ddl
    assert "acknowledged_at TIMESTAMP" in ddl


def test_ddl_not_null_columns() -> None:
    """DDL correctly marks required columns as NOT NULL."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "JSON", "JSON", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)
    ddl = store._build_create_table_sql()

    assert "channel VARCHAR(128) NOT NULL" in ddl
    assert "payload_json JSON NOT NULL" in ddl
    assert "status VARCHAR(32) NOT NULL" in ddl
    assert "available_at TIMESTAMP NOT NULL" in ddl
    assert "attempts INTEGER NOT NULL" in ddl
    assert "created_at TIMESTAMP NOT NULL" in ddl


def test_create_statements_with_no_index() -> None:
    """create_statements returns only table when _build_index_sql returns None."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class NoIndexStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

        def _build_index_sql(self) -> str | None:
            return None

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = NoIndexStore(config)
    statements = store.create_statements()

    assert len(statements) == 1
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]


def test_wrap_create_statement_unknown_object_type() -> None:
    """_wrap_create_statement returns statement unchanged for unknown object types."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    original = "CREATE TRIGGER test_trigger ON test_table"
    result = store._wrap_create_statement(original, "trigger")

    assert result == original


def test_wrap_create_statement_table() -> None:
    """_wrap_create_statement adds IF NOT EXISTS for table."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    result = store._wrap_create_statement("CREATE TABLE test (id INT)", "table")

    assert result == "CREATE TABLE IF NOT EXISTS test (id INT)"


def test_wrap_create_statement_index() -> None:
    """_wrap_create_statement adds IF NOT EXISTS for index."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    result = store._wrap_create_statement("CREATE INDEX test_idx ON test(id)", "index")

    assert result == "CREATE INDEX IF NOT EXISTS test_idx ON test(id)"


def test_wrap_drop_statement() -> None:
    """_wrap_drop_statement adds IF EXISTS for DROP TABLE."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    result = store._wrap_drop_statement("DROP TABLE test")

    assert result == "DROP TABLE IF EXISTS test"


def test_index_name_generation() -> None:
    """_index_name generates correct index name from table name."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)

    assert store._index_name() == "idx_sqlspec_event_queue_channel_status"


def test_index_name_with_schema_qualified_table() -> None:
    """_index_name replaces dots with underscores for schema-qualified tables."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"queue_table": "myschema.events"}}
    )
    store = TestStore(config)

    assert store._index_name() == "idx_myschema_events_channel_status"


def test_build_index_sql() -> None:
    """_build_index_sql generates correct CREATE INDEX statement."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = TestStore(config)
    index_sql = store._build_index_sql()

    assert index_sql is not None
    assert "CREATE INDEX idx_sqlspec_event_queue_channel_status" in index_sql
    assert "ON sqlspec_event_queue(channel, status, available_at)" in index_sql


def test_settings_property() -> None:
    """settings property returns extension settings dict."""
    from sqlspec.adapters.sqlite import SqliteConfig

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"events": {"queue_table": "custom", "custom_key": "custom_value"}},
    )
    store = TestStore(config)

    assert store.settings["queue_table"] == "custom"
    assert store.settings["custom_key"] == "custom_value"
