# pyright: reportPrivateUsage=false
"""Unit tests for adapter-specific event queue stores and DDL generation."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.core import StatementConfig
from sqlspec.extensions.events import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite import SqliteConfig

    BaseEventQueueStoreBase = BaseEventQueueStore[SqliteConfig]
else:
    BaseEventQueueStoreBase = BaseEventQueueStore


def test_asyncmy_store_column_types() -> None:
    """Asyncmy store uses MySQL-compatible column types."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "DATETIME(6)"


def test_asyncmy_store_index_sql_dynamic_check() -> None:
    """Asyncmy index SQL includes dynamic existence check."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)
    index_sql = store._build_index_sql()

    assert index_sql is not None
    assert "information_schema.statistics" in index_sql.lower()
    assert "PREPARE" in index_sql.upper()
    assert "EXECUTE" in index_sql.upper()


def test_asyncmy_store_schema_qualified_index() -> None:
    """Schema-qualified tables use explicit schema in index check."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(
        connection_config={"host": "localhost", "database": "test"},
        extension_config={"events": {"queue_table": "myschema.events"}},
    )
    store = AsyncmyEventQueueStore(config)
    index_sql = store._build_index_sql()
    assert index_sql is not None
    assert "'myschema'" in index_sql


def test_asyncmy_store_unqualified_table_uses_database() -> None:
    """Unqualified tables use DATABASE() for schema detection."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)
    index_sql = store._build_index_sql()
    assert index_sql is not None
    assert "DATABASE()" in index_sql


def test_bigquery_store_column_types() -> None:
    """BigQuery store uses BigQuery-compatible column types."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_bigquery_store_create_table_uses_cluster_by() -> None:
    """BigQuery DDL uses CLUSTER BY instead of index."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)
    create_sql = store._build_create_table_sql()

    assert "CREATE TABLE IF NOT EXISTS" in create_sql
    assert "STRING NOT NULL" in create_sql
    assert "INT64 NOT NULL" in create_sql
    assert "CLUSTER BY channel, status, available_at" in create_sql


def test_bigquery_store_create_table_has_defaults() -> None:
    """BigQuery DDL includes default values for status, attempts, and timestamps."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)
    create_sql = store._build_create_table_sql()

    assert "DEFAULT 'pending'" in create_sql
    assert "DEFAULT 0" in create_sql
    assert "DEFAULT CURRENT_TIMESTAMP()" in create_sql


def test_bigquery_store_no_index() -> None:
    """BigQuery store returns None for index SQL (uses clustering)."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)

    assert store._build_index_sql() is None


def test_bigquery_store_create_statements_single() -> None:
    """BigQuery create_statements returns single statement (no index)."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 1
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "CLUSTER BY" in statements[0]


def test_bigquery_store_drop_statements_single() -> None:
    """BigQuery drop_statements returns single statement with IF EXISTS."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(connection_config={"project": "test-project"})
    store = BigQueryEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 1
    assert "DROP TABLE IF EXISTS" in statements[0]
    assert store.table_name in statements[0]


def test_bigquery_store_custom_table_name() -> None:
    """BigQuery store uses custom table name from extension_config."""
    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore

    config = BigQueryConfig(
        connection_config={"project": "test-project"}, extension_config={"events": {"queue_table": "custom_events"}}
    )
    store = BigQueryEventQueueStore(config)

    assert store.table_name == "custom_events"
    assert "custom_events" in store._build_create_table_sql()
    assert "custom_events" in store.drop_statements()[0]


def test_oracle_sync_store_column_types() -> None:
    """Oracle sync store uses BLOB for JSON columns by default (blob_json mode)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "BLOB"
    assert metadata_type == "BLOB"
    assert timestamp_type == "TIMESTAMP"


def test_oracle_async_store_column_types() -> None:
    """Oracle async store uses same types as sync."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore

    config = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleAsyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "BLOB"
    assert metadata_type == "BLOB"
    assert timestamp_type == "TIMESTAMP"


def test_oracle_store_native_json_storage() -> None:
    """Oracle store uses JSON type when json_storage='json' (21c+)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"json_storage": "json"}}
    )
    store = OracleSyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_oracle_store_blob_json_creates_is_json_constraint() -> None:
    """Oracle store adds IS JSON constraint for blob_json storage (12c+)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"json_storage": "blob_json"}}
    )
    store = OracleSyncEventQueueStore(config)
    script = store.create_statements()[0]

    assert "CHECK (payload_json IS JSON)" in script
    assert "CHECK (metadata_json IS JSON)" in script


def test_oracle_store_plain_blob_no_constraint() -> None:
    """Oracle store omits IS JSON constraint for plain blob storage (11g)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"json_storage": "blob"}}
    )
    store = OracleSyncEventQueueStore(config)
    script = store.create_statements()[0]

    assert "payload_json BLOB NOT NULL" in script
    assert "metadata_json BLOB" in script
    assert "IS JSON" not in script


def test_oracle_store_index_name_truncated() -> None:
    """Oracle index names are truncated to 30 characters."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"},
        extension_config={"events": {"queue_table": "very_long_table_name_for_events"}},
    )
    store = OracleSyncEventQueueStore(config)
    index_name = store._index_name()

    assert len(index_name) <= 30


def test_oracle_store_create_statement_single_plsql_script() -> None:
    """Oracle CREATE returns single PL/SQL script with multiple blocks (like ADK/Litestar)."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 1
    script = statements[0]
    assert "CREATE TABLE" in script
    assert "CREATE INDEX" in script
    assert script.count("BEGIN") == 2
    assert script.count("EXCEPTION") == 2
    assert script.count("-955") == 2


def test_oracle_store_drop_statement_plsql_wrapper() -> None:
    """Oracle DROP statements handle errors gracefully."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 2
    assert "DROP INDEX" in statements[0]
    assert "DROP TABLE" in statements[1]
    assert "-1418" in statements[0]
    assert "-942" in statements[1]


def test_oracle_store_in_memory_clause_in_script() -> None:
    """Oracle store adds INMEMORY clause when configured."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"in_memory": True}}
    )
    store = OracleSyncEventQueueStore(config)
    script = store.create_statements()[0]

    assert "INMEMORY PRIORITY HIGH" in script


def test_oracle_store_no_in_memory_by_default() -> None:
    """Oracle store omits INMEMORY clause by default."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore

    config = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(config)
    script = store.create_statements()[0]

    assert "INMEMORY" not in script


def test_spanner_store_column_types() -> None:
    """Spanner store uses GoogleSQL column types."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_spanner_store_create_table_uses_string_types() -> None:
    """Spanner DDL uses STRING(n) instead of VARCHAR."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    create_sql = store._build_create_table_sql()

    assert "STRING(64)" in create_sql
    assert "STRING(128)" in create_sql
    assert "INT64" in create_sql
    assert "PRIMARY KEY (event_id)" in create_sql


def test_spanner_store_separate_index_statement() -> None:
    """Spanner requires separate index creation statement."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    index_sql = store._build_index_sql()

    assert index_sql is not None
    assert "CREATE INDEX" in index_sql
    assert "(channel, status, available_at)" in index_sql


def test_spanner_store_create_statements_no_if_not_exists() -> None:
    """Spanner DDL does not support IF NOT EXISTS."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    for stmt in statements:
        assert "IF NOT EXISTS" not in stmt


def test_spanner_store_drop_statements_reverse_order() -> None:
    """Spanner drops index before table."""
    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerSyncEventQueueStore

    config = SpannerSyncConfig(connection_config={"project": "test", "instance": "inst", "database": "db"})
    store = SpannerSyncEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 2
    assert "DROP INDEX" in statements[0]
    assert "DROP TABLE" in statements[1]


def test_adbc_store_postgres_dialect() -> None:
    """ADBC store detects PostgreSQL dialect from URI."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "postgresql://localhost/test"})
    store = AdbcEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"


def test_adbc_store_snowflake_dialect() -> None:
    """ADBC store detects Snowflake dialect from driver name."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "snowflake"})
    store = AdbcEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "VARIANT"
    assert metadata_type == "VARIANT"
    assert timestamp_type == "TIMESTAMP_TZ"


def test_adbc_store_snowflake_no_index() -> None:
    """ADBC store returns None for Snowflake index (not supported)."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "snowflake"})
    store = AdbcEventQueueStore(config)

    assert store._build_index_sql() is None


def test_adbc_store_bigquery_dialect() -> None:
    """ADBC store generates BigQuery-specific DDL."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "bigquery://project"})
    store = AdbcEventQueueStore(config)
    create_sql = store._build_create_table_sql()

    assert "STRING NOT NULL" in create_sql
    assert "INT64 NOT NULL" in create_sql
    assert "CLUSTER BY" in create_sql


def test_adbc_store_bigquery_no_index() -> None:
    """ADBC store returns None for BigQuery index (uses clustering)."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "bigquery://project"})
    store = AdbcEventQueueStore(config)

    assert store._build_index_sql() is None


def test_adbc_store_flightsql_dialect() -> None:
    """ADBC store detects FlightSQL dialect and uses SQLite types."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "flightsql"})
    store = AdbcEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "TEXT"
    assert metadata_type == "TEXT"
    assert timestamp_type == "TIMESTAMP"


def test_adbc_store_duckdb_dialect() -> None:
    """ADBC store detects DuckDB dialect."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "duckdb:///:memory:"})
    store = AdbcEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_adbc_store_sqlite_fallback() -> None:
    """ADBC store falls back to SQLite types for unknown dialects."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "unknown_driver"})
    store = AdbcEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "TEXT"
    assert metadata_type == "TEXT"
    assert timestamp_type == "TIMESTAMP"


def test_adbc_store_postgresql_partial_index() -> None:
    """ADBC PostgreSQL dialect creates partial index for pending events."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "postgres"})
    store = AdbcEventQueueStore(config)
    index_sql = store._build_index_sql()

    assert index_sql is not None
    assert "WHERE status = 'pending'" in index_sql


def test_adbc_store_separate_statements() -> None:
    """ADBC store returns separate table and index statements."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "postgres"})
    store = AdbcEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE" in statements[0]
    assert "CREATE INDEX" in statements[1]


def test_adbc_store_drop_statements() -> None:
    """ADBC store drops index before table."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "postgres"})
    store = AdbcEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 2
    assert "DROP INDEX" in statements[0]
    assert "DROP TABLE" in statements[1]


def test_adbc_store_bigquery_single_drop_statement() -> None:
    """ADBC BigQuery store returns single drop statement (no index)."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "bigquery"})
    store = AdbcEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 1
    assert "DROP TABLE" in statements[0]


def test_adbc_store_statement_config_dialect_fallback() -> None:
    """ADBC store uses statement_config dialect when connection_config hints fail."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(
        connection_config={"driver_name": "generic"}, statement_config=StatementConfig(dialect="duckdb")
    )
    store = AdbcEventQueueStore(config)
    payload_type, _, _ = store._column_types()

    assert payload_type == "JSON"


def test_adbc_store_dialect_caching() -> None:
    """ADBC store caches dialect detection (only computes once)."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "postgresql://localhost/test"})
    store = AdbcEventQueueStore(config)

    assert store._dialect is None

    dialect1 = store.dialect
    assert store._dialect == "postgres"
    assert dialect1 == "postgres"

    dialect2 = store.dialect
    assert dialect1 is dialect2

    store._column_types()
    store._build_create_table_sql()
    store._build_index_sql()
    store.drop_statements()
    assert store._dialect == "postgres"


def test_adbc_store_dialect_property_exposes_detected_dialect() -> None:
    """ADBC store dialect property returns the detected dialect string."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import DIALECT_DUCKDB, AdbcEventQueueStore

    config = AdbcConfig(connection_config={"driver_name": "duckdb"})
    store = AdbcEventQueueStore(config)

    assert store.dialect == DIALECT_DUCKDB
    assert store.dialect == "duckdb"


def test_base_store_table_name_property() -> None:
    """BaseEventQueueStore.table_name returns configured name."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"queue_table": "custom_queue"}}
    )

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = TestStore(config)
    assert store.table_name == "custom_queue"


def test_base_store_settings_property() -> None:
    """BaseEventQueueStore.settings returns extension settings."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"events": {"queue_table": "my_queue", "custom_setting": "value"}},
    )

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = TestStore(config)
    assert store.settings.get("queue_table") == "my_queue"
    assert store.settings.get("custom_setting") == "value"


def test_base_store_default_table_name() -> None:
    """BaseEventQueueStore uses sqlspec_event_queue as default."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = TestStore(config)
    assert store.table_name == "sqlspec_event_queue"


def test_base_store_create_statements_if_not_exists() -> None:
    """Base create_statements adds IF NOT EXISTS wrappers."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = TestStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_base_store_drop_statements_if_exists() -> None:
    """Base drop_statements adds IF EXISTS wrapper."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class TestStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = TestStore(config)
    statements = store.drop_statements()

    assert len(statements) == 1
    assert "DROP TABLE IF EXISTS" in statements[0]


def test_duckdb_store_column_types() -> None:
    """DuckDB store uses native JSON and TIMESTAMP types."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_duckdb_store_default_table_name() -> None:
    """DuckDB store uses default table name when not configured."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)

    assert store.table_name == "sqlspec_event_queue"


def test_duckdb_store_custom_table_name() -> None:
    """DuckDB store uses custom table name from extension_config."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"queue_table": "custom_events"}}
    )
    store = DuckDBEventQueueStore(config)

    assert store.table_name == "custom_events"


def test_duckdb_store_create_statements() -> None:
    """DuckDB store generates CREATE TABLE and CREATE INDEX statements."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "JSON NOT NULL" in statements[0]
    assert "TIMESTAMP NOT NULL" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_duckdb_store_drop_statements() -> None:
    """DuckDB store generates DROP TABLE statement with IF EXISTS."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 1
    assert "DROP TABLE IF EXISTS" in statements[0]


def test_duckdb_store_settings_property() -> None:
    """DuckDB store exposes extension settings via settings property."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(
        connection_config={"database": ":memory:"},
        extension_config={"events": {"queue_table": "my_queue", "custom_key": "custom_value"}},
    )
    store = DuckDBEventQueueStore(config)

    assert store.settings.get("queue_table") == "my_queue"
    assert store.settings.get("custom_key") == "custom_value"


def test_duckdb_store_schema_qualified_table() -> None:
    """DuckDB store accepts schema-qualified table names."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"queue_table": "main.event_queue"}}
    )
    store = DuckDBEventQueueStore(config)

    assert store.table_name == "main.event_queue"


def test_duckdb_store_index_name() -> None:
    """DuckDB store generates correct index name."""
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.duckdb.events.store import DuckDBEventQueueStore

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    store = DuckDBEventQueueStore(config)
    index_name = store._index_name()

    assert index_name == "idx_sqlspec_event_queue_channel_status"


# PostgreSQL adapter stores (asyncpg, psycopg, psqlpy)


def test_asyncpg_store_column_types() -> None:
    """AsyncPG store uses PostgreSQL-native JSONB and TIMESTAMPTZ."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    store = AsyncpgEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"


def test_asyncpg_store_create_statements() -> None:
    """AsyncPG store generates PostgreSQL-compatible DDL."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore

    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    store = AsyncpgEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "JSONB NOT NULL" in statements[0]
    assert "TIMESTAMPTZ NOT NULL" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_asyncpg_store_custom_table_name() -> None:
    """AsyncPG store uses custom table name from extension_config."""
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        extension_config={"events": {"queue_table": "my_pg_events"}},
    )
    store = AsyncpgEventQueueStore(config)

    assert store.table_name == "my_pg_events"


def test_psycopg_sync_store_column_types() -> None:
    """Psycopg sync store uses PostgreSQL-native JSONB and TIMESTAMPTZ."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.store import PsycopgSyncEventQueueStore

    config = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})
    store = PsycopgSyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"


def test_psycopg_async_store_column_types() -> None:
    """Psycopg async store uses PostgreSQL-native JSONB and TIMESTAMPTZ."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig
    from sqlspec.adapters.psycopg.events.store import PsycopgAsyncEventQueueStore

    config = PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})
    store = PsycopgAsyncEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"


def test_psycopg_stores_same_column_types() -> None:
    """Both psycopg stores (sync and async) use identical column types."""
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.store import PsycopgAsyncEventQueueStore, PsycopgSyncEventQueueStore

    sync_config = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})
    async_config = PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})

    sync_store = PsycopgSyncEventQueueStore(sync_config)
    async_store = PsycopgAsyncEventQueueStore(async_config)

    assert sync_store._column_types() == async_store._column_types()


def test_psqlpy_store_column_types() -> None:
    """Psqlpy store uses PostgreSQL-native JSONB and TIMESTAMPTZ."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.store import PsqlpyEventQueueStore

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    store = PsqlpyEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSONB"
    assert metadata_type == "JSONB"
    assert timestamp_type == "TIMESTAMPTZ"


def test_psqlpy_store_create_statements() -> None:
    """Psqlpy store generates PostgreSQL-compatible DDL."""
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.store import PsqlpyEventQueueStore

    config = PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})
    store = PsqlpyEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "JSONB NOT NULL" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_all_postgres_stores_have_consistent_types() -> None:
    """All PostgreSQL stores (asyncpg, psycopg, psqlpy) use JSONB/TIMESTAMPTZ."""
    asyncpg = pytest.importorskip("asyncpg")
    psycopg = pytest.importorskip("psycopg")
    psqlpy = pytest.importorskip("psqlpy")

    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psqlpy.events.store import PsqlpyEventQueueStore
    from sqlspec.adapters.psycopg import PsycopgSyncConfig
    from sqlspec.adapters.psycopg.events.store import PsycopgSyncEventQueueStore

    # Suppress unused import warnings
    _ = asyncpg, psycopg, psqlpy

    stores = [
        AsyncpgEventQueueStore(AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})),
        PsycopgSyncEventQueueStore(PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})),
        PsqlpyEventQueueStore(PsqlpyConfig(connection_config={"dsn": "postgresql://localhost/test"})),
    ]

    expected = ("JSONB", "JSONB", "TIMESTAMPTZ")
    for store in stores:
        assert store._column_types() == expected, f"{store.__class__.__name__} has inconsistent column types"


# SQLite adapter stores (sqlite, aiosqlite)


def test_sqlite_store_column_types() -> None:
    """SQLite store uses TEXT for JSON (no native JSON type)."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = SqliteEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "TEXT"
    assert metadata_type == "TEXT"
    assert timestamp_type == "TIMESTAMP"


def test_sqlite_store_create_statements() -> None:
    """SQLite store generates SQLite-compatible DDL."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = SqliteEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "TEXT NOT NULL" in statements[0]
    assert "TIMESTAMP NOT NULL" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_sqlite_store_drop_statements() -> None:
    """SQLite store generates DROP TABLE with IF EXISTS."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = SqliteEventQueueStore(config)
    statements = store.drop_statements()

    assert len(statements) == 1
    assert "DROP TABLE IF EXISTS" in statements[0]


def test_sqlite_store_custom_table_name() -> None:
    """SQLite store uses custom table name from extension_config."""
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"queue_table": "my_sqlite_events"}}
    )
    store = SqliteEventQueueStore(config)

    assert store.table_name == "my_sqlite_events"


def test_aiosqlite_store_column_types() -> None:
    """AioSQLite store uses TEXT for JSON (same as sync SQLite)."""
    pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteEventQueueStore(config)
    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "TEXT"
    assert metadata_type == "TEXT"
    assert timestamp_type == "TIMESTAMP"


def test_aiosqlite_store_create_statements() -> None:
    """AioSQLite store generates SQLite-compatible DDL."""
    pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    store = AiosqliteEventQueueStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "TEXT NOT NULL" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_sqlite_stores_same_column_types() -> None:
    """Both SQLite stores (sync and async) use identical column types."""
    pytest.importorskip("aiosqlite")
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.adapters.aiosqlite.events.store import AiosqliteEventQueueStore
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore

    sync_config = SqliteConfig(connection_config={"database": ":memory:"})
    async_config = AiosqliteConfig(connection_config={"database": ":memory:"})

    sync_store = SqliteEventQueueStore(sync_config)
    async_store = AiosqliteEventQueueStore(async_config)

    assert sync_store._column_types() == async_store._column_types()
