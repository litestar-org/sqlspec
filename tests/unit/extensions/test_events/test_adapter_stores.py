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


def test_asyncmy_store_index_sql_has_no_information_schema_probe() -> None:
    """Asyncmy index SQL is a plain ADD INDEX with no embedded existence probe."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)
    index_sql = store._index_ddl()

    assert index_sql is not None
    lowered = index_sql.lower()
    assert "information_schema" not in lowered
    assert "prepare" not in lowered
    assert "set @" not in lowered
    assert "ADD INDEX" in index_sql
    assert f"'{store._index_name()}'" not in index_sql


def test_asyncmy_store_schema_qualified_index_target() -> None:
    """Schema-qualified tables split into (schema, table) for the DD check."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(
        connection_config={"host": "localhost", "database": "test"},
        extension_config={"events": {"queue_table": "myschema.events"}},
    )
    store = AsyncmyEventQueueStore(config)

    assert store._index_existence_target() == ("myschema", "events")
    assert "ALTER TABLE myschema.events ADD INDEX" in (store._index_ddl() or "")


def test_asyncmy_store_unqualified_table_target() -> None:
    """Unqualified tables report a None schema for the DD check."""
    pytest.importorskip("asyncmy")
    from sqlspec.adapters.asyncmy import AsyncmyConfig
    from sqlspec.adapters.asyncmy.events.store import AsyncmyEventQueueStore

    config = AsyncmyConfig(connection_config={"host": "localhost", "database": "test"})
    store = AsyncmyEventQueueStore(config)

    assert store._index_existence_target() == (None, "sqlspec_event_queue")


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
    create_sql = store._table_ddl()

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
    create_sql = store._table_ddl()

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

    assert store._index_ddl() is None


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
    assert "custom_events" in store._table_ddl()
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
    create_sql = store._table_ddl()

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
    index_sql = store._index_ddl()

    assert index_sql is not None
    assert "CREATE INDEX" in index_sql
    assert "(channel, status, created_at, available_at)" in index_sql


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

    assert store._index_ddl() is None


def test_adbc_store_bigquery_dialect() -> None:
    """ADBC store generates BigQuery-specific DDL."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    config = AdbcConfig(connection_config={"uri": "bigquery://project"})
    store = AdbcEventQueueStore(config)
    create_sql = store._table_ddl()

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

    assert store._index_ddl() is None


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
    index_sql = store._index_ddl()

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
    store._table_ddl()
    store._index_ddl()
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

    class FixtureStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = FixtureStore(config)
    assert store.table_name == "custom_queue"


def test_base_store_settings_property() -> None:
    """BaseEventQueueStore.settings returns extension settings."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"events": {"queue_table": "my_queue", "retention_seconds": 3600}},
    )

    class FixtureStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = FixtureStore(config)
    assert store.settings.get("queue_table") == "my_queue"
    assert store.settings.get("retention_seconds") == 3600


def test_base_store_default_table_name() -> None:
    """BaseEventQueueStore uses sqlspec_event_queue as default."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class FixtureStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = FixtureStore(config)
    assert store.table_name == "sqlspec_event_queue"


def test_base_store_create_statements_if_not_exists() -> None:
    """Base create_statements adds IF NOT EXISTS wrappers."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class FixtureStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = FixtureStore(config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE IF NOT EXISTS" in statements[0]
    assert "CREATE INDEX IF NOT EXISTS" in statements[1]


def test_base_store_drop_statements_if_exists() -> None:
    """Base drop_statements adds IF EXISTS wrapper."""
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"events": {}})

    class FixtureStore(BaseEventQueueStoreBase):
        def _column_types(self):
            return "TEXT", "TEXT", "TIMESTAMP"

    store = FixtureStore(config)
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
        extension_config={"events": {"queue_table": "my_queue", "retention_seconds": 3600}},
    )
    store = DuckDBEventQueueStore(config)

    assert store.settings.get("queue_table") == "my_queue"
    assert store.settings.get("retention_seconds") == 3600


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


@pytest.mark.anyio
async def test_oracle_event_stores_prepare_schema_primes_version_cache_for_23ai_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oracle sync and async event stores prime _oracle_version_cache on context.driver without changing create_statements."""
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore, OracleSyncEventQueueStore

    monkeypatch.setattr(OracleSyncConfig, "provide_session", MagicMock(side_effect=AssertionError("nested session")))
    monkeypatch.setattr(OracleAsyncConfig, "provide_session", MagicMock(side_effect=AssertionError("nested session")))

    for version, expected_token in (
        (OracleVersionInfo(23, 4, 0, compatible="23.0.0"), "JSON"),
        (OracleVersionInfo(19, 0, 0, compatible="19.0.0"), "BLOB"),
    ):
        sync_cfg = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
        sync_store = OracleSyncEventQueueStore(sync_cfg)
        before_sync_ddl = sync_store.create_statements()
        sync_driver = MagicMock()
        sync_driver.data_dictionary.get_version.return_value = version
        sync_store.prepare_schema_sync(sync_driver)
        after_sync_ddl = sync_store.create_statements()
        assert before_sync_ddl == after_sync_ddl
        sync_cols = {c.name: c.dtype.upper() for c in sync_store.get_schema_targets()[0].create_table.columns}
        assert sync_cols["payload_json"] == expected_token
        assert sync_cols["metadata_json"] == expected_token

        async_cfg = OracleAsyncConfig(connection_config={"dsn": "localhost/xe"})
        async_store = OracleAsyncEventQueueStore(async_cfg)
        before_async_ddl = async_store.create_statements()
        async_driver = MagicMock()
        async_driver.data_dictionary.get_version = AsyncMock(return_value=version)
        await async_store.prepare_schema_async(async_driver)
        after_async_ddl = async_store.create_statements()
        assert before_async_ddl == after_async_ddl
        async_cols = {c.name: c.dtype.upper() for c in async_store.get_schema_targets()[0].create_table.columns}
        assert async_cols["payload_json"] == expected_token
        assert async_cols["metadata_json"] == expected_token


def test_oracle_event_reconcile_accepts_existing_blob_queue_on_23ai() -> None:
    """Existing BLOB event queue columns on Oracle 23ai remain compatible with reconcile JSON targets."""
    from unittest.mock import MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.adapters.oracledb.events.store import OracleSyncEventQueueStore
    from sqlspec.migrations.schema import ensure_schema_sync

    cfg = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    store = OracleSyncEventQueueStore(cfg)
    driver = MagicMock()
    driver.data_dictionary.get_version.return_value = OracleVersionInfo(23, 4, 0, compatible="23.0.0")
    driver.data_dictionary.get_tables.return_value = [{"table_name": "SQLSPEC_EVENT_QUEUE"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "EVENT_ID", "data_type": "VARCHAR2", "nullable": False},
        {"column_name": "CHANNEL", "data_type": "VARCHAR2", "nullable": False},
        {"column_name": "PAYLOAD_JSON", "data_type": "BLOB", "nullable": False},
        {"column_name": "METADATA_JSON", "data_type": "BLOB", "nullable": True},
        {"column_name": "STATUS", "data_type": "VARCHAR2", "nullable": False},
        {"column_name": "AVAILABLE_AT", "data_type": "TIMESTAMP(6)", "nullable": False},
        {"column_name": "LEASE_EXPIRES_AT", "data_type": "TIMESTAMP(6)", "nullable": True},
        {"column_name": "ATTEMPTS", "data_type": "NUMBER", "nullable": False},
        {"column_name": "CREATED_AT", "data_type": "TIMESTAMP(6)", "nullable": False},
        {"column_name": "ACKNOWLEDGED_AT", "data_type": "TIMESTAMP(6)", "nullable": True},
    ]

    store.prepare_schema_sync(driver)
    result = ensure_schema_sync(
        driver, store.get_schema_targets(), manage_schema=True, create_schema=True, commit=False, strict=True
    )

    assert result.created_tables == []
    assert result.added_columns == {}
    driver.execute.assert_not_called()
    driver.execute_script.assert_not_called()


def test_oracle_event_stores_companion_index_seam() -> None:
    """Oracle sync and async event stores expose _index_existence_target and PL/SQL companion index DDL."""
    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore, OracleSyncEventQueueStore

    sync_plain = OracleSyncEventQueueStore(OracleSyncConfig(connection_config={"dsn": "localhost/xe"}))
    async_qual = OracleAsyncEventQueueStore(
        OracleAsyncConfig(
            connection_config={"dsn": "localhost/xe"},
            extension_config={"events": {"queue_table": "APP.sqlspec_event_queue"}},
        )
    )

    assert sync_plain._index_existence_target() == (None, "sqlspec_event_queue")
    assert async_qual._index_existence_target() == ("APP", "sqlspec_event_queue")

    for store in (sync_plain, async_qual):
        stmt = store.companion_index_statement()
        assert stmt is not None
        assert len(store._index_name()) <= 30
        assert store._index_name() in stmt
        assert "SQLCODE NOT IN (-955, -1408)" in stmt


def test_adbc_store_sqlite_and_duckdb_emit_if_not_exists_without_duplication() -> None:
    """ADBC SQLite, DuckDB, FlightSQL, and PostgreSQL stores emit IF NOT EXISTS once without duplication."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore

    configs = [
        AdbcConfig(connection_config={"uri": "sqlite:///:memory:"}),
        AdbcConfig(connection_config={"uri": "duckdb:///:memory:"}),
        AdbcConfig(connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://localhost:8815"}),
        AdbcConfig(connection_config={"driver_name": "postgres"}),
    ]
    for cfg in configs:
        store = AdbcEventQueueStore(cfg)
        stmts = store.create_statements()
        assert len(stmts) == 2
        assert stmts[0].upper().count("CREATE TABLE IF NOT EXISTS") == 1
        assert "IF NOT EXISTS IF NOT EXISTS" not in stmts[0].upper()
        assert stmts[1].upper().count("CREATE INDEX IF NOT EXISTS") == 1
        assert "IF NOT EXISTS IF NOT EXISTS" not in stmts[1].upper()
        companion = store.companion_index_statement()
        assert companion is not None
        assert companion.upper().count("CREATE INDEX IF NOT EXISTS") == 1
        assert "IF NOT EXISTS IF NOT EXISTS" not in companion.upper()


def test_adbc_store_sqlite_companion_index_reconcile_is_idempotent() -> None:
    """Repeating AdbcEventQueueStore companion index reconciliation against SQLite succeeds without duplicate-index error."""
    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.adapters.adbc.events.store import AdbcEventQueueStore
    from sqlspec.adapters.sqlite import SqliteConfig

    adbc_store = AdbcEventQueueStore(AdbcConfig(connection_config={"uri": "sqlite:///:memory:"}))
    sqlite_cfg = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with sqlite_cfg.provide_session() as driver:
            driver.begin()
            for stmt in adbc_store.create_statements():
                driver.execute_script(stmt)
            adbc_store.reconcile_companion_objects_sync(driver)
            adbc_store.reconcile_companion_objects_sync(driver)
            assert driver.connection.in_transaction is True
            driver.commit()
    finally:
        sqlite_cfg.close_pool()


@pytest.mark.anyio
async def test_native_backend_base_store_defaults_and_aq_channels_runtime_non_restriction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BaseEventQueueStore native hooks default to no-ops and aq_channels never restricts runtime publish."""
    from typing import Any, cast, get_type_hints
    from unittest.mock import MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.events.backend import OracleSyncAQEventBackend
    from sqlspec.adapters.oracledb.events.store import (
        OracleAsyncEventQueueStore,
        OracleEventsConfig,
        OracleSyncEventQueueStore,
    )
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
    from sqlspec.exceptions import ImproperConfigurationError

    hints = get_type_hints(OracleEventsConfig, include_extras=True)
    assert "aq_channels" in hints

    sqlite_cfg = SqliteConfig(connection_config={"database": ":memory:"})
    sqlite_store = SqliteEventQueueStore(sqlite_cfg)
    sync_driver = MagicMock()
    async_driver = MagicMock()
    assert sqlite_store.native_backend_targets("poll_queue") == ()
    sqlite_store.reconcile_native_backend_sync(sync_driver, "poll_queue", (), create_schema=True)
    await sqlite_store.reconcile_native_backend_async(async_driver, "poll_queue", (), create_schema=True)
    sync_driver.select.assert_not_called()
    sync_driver.execute.assert_not_called()
    async_driver.select.assert_not_called()
    async_driver.execute.assert_not_called()

    bad_sqlite_cfg = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"aq_channels": ["alpha"]}}
    )
    with pytest.raises(ImproperConfigurationError, match="aq_channels"):
        SqliteEventQueueStore(bad_sqlite_cfg)

    ora_sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"},
        extension_config={"events": {"backend": "aq", "aq_queue": "EVT_{channel}", "aq_channels": ["alpha"]}},
    )
    ora_async_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/xe"},
        extension_config={"events": {"backend": "txeventq", "aq_queue": "EVT_{channel}", "aq_channels": ["alpha"]}},
    )
    assert OracleSyncEventQueueStore(ora_sync_cfg).native_backend_targets("aq") == ("EVT_ALPHA",)
    assert OracleAsyncEventQueueStore(ora_async_cfg).native_backend_targets("txeventq") == ("EVT_ALPHA",)

    opened_queues: list[tuple[str, str]] = []
    mock_queue = MagicMock()
    mock_conn = MagicMock()

    def _open_queue(name: str, payload_type: str) -> MagicMock:
        opened_queues.append((name, payload_type))
        return mock_queue

    mock_conn.queue.side_effect = _open_queue
    mock_session_driver = MagicMock()
    mock_session_driver.connection = mock_conn
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_session_driver
    mock_cm.__exit__.return_value = False
    monkeypatch.setattr(OracleSyncConfig, "provide_session", lambda _self: mock_cm)

    backend = OracleSyncAQEventBackend(ora_sync_cfg, cast("dict[str, Any]", ora_sync_cfg.extension_config["events"]))
    published_id = backend.publish("beta", {"hello": "world"})
    assert len(published_id) == 32
    assert opened_queues == [("EVT_BETA", "JSON")]
    mock_queue.enqone.assert_called_once()


def test_native_oracle_target_resolution_and_refusals() -> None:
    """Oracle native_backend_targets expands templates, folds identifiers, and refuses invalid settings or <21c."""
    from typing import Any

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore, OracleSyncEventQueueStore
    from sqlspec.exceptions import MigrationError

    default_store = OracleSyncEventQueueStore(OracleSyncConfig(connection_config={"dsn": "localhost/xe"}))
    assert default_store.native_backend_targets("poll_queue") == ()
    assert default_store.native_backend_targets("aq") == ("SQLSPEC_EVENTS_QUEUE",)
    assert default_store.native_backend_targets("txeventq") == ("SQLSPEC_EVENTS_QUEUE",)

    lower_store = OracleSyncEventQueueStore(
        OracleSyncConfig(connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"aq_queue": "evt"}})
    )
    assert lower_store.native_backend_targets("aq") == ("EVT",)

    expanded_async_store = OracleAsyncEventQueueStore(
        OracleAsyncConfig(
            connection_config={"dsn": "localhost/xe"},
            extension_config={"events": {"aq_queue": "evt_{channel}", "aq_channels": ["alpha", "beta"]}},
        )
    )
    assert expanded_async_store.native_backend_targets("txeventq") == ("EVT_ALPHA", "EVT_BETA")

    empty_channel_cases: tuple[dict[str, object], ...] = ({}, {"aq_channels": []})
    for channels_setting in empty_channel_cases:
        events_cfg: Any = {"aq_queue": "EVT_{channel}", **channels_setting}
        missing_channels_store = OracleSyncEventQueueStore(
            OracleSyncConfig(connection_config={"dsn": "localhost/xe"}, extension_config={"events": events_cfg})
        )
        with pytest.raises(MigrationError, match=r'aq_channels.*migration_mode="versioned"'):
            missing_channels_store.native_backend_targets("aq")

    fixed_with_channels_store = OracleSyncEventQueueStore(
        OracleSyncConfig(
            connection_config={"dsn": "localhost/xe"},
            extension_config={"events": {"aq_queue": "EVT", "aq_channels": ["a"]}},
        )
    )
    with pytest.raises(MigrationError) as exc_fixed:
        fixed_with_channels_store.native_backend_targets("aq")
    assert "aq_channels" in str(exc_fixed.value)
    assert "aq_queue" in str(exc_fixed.value)

    bad_channel_store = OracleSyncEventQueueStore(
        OracleSyncConfig(
            connection_config={"dsn": "localhost/xe"},
            extension_config={"events": {"aq_queue": "EVT_{channel}", "aq_channels": ["bad-name!"]}},
        )
    )
    with pytest.raises(MigrationError, match="Invalid events channel name"):
        bad_channel_store.native_backend_targets("aq")

    dotted_store = OracleSyncEventQueueStore(
        OracleSyncConfig(
            connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"aq_queue": "owner.evt"}}
        )
    )
    with pytest.raises(MigrationError, match="bare"):
        dotted_store.native_backend_targets("aq")

    old_cfg = OracleSyncConfig(connection_config={"dsn": "localhost/xe"})
    old_cfg._oracle_version_cache.version = OracleVersionInfo(19, 0, 0, compatible="19.0.0")
    old_cfg._oracle_version_cache.resolved = True
    old_store = OracleSyncEventQueueStore(old_cfg)
    with pytest.raises(MigrationError, match=r'Oracle 21c.*migration_mode="versioned"'):
        old_store.native_backend_targets("aq")
    assert old_store.native_backend_targets("poll_queue") == ()


@pytest.mark.anyio
async def test_native_oracle_events_reconcile_refuses_before_table_ddl_and_honors_manage_schema() -> None:
    """Events reconcile refuses bad AQ settings or Oracle <21c before table DDL and skips when manage_schema=False."""
    import inspect
    from typing import Any, cast
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.exceptions import MigrationError
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext

    bad_settings_cases: list[dict[str, object]] = [
        {"backend": "aq", "aq_queue": "EVT_{channel}", "migration_mode": "reconcile"},
        {"backend": "aq", "aq_queue": "EVT", "aq_channels": ["a"], "migration_mode": "reconcile"},
        {"backend": "aq", "aq_queue": "EVT_{channel}", "aq_channels": ["bad-name!"], "migration_mode": "reconcile"},
        {"backend": "txeventq", "aq_queue": "owner.evt", "migration_mode": "reconcile"},
    ]
    for events_settings in bad_settings_cases:
        cfg = OracleSyncConfig(
            connection_config={"dsn": "localhost/xe"}, extension_config={"events": cast("Any", events_settings)}
        )
        drv = MagicMock()
        drv.data_dictionary.get_version.return_value = OracleVersionInfo(23, 4, 0, compatible="23.0.0")
        with pytest.raises(MigrationError):
            events_reconcile(MigrationContext(config=cfg, driver=drv))
        drv.data_dictionary.get_tables.assert_not_called()
        drv.execute_script.assert_not_called()
        drv.execute.assert_not_called()

    ora19_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/xe"},
        extension_config={"events": {"backend": "aq", "json_storage": "blob_json", "migration_mode": "reconcile"}},
    )
    ora19_drv = MagicMock()
    ora19_drv.data_dictionary.get_version = AsyncMock(return_value=OracleVersionInfo(19, 0, 0, compatible="19.0.0"))
    ora19_drv.data_dictionary.get_tables = AsyncMock(return_value=[])
    ora19_drv.execute_script = AsyncMock()
    ora19_drv.execute = AsyncMock()
    coro = events_reconcile(MigrationContext(config=ora19_cfg, driver=ora19_drv))
    assert inspect.isawaitable(coro)
    with pytest.raises(MigrationError, match="Oracle 21c"):
        await coro
    ora19_drv.data_dictionary.get_tables.assert_not_awaited()
    ora19_drv.execute_script.assert_not_awaited()
    ora19_drv.execute.assert_not_awaited()

    unmanaged_sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/xe"},
        extension_config={
            "events": {
                "backend": "aq",
                "aq_queue": "EVT_{channel}",
                "manage_schema": False,
                "migration_mode": "reconcile",
            }
        },
    )
    unmanaged_sync_drv = MagicMock()
    assert events_reconcile(MigrationContext(config=unmanaged_sync_cfg, driver=unmanaged_sync_drv)) is None
    unmanaged_sync_drv.data_dictionary.get_version.assert_not_called()
    unmanaged_sync_drv.data_dictionary.get_tables.assert_not_called()
    unmanaged_sync_drv.select.assert_not_called()
    unmanaged_sync_drv.execute.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_native_oracle_aq_and_txeventq_provisioning_sync_and_async(is_async: bool) -> None:
    """Oracle event stores provision AQ and TxEventQ queues idempotently after table reconcile."""
    import inspect
    from typing import Any
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore, OracleSyncEventQueueStore
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext

    def _make_driver(
        queue_rows: dict[str, list[dict[str, str]]],
        qt_rows: set[str],
        select_calls: list[tuple[str, dict[str, Any]]],
        exec_calls: list[tuple[str, dict[str, Any]]],
        call_order: list[str] | None = None,
    ) -> MagicMock:
        drv = MagicMock()

        def _handle_select(sql: str, params: dict[str, Any]) -> list[dict[str, str]]:
            select_calls.append((sql, dict(params)))
            if call_order is not None:
                call_order.append("select_catalog")
            if "FROM user_queues" in sql:
                return queue_rows.get(params["name"], [])
            if "FROM user_queue_tables" in sql:
                return [{"queue_table": params["name"]}] if params["name"] in qt_rows else []
            msg = f"Unexpected select SQL: {sql}"
            raise AssertionError(msg)

        def _handle_exec(sql: str, params: dict[str, Any]) -> None:
            exec_calls.append((sql, dict(params)))
            if call_order is not None:
                call_order.append("exec_aqadm")

        def _get_version(_d: object) -> OracleVersionInfo:
            if call_order is not None:
                call_order.append("get_version")
            return OracleVersionInfo(23, 4, 0, compatible="23.0.0")

        def _get_tables(_d: object, schema: str | None = None) -> list[dict[str, str]]:
            if call_order is not None:
                call_order.append("get_tables")
            return []

        def _get_indexes(_d: object, table: str | None = None, schema: str | None = None) -> list[dict[str, str]]:
            if call_order is not None:
                call_order.append("get_indexes")
            return []

        def _exec_script(_s: str) -> None:
            if call_order is not None:
                call_order.append("execute_script")

        if is_async:
            drv.select = AsyncMock(side_effect=_handle_select)
            drv.execute = AsyncMock(side_effect=_handle_exec)
            drv.data_dictionary.get_version = AsyncMock(side_effect=_get_version)
            drv.data_dictionary.get_tables = AsyncMock(side_effect=_get_tables)
            drv.data_dictionary.get_indexes = AsyncMock(side_effect=_get_indexes)
            drv.execute_script = AsyncMock(side_effect=_exec_script)
        else:
            drv.select.side_effect = _handle_select
            drv.execute.side_effect = _handle_exec
            drv.data_dictionary.get_version.side_effect = _get_version
            drv.data_dictionary.get_tables.side_effect = _get_tables
            drv.data_dictionary.get_indexes.side_effect = _get_indexes
            drv.execute_script.side_effect = _exec_script
        return drv

    async def _run_hook(
        store: OracleSyncEventQueueStore | OracleAsyncEventQueueStore,
        drv: MagicMock,
        backend_name: str,
        targets: tuple[str, ...],
    ) -> None:
        if isinstance(store, OracleAsyncEventQueueStore):
            await store.reconcile_native_backend_async(drv, backend_name, targets, create_schema=True)
        else:
            store.reconcile_native_backend_sync(drv, backend_name, targets, create_schema=True)

    store_cls: Any = OracleAsyncEventQueueStore if is_async else OracleSyncEventQueueStore
    cfg_cls: Any = OracleAsyncConfig if is_async else OracleSyncConfig

    long_queue = "Q" + ("X" * 127)
    expected_long_qt = f"{long_queue[:125]}_QT"
    assert len(expected_long_qt) == 128

    aq_store = store_cls(
        cfg_cls(connection_config={"dsn": "localhost/xe"}, extension_config={"events": {"aq_queue": "evt"}})
    )
    select_calls: list[tuple[str, dict[str, Any]]] = []
    exec_calls: list[tuple[str, dict[str, Any]]] = []
    drv = _make_driver({}, set(), select_calls, exec_calls)
    await _run_hook(aq_store, drv, "aq", ("evt", long_queue))
    assert select_calls == [
        (
            "SELECT name, TRIM(enqueue_enabled), TRIM(dequeue_enabled) FROM user_queues WHERE name = :name",
            {"name": "EVT"},
        ),
        ("SELECT queue_table FROM user_queue_tables WHERE queue_table = :name", {"name": "EVT_QT"}),
        (
            "SELECT name, TRIM(enqueue_enabled), TRIM(dequeue_enabled) FROM user_queues WHERE name = :name",
            {"name": long_queue},
        ),
        ("SELECT queue_table FROM user_queue_tables WHERE queue_table = :name", {"name": expected_long_qt}),
    ]
    assert len(exec_calls) == 6
    assert "DBMS_AQADM.CREATE_QUEUE_TABLE" in exec_calls[0][0]
    assert "queue_payload_type => 'JSON'" in exec_calls[0][0]
    assert exec_calls[0][1] == {"queue_table": "EVT_QT"}
    assert "DBMS_AQADM.CREATE_QUEUE(" in exec_calls[1][0]
    assert exec_calls[1][1] == {"queue_name": "EVT", "queue_table": "EVT_QT"}
    assert "DBMS_AQADM.START_QUEUE" in exec_calls[2][0]
    assert exec_calls[2][1] == {"queue_name": "EVT"}
    assert exec_calls[3][1] == {"queue_table": expected_long_qt}
    assert exec_calls[4][1] == {"queue_name": long_queue, "queue_table": expected_long_qt}
    assert exec_calls[5][1] == {"queue_name": long_queue}

    tx_select: list[tuple[str, dict[str, Any]]] = []
    tx_exec: list[tuple[str, dict[str, Any]]] = []
    tx_drv = _make_driver({}, set(), tx_select, tx_exec)
    await _run_hook(aq_store, tx_drv, "txeventq", ("EVT",))
    assert tx_select == [
        (
            "SELECT name, TRIM(enqueue_enabled), TRIM(dequeue_enabled) FROM user_queues WHERE name = :name",
            {"name": "EVT"},
        )
    ]
    assert len(tx_exec) == 2
    assert "DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE" in tx_exec[0][0]
    assert "queue_payload_type => 'JSON'" in tx_exec[0][0]
    assert tx_exec[0][1] == {"queue_name": "EVT"}
    assert "DBMS_AQADM.START_QUEUE" in tx_exec[1][0]
    assert tx_exec[1][1] == {"queue_name": "EVT"}

    existing_select: list[tuple[str, dict[str, Any]]] = []
    existing_exec: list[tuple[str, dict[str, Any]]] = []
    existing_drv = _make_driver(
        {"EVT": [{"name": "EVT", "trim(enqueue_enabled)": "YES", "trim(dequeue_enabled)": "YES"}]},
        set(),
        existing_select,
        existing_exec,
    )
    await _run_hook(aq_store, existing_drv, "aq", ("EVT",))
    assert len(existing_select) == 1
    assert "user_queues" in existing_select[0][0]
    assert existing_exec == []

    partial_select: list[tuple[str, dict[str, Any]]] = []
    partial_exec: list[tuple[str, dict[str, Any]]] = []
    partial_drv = _make_driver({}, {"EVT_QT"}, partial_select, partial_exec)
    await _run_hook(aq_store, partial_drv, "aq", ("EVT",))
    assert len(partial_select) == 2
    assert len(partial_exec) == 2
    assert "CREATE_QUEUE(" in partial_exec[0][0]
    assert "START_QUEUE" in partial_exec[1][0]

    stopped_select: list[tuple[str, dict[str, Any]]] = []
    stopped_exec: list[tuple[str, dict[str, Any]]] = []
    stopped_drv = _make_driver(
        {"EVT": [{"name": "EVT", "trim(enqueue_enabled)": "NO", "trim(dequeue_enabled)": "YES"}]},
        set(),
        stopped_select,
        stopped_exec,
    )
    await _run_hook(aq_store, stopped_drv, "aq", ("EVT",))
    assert len(stopped_select) == 1
    assert len(stopped_exec) == 1
    assert "START_QUEUE" in stopped_exec[0][0]
    assert stopped_exec[0][1] == {"queue_name": "EVT"}

    e2e_cfg = cfg_cls(
        connection_config={"dsn": "localhost/xe"},
        extension_config={"events": {"backend": "txeventq", "aq_queue": "EVT", "migration_mode": "reconcile"}},
    )
    e2e_select: list[tuple[str, dict[str, Any]]] = []
    e2e_exec: list[tuple[str, dict[str, Any]]] = []
    order: list[str] = []
    e2e_drv = _make_driver({}, set(), e2e_select, e2e_exec, call_order=order)
    outcome = events_reconcile(MigrationContext(config=e2e_cfg, driver=e2e_drv))
    if inspect.isawaitable(outcome):
        await outcome
    assert order == [
        "get_version",
        "get_tables",
        "execute_script",
        "get_indexes",
        "execute_script",
        "select_catalog",
        "exec_aqadm",
        "exec_aqadm",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_native_oracle_create_schema_false_and_privilege_errors_sync_and_async(is_async: bool) -> None:
    """Oracle native provisioning enforces create_schema=False and wraps only DBMS_AQADM privilege errors."""
    from typing import Any
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("oracledb")
    from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.events.store import OracleAsyncEventQueueStore, OracleSyncEventQueueStore
    from sqlspec.exceptions import MigrationError, SQLSpecError

    store_cls: Any = OracleAsyncEventQueueStore if is_async else OracleSyncEventQueueStore
    cfg_cls: Any = OracleAsyncConfig if is_async else OracleSyncConfig
    store = store_cls(cfg_cls(connection_config={"dsn": "localhost/xe"}))

    async def _call(drv: MagicMock, *, create_schema: bool) -> None:
        if isinstance(store, OracleAsyncEventQueueStore):
            await store.reconcile_native_backend_async(drv, "aq", ("EVT",), create_schema=create_schema)
        else:
            store.reconcile_native_backend_sync(drv, "aq", ("EVT",), create_schema=create_schema)

    for rows in ([], [{"name": "EVT", "trim(enqueue_enabled)": "YES", "trim(dequeue_enabled)": "NO"}]):
        drv = MagicMock()
        if is_async:
            drv.select = AsyncMock(return_value=rows)
            drv.execute = AsyncMock()
        else:
            drv.select.return_value = rows
        with pytest.raises(MigrationError, match=r"EVT.*create_schema"):
            await _call(drv, create_schema=False)
        drv.execute.assert_not_called()

    started_drv = MagicMock()
    started_rows = [{"name": "EVT", "trim(enqueue_enabled)": "YES", "trim(dequeue_enabled)": "YES"}]
    if is_async:
        started_drv.select = AsyncMock(return_value=started_rows)
        started_drv.execute = AsyncMock()
    else:
        started_drv.select.return_value = started_rows
    await _call(started_drv, create_schema=False)
    started_drv.execute.assert_not_called()

    priv_errors = [
        SQLSpecError("ORA-01031: insufficient privileges"),
        SQLSpecError("ORA-06550: line 1, column 7:\nPLS-00201: identifier 'DBMS_AQADM' must be declared"),
    ]
    for err in priv_errors:
        priv_drv = MagicMock()
        if is_async:
            priv_drv.select = AsyncMock(return_value=[])
            priv_drv.execute = AsyncMock(side_effect=err)
        else:
            priv_drv.select.return_value = []
            priv_drv.execute.side_effect = err
        with pytest.raises(MigrationError) as exc_info:
            await _call(priv_drv, create_schema=True)
        assert "GRANT EXECUTE ON DBMS_AQADM" in str(exc_info.value)
        assert "AQ_ADMINISTRATOR_ROLE" in str(exc_info.value)

    other_err = SQLSpecError("ORA-00942: table or view does not exist")
    other_drv = MagicMock()
    if is_async:
        other_drv.select = AsyncMock(return_value=[])
        other_drv.execute = AsyncMock(side_effect=other_err)
    else:
        other_drv.select.return_value = []
        other_drv.execute.side_effect = other_err
    with pytest.raises(SQLSpecError, match="ORA-00942"):
        await _call(other_drv, create_schema=True)


@pytest.mark.anyio
async def test_spanner_event_store_reconcile_companion_index_and_sharded_target() -> None:
    """Spanner event stores parse sharded DDL without mutating create_statement and repair indexes idempotently."""
    import inspect
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("google.cloud.spanner")
    from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
    from sqlspec.adapters.spanner.events.store import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore
    from sqlspec.exceptions import MigrationError
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext
    from sqlspec.migrations.schema import ensure_schema_sync

    conn = {"project": "test-project", "instance_id": "inst", "database_id": "db"}
    sync_cfg = SpannerSyncConfig(
        connection_config=conn, extension_config={"events": {"shard_count": 4, "migration_mode": "reconcile"}}
    )
    async_cfg = SpannerAsyncConfig(
        connection_config=conn,
        extension_config={"events": {"queue_table": "app.events_q", "shard_count": 4, "migration_mode": "reconcile"}},
    )
    sync_store = SpannerSyncEventQueueStore(sync_cfg)
    async_store = SpannerAsyncEventQueueStore(async_cfg)

    assert sync_store._index_existence_target() == (None, "sqlspec_event_queue")
    assert async_store._index_existence_target() == ("app", "events_q")

    target = sync_store._schema_target()
    assert "shard_id" in target.unsafe_add_columns
    assert target.constraint_columns == frozenset({"shard_id", "event_id"})
    assert "NOT NULL AS (" in str(target.create_statement)

    full_cols = [
        {"column_name": "event_id", "data_type": "STRING(64)", "nullable": False},
        {"column_name": "shard_id", "data_type": "INT64", "nullable": False},
        {"column_name": "channel", "data_type": "STRING(128)", "nullable": False},
        {"column_name": "payload_json", "data_type": "JSON", "nullable": False},
        {"column_name": "metadata_json", "data_type": "JSON", "nullable": True},
        {"column_name": "status", "data_type": "STRING(32)", "nullable": False},
        {"column_name": "available_at", "data_type": "TIMESTAMP", "nullable": False},
        {"column_name": "lease_expires_at", "data_type": "TIMESTAMP", "nullable": True},
        {"column_name": "attempts", "data_type": "INT64", "nullable": False},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "nullable": False},
        {"column_name": "acknowledged_at", "data_type": "TIMESTAMP", "nullable": True},
    ]

    sync_drv = MagicMock()
    sync_drv.data_dictionary.get_tables.return_value = [{"table_name": "sqlspec_event_queue"}]
    sync_drv.data_dictionary.get_columns.return_value = full_cols
    sync_drv.data_dictionary.get_indexes.return_value = []
    assert events_reconcile(MigrationContext(config=sync_cfg, driver=sync_drv)) is None
    sync_drv.execute_script.assert_called_once_with(sync_store._index_ddl())
    sync_drv.commit.assert_not_called()

    sync_drv.execute_script.reset_mock()
    sync_drv.data_dictionary.get_indexes.return_value = [
        {"index_name": sync_store._index_name().upper(), "table_name": "SQLSPEC_EVENT_QUEUE"}
    ]
    sync_store.reconcile_companion_objects_sync(sync_drv)
    sync_drv.execute_script.assert_not_called()

    async_drv = MagicMock()
    async_drv.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "events_q"}])
    async_drv.data_dictionary.get_columns = AsyncMock(return_value=full_cols)
    async_drv.data_dictionary.get_indexes = AsyncMock(return_value=[])
    async_drv.execute_script = AsyncMock()
    async_drv.commit = AsyncMock()
    coro = events_reconcile(MigrationContext(config=async_cfg, driver=async_drv))
    assert inspect.isawaitable(coro)
    await coro
    async_drv.execute_script.assert_awaited_once_with(async_store._index_ddl())
    async_drv.commit.assert_not_awaited()

    missing_shard_drv = MagicMock()
    missing_shard_drv.data_dictionary.get_tables.return_value = [{"table_name": "sqlspec_event_queue"}]
    missing_shard_drv.data_dictionary.get_columns.return_value = [
        row for row in full_cols if row["column_name"] != "shard_id"
    ]
    with pytest.raises(MigrationError, match=r'shard_id.*migration_mode="versioned"'):
        ensure_schema_sync(
            missing_shard_drv, [target], manage_schema=True, create_schema=True, commit=False, strict=True
        )


def test_bigquery_event_queue_store_reconcile_additive_and_refusal() -> None:
    """BigQuery event queue store adds nullable columns and refuses defaulted or NOT NULL columns."""
    from unittest.mock import MagicMock

    pytest.importorskip("google.cloud.bigquery")
    from sqlspec.adapters.bigquery.config import BigQueryConfig
    from sqlspec.adapters.bigquery.events.store import BigQueryEventQueueStore
    from sqlspec.exceptions import MigrationError
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext

    config = BigQueryConfig(
        connection_config={"project": "test-project"},
        extension_config={"events": {"queue_table": "bq_events", "partitioning": True, "migration_mode": "reconcile"}},
    )
    store = BigQueryEventQueueStore(config)
    assert store._index_ddl() is None
    assert store.companion_index_statement() is None

    all_cols = [
        {"column_name": "event_id", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "channel", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "payload_json", "data_type": "JSON", "is_nullable": "NO"},
        {"column_name": "metadata_json", "data_type": "JSON", "is_nullable": "YES"},
        {"column_name": "status", "data_type": "STRING", "is_nullable": "NO"},
        {"column_name": "available_at", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        {"column_name": "lease_expires_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
        {"column_name": "attempts", "data_type": "INT64", "is_nullable": "NO"},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        {"column_name": "acknowledged_at", "data_type": "TIMESTAMP", "is_nullable": "YES"},
    ]

    add_nullable_drv = MagicMock()
    add_nullable_drv.data_dictionary.get_tables.return_value = [{"table_name": "bq_events"}]
    add_nullable_drv.data_dictionary.get_columns.return_value = [
        row for row in all_cols if row["column_name"] != "acknowledged_at"
    ]
    assert events_reconcile(MigrationContext(config=config, dialect="bigquery", driver=add_nullable_drv)) is None
    assert add_nullable_drv.execute.call_count == 1
    stmt = add_nullable_drv.execute.call_args.args[0]
    sql = stmt.sql if hasattr(stmt, "sql") else str(stmt)
    assert "ADD COLUMN `acknowledged_at` TIMESTAMP" in sql

    missing_default_drv = MagicMock()
    missing_default_drv.data_dictionary.get_tables.return_value = [{"table_name": "bq_events"}]
    missing_default_drv.data_dictionary.get_columns.return_value = [
        row for row in all_cols if row["column_name"] != "status"
    ]
    with pytest.raises(MigrationError, match="status") as exc_info:
        events_reconcile(MigrationContext(config=config, dialect="bigquery", driver=missing_default_drv))
    assert 'migration_mode="versioned"' in str(exc_info.value)


def test_db2_event_stores_companion_index_reconcile() -> None:
    """Db2 event queue stores probe catalog-folded index metadata before executing CREATE INDEX."""
    from unittest.mock import MagicMock

    from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
    from sqlspec.adapters.db2.events.store import Db2AsyncEventQueueStore, Db2SyncEventQueueStore

    sync_cfg = Db2SyncConfig(connection_config={"database": "TESTDB"})
    sync_store = Db2SyncEventQueueStore(sync_cfg)
    assert sync_store._index_existence_target() == (None, "SQLSPEC_EVENT_QUEUE")
    assert sync_store.get_schema_targets()[0].table_name.lower() == "sqlspec_event_queue"

    missing_driver = MagicMock()
    missing_driver.data_dictionary.get_indexes.return_value = []
    sync_store.reconcile_companion_objects_sync(missing_driver)
    missing_driver.execute_script.assert_called_once_with(
        "CREATE INDEX idx_sqlspec_event_queue_channel_status ON sqlspec_event_queue(channel, status, available_at)"
    )

    present_driver = MagicMock()
    present_driver.data_dictionary.get_indexes.return_value = [
        {"index_name": "IDX_SQLSPEC_EVENT_QUEUE_CHANNEL_STATUS", "table_name": "SQLSPEC_EVENT_QUEUE"}
    ]
    sync_store.reconcile_companion_objects_sync(present_driver)
    present_driver.execute_script.assert_not_called()

    async_cfg = Db2AsyncConfig(
        connection_config={"database": "TESTDB"}, extension_config={"events": {"queue_table": "app.queue_events"}}
    )
    async_store = Db2AsyncEventQueueStore(async_cfg)
    assert async_store._index_existence_target() == ("APP", "QUEUE_EVENTS")
