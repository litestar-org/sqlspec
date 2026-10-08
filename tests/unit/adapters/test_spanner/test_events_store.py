# pyright: reportPrivateUsage=false
"""Unit tests for SpannerSyncEventQueueStore and SpannerAsyncEventQueueStore."""

import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore
from sqlspec.exceptions import ImproperConfigurationError


def _mock_spanner_config(
    events_settings: dict[str, Any] | None = None,
    *,
    dialect: str = "spanner",
) -> MagicMock:
    """Create a mock SpannerSyncConfig."""
    config = MagicMock()
    settings: dict[str, Any] = {"queue_table": "test_events"}
    if events_settings is not None:
        settings.update(events_settings)
    config.extension_config = {"events": settings}
    config.statement_config = SimpleNamespace(dialect=dialect)
    return config


def _configure_list_tables(mock_database: MagicMock, table_ids: tuple[str, ...] = ()) -> None:
    """Configure sync and async iteration over mock_database.list_tables()."""
    tables = [SimpleNamespace(table_id=table_id) for table_id in table_ids]
    list_tables_result = MagicMock()
    list_tables_result.__iter__.return_value = iter(tables)
    list_tables_result.__aiter__.return_value = iter(tables)
    mock_database.list_tables.return_value = list_tables_result


def test_column_types_returns_spanner_types() -> None:
    """Verify Spanner-specific column types."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    payload_type, metadata_type, timestamp_type = store._column_types()

    assert payload_type == "JSON"
    assert metadata_type == "JSON"
    assert timestamp_type == "TIMESTAMP"


def test_table_ddl_uses_string_types() -> None:
    """Verify CREATE TABLE uses STRING instead of VARCHAR."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._table_ddl()

    assert "STRING(64)" in sql
    assert "STRING(128)" in sql
    assert "STRING(32)" in sql
    assert "VARCHAR" not in sql


def test_table_ddl_uses_int64() -> None:
    """Verify CREATE TABLE uses INT64 instead of INTEGER."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._table_ddl()

    assert "INT64" in sql
    assert "INTEGER" not in sql


def test_table_ddl_no_default_clauses() -> None:
    """Verify CREATE TABLE has no DEFAULT clauses (Spanner restriction)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._table_ddl()

    assert "DEFAULT" not in sql


def test_table_ddl_inline_primary_key_and_row_deletion_policy() -> None:
    """Verify PRIMARY KEY is declared inline with default ROW DELETION POLICY."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._table_ddl()

    assert sql.endswith(
        ") PRIMARY KEY (event_id), ROW DELETION POLICY (OLDER_THAN(acknowledged_at, INTERVAL 1 DAY))"
    )


def test_table_ddl_row_deletion_policy_rounding_and_disabled() -> None:
    """Verify ROW DELETION POLICY rounds seconds up to whole days and can be disabled."""
    rounded_store = SpannerSyncEventQueueStore(_mock_spanner_config({"retention_seconds": 86_401}))
    assert rounded_store._table_ddl().endswith(
        ") PRIMARY KEY (event_id), ROW DELETION POLICY (OLDER_THAN(acknowledged_at, INTERVAL 2 DAY))"
    )

    disabled_store = SpannerSyncEventQueueStore(_mock_spanner_config({"retention_seconds": 0}))
    disabled_sql = disabled_store._table_ddl()
    assert "ROW DELETION POLICY" not in disabled_sql
    assert disabled_sql.endswith(") PRIMARY KEY (event_id)")

    with pytest.raises(ImproperConfigurationError, match="retention_seconds"):
        SpannerSyncEventQueueStore(_mock_spanner_config({"retention_seconds": True}))._table_ddl()


def test_index_ddl_covering_and_no_if_not_exists() -> None:
    """Verify index creation includes created_at and STORING covering columns without IF NOT EXISTS."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._index_ddl()

    assert sql == (
        "CREATE INDEX idx_test_events_channel_status ON test_events"
        "(channel, status, created_at, available_at) "
        "STORING (payload_json, metadata_json, attempts, lease_expires_at)"
    )


def test_shard_count_ddl_and_validation() -> None:
    """Verify shard_count > 1 generates computed shard_id column and shard-prefixed PK/index."""
    config = _mock_spanner_config({"shard_count": 8})
    store = SpannerSyncEventQueueStore(config)

    table_sql = store._table_ddl()
    index_sql = store._index_ddl()

    assert "shard_id INT64 NOT NULL AS (MOD(FARM_FINGERPRINT(event_id), 8)) STORED" in table_sql
    assert ") PRIMARY KEY (shard_id, event_id)" in table_sql
    assert index_sql == (
        "CREATE INDEX idx_test_events_channel_status ON test_events"
        "(shard_id, channel, status, created_at, available_at) "
        "STORING (payload_json, metadata_json, attempts, lease_expires_at)"
    )

    for invalid_value in (0, -2, True, "8"):
        with pytest.raises(ImproperConfigurationError, match="shard_count"):
            SpannerSyncEventQueueStore(_mock_spanner_config({"shard_count": invalid_value}))._table_ddl()


def test_spangres_dialect_ddl() -> None:
    """Verify Spangres dialect DDL uses PostgreSQL types, INCLUDE covering index, and TTL INTERVAL."""
    config = _mock_spanner_config({"shard_count": 4, "retention_seconds": 172_800}, dialect="spangres")
    store = SpannerSyncEventQueueStore(config)

    table_sql = store._table_ddl()
    index_sql = store._index_ddl()

    assert "event_id varchar(64) NOT NULL" in table_sql
    assert "payload_json jsonb NOT NULL" in table_sql
    assert "available_at timestamptz NOT NULL" in table_sql
    assert "attempts bigint NOT NULL" in table_sql
    assert "shard_id bigint NOT NULL GENERATED ALWAYS AS (spanner.farm_fingerprint(event_id) % 4) STORED" in table_sql
    assert "PRIMARY KEY (shard_id, event_id)" in table_sql
    assert table_sql.endswith("TTL INTERVAL '2 days' ON acknowledged_at")
    assert index_sql == (
        "CREATE INDEX idx_test_events_channel_status ON test_events"
        "(shard_id, channel, status, created_at, available_at) "
        "INCLUDE (payload_json, metadata_json, attempts, lease_expires_at)"
    )


def test_wrap_create_statement_returns_unchanged() -> None:
    """Verify _wrap_create_statement returns statement unchanged."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    original = "CREATE TABLE foo (id INT64) PRIMARY KEY (id)"
    result = store._wrap_create_statement(original, "table")

    assert result == original


def test_wrap_drop_statement_returns_unchanged() -> None:
    """Verify _wrap_drop_statement returns statement unchanged."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    original = "DROP TABLE foo"
    result = store._wrap_drop_statement(original)

    assert result == original


def test_create_statements_returns_two_statements() -> None:
    """Verify create_statements returns table and index separately."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE" in statements[0]
    assert "CREATE INDEX" in statements[1]


def test_drop_statements_index_first() -> None:
    """Verify drop_statements drops index before table."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    statements = store.drop_statements()

    assert len(statements) == 2
    assert "DROP INDEX" in statements[0]
    assert "DROP TABLE" in statements[1]


def test_table_name_from_config() -> None:
    """Verify table name is read from extension config."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    assert store.table_name == "test_events"


def test_table_name_default() -> None:
    """Verify default table name when not configured."""
    config = MagicMock()
    config.extension_config = {"events": {}}
    store = SpannerSyncEventQueueStore(config)

    assert store.table_name == "sqlspec_event_queue"


def test_index_name_generation() -> None:
    """Verify index name is generated from table name."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    index_name = store._index_name()

    assert index_name == "idx_test_events_channel_status"


_STORE_VARIANTS = pytest.mark.parametrize(
    ("store_cls", "config_cls", "mock_factory"),
    [
        (SpannerSyncEventQueueStore, SpannerSyncConfig, MagicMock),
        (SpannerAsyncEventQueueStore, SpannerAsyncConfig, AsyncMock),
    ],
    ids=["sync", "async"],
)


@_STORE_VARIANTS
@pytest.mark.parametrize(
    ("operation", "existing_tables", "expected_prefixes"),
    [
        ("create_table", (), ("CREATE TABLE", "CREATE INDEX")),
        ("drop_table", ("test_events",), ("DROP INDEX", "DROP TABLE")),
    ],
)
async def test_ddl_operations_run_through_update_ddl(
    store_cls: Any,
    config_cls: type[Any],
    mock_factory: type[MagicMock],
    operation: str,
    existing_tables: tuple[str, ...],
    expected_prefixes: tuple[str, str],
) -> None:
    """create_table and drop_table submit both statements through update_ddl and wait for the operation."""
    config = MagicMock(spec=config_cls)
    config.extension_config = {"events": {"queue_table": "test_events"}}
    config.statement_config = SimpleNamespace(dialect="spanner")
    mock_operation = MagicMock()
    mock_operation.result = mock_factory(return_value=None)
    mock_database = MagicMock()
    _configure_list_tables(mock_database, existing_tables)
    mock_database.update_ddl = mock_factory(return_value=mock_operation)
    config.get_database = mock_factory(return_value=mock_database)

    result = getattr(store_cls(config), operation)()
    if inspect.isawaitable(result):
        await result

    statements = mock_database.update_ddl.call_args.args[0]
    assert all(statement.startswith(prefix) for statement, prefix in zip(statements, expected_prefixes, strict=True))
    mock_operation.result.assert_called_once_with(timeout=None)


@_STORE_VARIANTS
@pytest.mark.parametrize(
    ("operation", "existing_tables"),
    [
        ("create_table", ("test_events",)),
        ("drop_table", ()),
    ],
)
async def test_ddl_operations_are_idempotent_when_table_already_exists_or_missing(
    store_cls: Any,
    config_cls: type[Any],
    mock_factory: type[MagicMock],
    operation: str,
    existing_tables: tuple[str, ...],
) -> None:
    """create_table skips existing tables and drop_table skips missing tables."""
    config = MagicMock(spec=config_cls)
    config.extension_config = {"events": {"queue_table": "test_events"}}
    config.statement_config = SimpleNamespace(dialect="spanner")
    mock_database = MagicMock()
    _configure_list_tables(mock_database, existing_tables)
    mock_database.update_ddl = mock_factory()
    config.get_database = mock_factory(return_value=mock_database)

    result = getattr(store_cls(config), operation)()
    if inspect.isawaitable(result):
        await result

    mock_database.update_ddl.assert_not_called()


@_STORE_VARIANTS
async def test_ddl_operations_reject_other_config_types(
    store_cls: Any, config_cls: type[Any], mock_factory: type[MagicMock]
) -> None:
    """create_table and drop_table raise TypeError when the store's config is not its variant's config."""
    store = store_cls(_mock_spanner_config())

    for operation in ("create_table", "drop_table"):
        with pytest.raises(TypeError, match=config_cls.__name__):
            result = getattr(store, operation)()
            if inspect.isawaitable(result):
                await result
