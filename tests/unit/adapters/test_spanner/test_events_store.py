# pyright: reportPrivateUsage=false
"""Unit tests for SpannerSyncEventQueueStore and SpannerAsyncEventQueueStore."""

import inspect
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore


def _mock_spanner_config() -> MagicMock:
    """Create a mock SpannerSyncConfig."""
    config = MagicMock()
    config.extension_config = {"events": {"queue_table": "test_events"}}
    return config


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


def test_table_ddl_inline_primary_key() -> None:
    """Verify PRIMARY KEY is declared inline (Spanner requirement)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._table_ddl()

    assert sql.endswith(") PRIMARY KEY (event_id)")


def test_index_ddl_no_if_not_exists() -> None:
    """Verify index creation has no IF NOT EXISTS (Spanner restriction)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._index_ddl()

    assert sql is not None
    assert "IF NOT EXISTS" not in sql
    assert "CREATE INDEX" in sql


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
    ("operation", "expected_prefixes"),
    [("create_table", ("CREATE TABLE", "CREATE INDEX")), ("drop_table", ("DROP INDEX", "DROP TABLE"))],
)
async def test_ddl_operations_run_through_update_ddl(
    store_cls: Any,
    config_cls: type[Any],
    mock_factory: type[MagicMock],
    operation: str,
    expected_prefixes: tuple[str, str],
) -> None:
    """create_table and drop_table submit both statements through update_ddl and wait for the operation."""
    config = MagicMock(spec=config_cls)
    config.extension_config = {"events": {"queue_table": "test_events"}}
    mock_operation = MagicMock()
    mock_operation.result = mock_factory(return_value=None)
    mock_database = MagicMock()
    mock_database.update_ddl = mock_factory(return_value=mock_operation)
    config.get_database = mock_factory(return_value=mock_database)

    result = getattr(store_cls(config), operation)()
    if inspect.isawaitable(result):
        await result

    statements = mock_database.update_ddl.call_args.args[0]
    assert all(statement.startswith(prefix) for statement, prefix in zip(statements, expected_prefixes, strict=True))
    mock_operation.result.assert_called_once_with(timeout=None)


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
