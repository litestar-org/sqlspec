# pyright: reportPrivateUsage=false
"""Unit tests for SpannerSyncEventQueueStore."""

from unittest.mock import MagicMock, patch

from sqlspec.adapters.spanner.events import SpannerSyncEventQueueStore


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


def test_build_create_table_sql_uses_string_types() -> None:
    """Verify CREATE TABLE uses STRING instead of VARCHAR."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._build_create_table_sql()

    assert "STRING(64)" in sql
    assert "STRING(128)" in sql
    assert "STRING(32)" in sql
    assert "VARCHAR" not in sql


def test_build_create_table_sql_uses_int64() -> None:
    """Verify CREATE TABLE uses INT64 instead of INTEGER."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._build_create_table_sql()

    assert "INT64" in sql
    assert "INTEGER" not in sql


def test_build_create_table_sql_no_default_clauses() -> None:
    """Verify CREATE TABLE has no DEFAULT clauses (Spanner restriction)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._build_create_table_sql()

    assert "DEFAULT" not in sql


def test_build_create_table_sql_inline_primary_key() -> None:
    """Verify PRIMARY KEY is declared inline (Spanner requirement)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._build_create_table_sql()

    assert sql.endswith(") PRIMARY KEY (event_id)")


def test_build_index_sql_no_if_not_exists() -> None:
    """Verify index creation has no IF NOT EXISTS (Spanner restriction)."""
    config = _mock_spanner_config()
    store = SpannerSyncEventQueueStore(config)

    sql = store._build_index_sql()

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


def test_create_table_uses_update_ddl() -> None:
    """Verify create_table calls database.update_ddl with statements."""
    from sqlspec.adapters.spanner.config import SpannerSyncConfig

    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"events": {"queue_table": "test_events"}}

    mock_database = MagicMock()
    mock_operation = MagicMock()
    mock_database.update_ddl.return_value = mock_operation
    config.get_database.return_value = mock_database

    store = SpannerSyncEventQueueStore(config)

    with patch.object(store, "_config", config):
        store.create_table()

    mock_database.update_ddl.assert_called_once()
    call_args = mock_database.update_ddl.call_args[0][0]
    assert len(call_args) == 2
    mock_operation.result.assert_called_once()


def test_drop_table_uses_update_ddl() -> None:
    """Verify drop_table calls database.update_ddl with statements."""
    from sqlspec.adapters.spanner.config import SpannerSyncConfig

    config = MagicMock(spec=SpannerSyncConfig)
    config.extension_config = {"events": {"queue_table": "test_events"}}

    mock_database = MagicMock()
    mock_operation = MagicMock()
    mock_database.update_ddl.return_value = mock_operation
    config.get_database.return_value = mock_database

    store = SpannerSyncEventQueueStore(config)

    with patch.object(store, "_config", config):
        store.drop_table()

    mock_database.update_ddl.assert_called_once()
    call_args = mock_database.update_ddl.call_args[0][0]
    assert len(call_args) == 2
    assert "DROP INDEX" in call_args[0]
    assert "DROP TABLE" in call_args[1]
    mock_operation.result.assert_called_once()
