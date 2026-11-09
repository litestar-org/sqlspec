"""Unit tests for BaseEventQueueStore and validation utilities."""

from typing import get_type_hints

import pytest

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import normalize_event_channel_name, normalize_queue_table_name


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlite_event_schema_reconciliation(asynchronous: bool) -> None:
    """First reconciliation creates the queue; subsequent reconciliation is idempotent."""
    if asynchronous:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.events import AiosqliteEventQueueStore

        async_config = AiosqliteConfig()
        async_store = AiosqliteEventQueueStore(async_config)
        try:
            async with async_config.provide_session() as driver:
                await async_store.prepare_schema_async(driver)
                first = await async_store.reconcile_schema_async(driver)
                second = await async_store.reconcile_schema_async(driver)
                count = await driver.select_value("SELECT COUNT(*) FROM sqlspec_event_queue")
        finally:
            await async_config.close_pool()
    else:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.events import SqliteEventQueueStore

        config = SqliteConfig()
        store = SqliteEventQueueStore(config)
        try:
            with config.provide_session() as sync_driver:
                store.prepare_schema_sync(sync_driver)
                first = store.reconcile_schema_sync(sync_driver)
                second = store.reconcile_schema_sync(sync_driver)
                count = sync_driver.select_value("SELECT COUNT(*) FROM sqlspec_event_queue")
        finally:
            config.close_pool()
    assert first.created_tables == ["sqlspec_event_queue"]
    assert second.created_tables == []
    assert second.added_columns == {}
    assert count == 0


def test_normalize_queue_table_name_simple() -> None:
    """Simple table names pass validation."""
    result = normalize_queue_table_name("events_queue")
    assert result == "events_queue"


def test_normalize_queue_table_name_underscore_prefix() -> None:
    """Underscore-prefixed names are valid identifiers."""
    result = normalize_queue_table_name("_internal_events")
    assert result == "_internal_events"


def test_normalize_queue_table_name_schema_qualified() -> None:
    """Schema-qualified names are validated segment by segment."""
    result = normalize_queue_table_name("app_schema.events_queue")
    assert result == "app_schema.events_queue"


def test_normalize_queue_table_name_three_segments() -> None:
    """Three-part names (catalog.schema.table) are supported."""
    result = normalize_queue_table_name("catalog.schema.events")
    assert result == "catalog.schema.events"


def test_normalize_queue_table_name_invalid_characters() -> None:
    """Names with invalid characters raise EventChannelError."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events-queue")


def test_normalize_queue_table_name_starts_with_number() -> None:
    """Names starting with numbers are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("123_events")


def test_normalize_queue_table_name_empty_segment() -> None:
    """Empty segments in schema-qualified names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("schema..events")


def test_normalize_queue_table_name_special_chars() -> None:
    """Special characters like @ are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events@queue")


def test_normalize_queue_table_name_spaces() -> None:
    """Spaces in names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events queue")


def test_normalize_event_channel_name_simple() -> None:
    """Simple channel names pass validation."""
    result = normalize_event_channel_name("notifications")
    assert result == "notifications"


def test_normalize_event_channel_name_underscore() -> None:
    """Underscore-separated names are valid."""
    result = normalize_event_channel_name("user_events")
    assert result == "user_events"


def test_normalize_event_channel_name_underscore_prefix() -> None:
    """Underscore-prefixed channel names are valid."""
    result = normalize_event_channel_name("_internal")
    assert result == "_internal"


def test_normalize_event_channel_name_alphanumeric() -> None:
    """Alphanumeric channel names are valid."""
    result = normalize_event_channel_name("events2024")
    assert result == "events2024"


def test_normalize_event_channel_name_invalid_hyphen() -> None:
    """Hyphens in channel names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("user-events")


def test_normalize_event_channel_name_dot_not_allowed() -> None:
    """Dots are not allowed in channel names (unlike table names)."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("app.notifications")


def test_normalize_event_channel_name_starts_with_number() -> None:
    """Channel names starting with numbers are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("123events")


def test_normalize_event_channel_name_empty() -> None:
    """Empty channel names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("")


@pytest.mark.parametrize("method_name", ["reconcile_schema_sync", "reconcile_schema_async"])
def test_event_schema_return_type_reflection(method_name: str) -> None:
    """Framework reflection resolves the real schema result before first use."""
    from sqlspec.extensions.events._store import BaseEventQueueStore
    from sqlspec.migrations.schema import SchemaEnsureResult

    assert get_type_hints(getattr(BaseEventQueueStore, method_name))["return"] is SchemaEnsureResult


@pytest.mark.anyio
async def test_base_event_queue_store_companion_index_reconciliation_sync_and_async() -> None:
    """BaseEventQueueStore companion index hooks honor None DDL, None target, and catalog index checks without commit."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.extensions.events._store import BaseEventQueueStore

    cfg = SqliteConfig(connection_config={"database": ":memory:"})

    class NoIndexStore(BaseEventQueueStore[SqliteConfig]):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

        def _index_ddl(self) -> str | None:
            return None

    class SelfIdempotentIndexStore(BaseEventQueueStore[SqliteConfig]):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

    class CatalogCheckedIndexStore(BaseEventQueueStore[SqliteConfig]):
        def _column_types(self) -> tuple[str, str, str]:
            return "TEXT", "TEXT", "TIMESTAMP"

        def _index_existence_target(self) -> tuple[str | None, str]:
            return "app", self.table_name

        def _wrap_create_statement(self, statement: str, object_type: str) -> str:
            if object_type == "index":
                return statement
            return super()._wrap_create_statement(statement, object_type)

    no_idx_store = NoIndexStore(cfg)
    assert no_idx_store.companion_index_statement() is None
    assert len(no_idx_store.get_schema_targets()) == 1
    assert no_idx_store.get_schema_targets()[0].table_name == "sqlspec_event_queue"

    sync_drv_noop = MagicMock()
    async_drv_noop = MagicMock()
    async_drv_noop.execute_script = AsyncMock()
    no_idx_store.reconcile_companion_objects_sync(sync_drv_noop)
    await no_idx_store.reconcile_companion_objects_async(async_drv_noop)
    sync_drv_noop.execute_script.assert_not_called()
    async_drv_noop.execute_script.assert_not_awaited()

    direct_store = SelfIdempotentIndexStore(cfg)
    assert direct_store.companion_index_statement() is not None
    assert "CREATE INDEX IF NOT EXISTS" in str(direct_store.companion_index_statement())
    sync_drv_direct = MagicMock()
    async_drv_direct = MagicMock()
    async_drv_direct.execute_script = AsyncMock()
    direct_store.reconcile_companion_objects_sync(sync_drv_direct)
    await direct_store.reconcile_companion_objects_async(async_drv_direct)
    sync_drv_direct.execute_script.assert_called_once()
    sync_drv_direct.commit.assert_not_called()
    async_drv_direct.execute_script.assert_awaited_once()
    async_drv_direct.commit.assert_not_called()

    catalog_store = CatalogCheckedIndexStore(cfg)
    expected_idx = catalog_store._index_name()

    sync_drv_present = MagicMock()
    sync_drv_present.data_dictionary.get_indexes.return_value = [
        {"index_name": expected_idx, "table_name": "sqlspec_event_queue", "schema_name": "app"}
    ]
    catalog_store.reconcile_companion_objects_sync(sync_drv_present)
    sync_drv_present.data_dictionary.get_indexes.assert_called_once_with(
        sync_drv_present, table="sqlspec_event_queue", schema="app"
    )
    sync_drv_present.execute_script.assert_not_called()
    sync_drv_present.commit.assert_not_called()

    sync_drv_missing = MagicMock()
    sync_drv_missing.data_dictionary.get_indexes.return_value = []
    catalog_store.reconcile_companion_objects_sync(sync_drv_missing)
    sync_drv_missing.execute_script.assert_called_once()
    sync_drv_missing.commit.assert_not_called()

    async_drv_present = MagicMock()
    async_drv_present.data_dictionary.get_indexes = AsyncMock(
        return_value=[{"index_name": expected_idx, "table_name": "sqlspec_event_queue", "schema_name": "app"}]
    )
    async_drv_present.execute_script = AsyncMock()
    await catalog_store.reconcile_companion_objects_async(async_drv_present)
    async_drv_present.execute_script.assert_not_awaited()
    async_drv_present.commit.assert_not_called()

    async_drv_missing = MagicMock()
    async_drv_missing.data_dictionary.get_indexes = AsyncMock(return_value=[])
    async_drv_missing.execute_script = AsyncMock()
    await catalog_store.reconcile_companion_objects_async(async_drv_missing)
    async_drv_missing.execute_script.assert_awaited_once()
    async_drv_missing.commit.assert_not_called()


@pytest.mark.anyio
async def test_events_reconcile_backend_gating(monkeypatch: pytest.MonkeyPatch) -> None:
    """Events reconcile skips pure notify and reconciles poll_queue, notify_queue, aq, and txeventq with Oracle 23ai cache priming."""
    import inspect
    from unittest.mock import AsyncMock, MagicMock

    pytest.importorskip("asyncpg")
    pytest.importorskip("oracledb")

    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.oracledb import OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext

    notify_cfg = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        extension_config={"events": {"migration_mode": "reconcile"}},
    )
    notify_driver = MagicMock()
    notify_driver.execute_script = AsyncMock()
    notify_driver.data_dictionary.get_tables = AsyncMock()
    notify_coro = events_reconcile(MigrationContext(config=notify_cfg, driver=notify_driver))
    assert inspect.isawaitable(notify_coro)
    await notify_coro
    notify_driver.execute_script.assert_not_awaited()
    notify_driver.data_dictionary.get_tables.assert_not_awaited()

    notify_queue_cfg = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/test"},
        extension_config={"events": {"backend": "notify_queue", "migration_mode": "reconcile"}},
    )
    nq_driver = MagicMock()
    nq_driver.execute_script = AsyncMock()
    nq_driver.data_dictionary.get_tables = AsyncMock(return_value=[])
    nq_coro = events_reconcile(MigrationContext(config=notify_queue_cfg, driver=nq_driver))
    assert inspect.isawaitable(nq_coro)
    await nq_coro
    assert nq_driver.execute_script.await_count == 2
    nq_driver.commit.assert_not_called()

    monkeypatch.setattr(OracleSyncConfig, "provide_session", MagicMock(side_effect=AssertionError("nested session")))
    for backend_name in ("aq", "txeventq", "poll_queue"):
        ora_cfg = OracleSyncConfig(
            connection_config={"dsn": "localhost/xe"},
            extension_config={"events": {"backend": backend_name, "migration_mode": "reconcile"}},
        )
        call_order: list[str] = []

        def _get_version(drv: object, _order: list[str] = call_order) -> OracleVersionInfo:
            _order.append("get_version")
            return OracleVersionInfo(23, 4, 0, compatible="23.0.0")

        def _get_tables(drv: object, schema: str | None = None, _order: list[str] = call_order) -> list[object]:
            _order.append("get_tables")
            return []

        ora_driver = MagicMock()
        ora_driver.data_dictionary.get_version.side_effect = _get_version
        ora_driver.data_dictionary.get_tables.side_effect = _get_tables
        ora_driver.data_dictionary.get_indexes.return_value = []
        executed_scripts: list[str] = []
        ora_driver.execute_script.side_effect = lambda sql, *_a, _scripts=executed_scripts, **_kw: _scripts.append(
            str(sql)
        )

        outcome = events_reconcile(MigrationContext(config=ora_cfg, driver=ora_driver))
        assert outcome is None
        assert call_order[:2] == ["get_version", "get_tables"]
        assert any("payload_json JSON NOT NULL" in s for s in executed_scripts)
        assert any("SQLCODE NOT IN (-955, -1408)" in s for s in executed_scripts)
        ora_driver.commit.assert_not_called()


def test_events_reconcile_refuses_unresolvable_adapter_and_unclassified_flightsql() -> None:
    """Events reconcile refuses unresolvable adapters and unclassified FlightSQL before driver calls."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    pytest.importorskip("adbc_driver_manager")
    from sqlspec.adapters.adbc import AdbcConfig
    from sqlspec.exceptions import MigrationError
    from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
    from sqlspec.migrations.context import MigrationContext

    fake_driver = MagicMock()
    unresolvable = SimpleNamespace(extension_config={"events": {"migration_mode": "reconcile"}})
    with pytest.raises(MigrationError, match='migration_mode="versioned"'):
        events_reconcile(MigrationContext(config=unresolvable, driver=fake_driver))
    fake_driver.execute_script.assert_not_called()

    flightsql_cfg = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://localhost:8815"},
        extension_config={"events": {"migration_mode": "reconcile"}},
    )
    with pytest.raises(MigrationError, match='migration_mode="versioned"'):
        events_reconcile(MigrationContext(config=flightsql_cfg, driver=fake_driver))
    fake_driver.execute_script.assert_not_called()
