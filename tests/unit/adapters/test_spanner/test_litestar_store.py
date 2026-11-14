from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.spanner.litestar import SpannerAsyncStore, SpannerSyncStore
from sqlspec.adapters.spanner.type_converter import bytes_to_spanner


def _mock_database() -> MagicMock:
    """Create a mock database that captures run_in_transaction calls."""
    db = MagicMock()
    db.run_in_transaction = MagicMock(side_effect=lambda func: func(MagicMock()))
    return db


def test_set_uses_run_in_transaction() -> None:
    """Verify _set uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._set("s1", b"data", None)  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_uses_run_in_transaction() -> None:
    """Verify _delete uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._delete("s1")  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_all_uses_run_in_transaction() -> None:
    """Verify _delete_all uses execute_partitioned_dml."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._delete_all()  # pyright: ignore

    mock_db.execute_partitioned_dml.assert_called_once()


def test_delete_expired_uses_run_in_transaction() -> None:
    """Verify _delete_expired uses execute_partitioned_dml."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._delete_expired()  # pyright: ignore

    mock_db.execute_partitioned_dml.assert_called_once()


def test_set_single_dml_upsert() -> None:
    """Verify _set issues a single INSERT OR UPDATE DML statement."""
    executed_statements: list[str] = []

    class _MockTxn:
        def execute_update(self, sql: str, params: Any = None, param_types: Any = None) -> int:
            executed_statements.append(sql)
            return 1

    mock_db = MagicMock()
    mock_db.run_in_transaction = MagicMock(side_effect=lambda func: func(_MockTxn()))

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._set("s1", b"data", None)  # pyright: ignore

    assert len(executed_statements) == 1
    assert "INSERT OR UPDATE INTO" in executed_statements[0]


def test_row_deletion_policy_ddl() -> None:
    """Verify _table_ddl includes ROW DELETION POLICY when enabled."""
    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess", "enable_row_deletion_policy": True}}
    store = SpannerSyncStore(config)
    ddl = store._table_ddl()
    assert "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))" in ddl


def test_litestar_store_param_types_cached() -> None:
    """_get_param_types returns module-level cached constants."""
    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    store = SpannerSyncStore(config)
    t1 = store._get_param_types(session_id=True, data=True, expires_at=True)
    t2 = store._get_param_types(session_id=True, data=True, expires_at=True)
    assert t1 is t2


def _context_manager_yielding(value: Any) -> Any:
    class _Ctx:
        def __enter__(self) -> Any:
            return value

        def __exit__(self, *_: Any) -> None:
            pass

    return _Ctx()


def _async_context_manager_yielding(value: Any) -> Any:
    class _AsyncCtx:
        async def __aenter__(self) -> Any:
            return value

        async def __aexit__(self, *_: Any) -> None:
            pass

    return _AsyncCtx()


def test_get_uses_snapshot_session() -> None:
    """Verify _get uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = None
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_read_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._get("s1")  # pyright: ignore

    config.provide_read_session.assert_called_once_with()
    assert result is None


def test_exists_uses_snapshot_session() -> None:
    """Verify _exists uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"1": 1}
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_read_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._exists("s1")  # pyright: ignore

    config.provide_read_session.assert_called_once_with()
    assert result is True


def _mock_async_database() -> MagicMock:
    db = MagicMock()
    txn = MagicMock()
    txn.execute_update = AsyncMock(return_value=1)

    async def _run_in_txn(func: Any) -> Any:
        return await func(txn)

    db.run_in_transaction = AsyncMock(side_effect=_run_in_txn)
    db.execute_partitioned_dml = AsyncMock(return_value=1)
    db._txn = txn
    return db


async def test_async_store_create_table() -> None:
    """Verify SpannerAsyncStore.create_table consumes async list_tables and awaits update_ddl().result()."""
    db = _mock_async_database()

    async def _list_tables() -> Any:
        if False:
            yield None

    db.list_tables.side_effect = _list_tables
    op_mock = MagicMock()
    op_mock.result = AsyncMock(return_value=None)
    db.update_ddl = AsyncMock(return_value=op_mock)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess", "manage_schema": True, "create_schema": True}}
    config.get_database = AsyncMock(return_value=db)
    driver = MagicMock()
    config.provide_session.return_value = _async_context_manager_yielding(driver)

    store = SpannerAsyncStore(config)
    with patch.object(SpannerAsyncStore, "reconcile_schema", new_callable=AsyncMock) as mock_reconcile:
        await store.create_table()

    db.update_ddl.assert_awaited_once()
    op_mock.result.assert_awaited_once_with(timeout=300)
    mock_reconcile.assert_awaited_once_with(assume_existing=True)


async def test_async_store_set_delete_delete_all_delete_expired() -> None:
    """Verify SpannerAsyncStore write operations use async run_in_transaction."""
    db = _mock_async_database()
    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database = AsyncMock(return_value=db)

    store = SpannerAsyncStore(config)
    await store.set("s1", b"payload", expires_in=60)
    assert db.run_in_transaction.await_count == 1

    await store.delete("s1")
    assert db.run_in_transaction.await_count == 2

    await store.delete_all()
    assert db.execute_partitioned_dml.await_count == 1

    deleted = await store.delete_expired()
    assert deleted == 1
    assert db.execute_partitioned_dml.await_count == 2


async def test_async_store_get_exists_expires_in() -> None:
    """Verify SpannerAsyncStore read operations use async provide_read_session and optional renewal."""
    db = _mock_async_database()
    future_time = datetime.now(UTC) + timedelta(seconds=120)
    driver = MagicMock()
    driver.select_one_or_none = AsyncMock(return_value={"data": bytes_to_spanner(b"abc"), "expires_at": future_time})

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess", "shard_count": 4}}
    config.get_database = AsyncMock(return_value=db)
    config.provide_read_session.side_effect = lambda: _async_context_manager_yielding(driver)

    store = SpannerAsyncStore(config)
    val = await store.get("s1", renew_for=60)
    assert val == b"abc"
    db.run_in_transaction.assert_awaited_once()

    exists = await store.exists("s1")
    assert exists is True

    ttl = await store.expires_in("s1")
    assert ttl in range(1, 121)


def test_validate_builtin_reconcile_adapter_accepts_spanner() -> None:
    """Built-in extension reconciliation guard accepts Spanner sync and async configs."""
    from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
    from sqlspec.migrations.utils import validate_builtin_reconcile_adapter

    conn = {"project": "test-project", "instance_id": "inst", "database_id": "db"}
    assert validate_builtin_reconcile_adapter(SpannerSyncConfig(connection_config=conn)) == "spanner"
    assert validate_builtin_reconcile_adapter(SpannerAsyncConfig(connection_config=conn)) == "spanner"


async def test_spanner_litestar_companion_index_reconcile_sync_and_async() -> None:
    """Spanner session stores repair missing expiry indexes once and skip uppercase catalog matches."""
    import inspect

    from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
    from sqlspec.config import ExtensionConfigs
    from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
    from sqlspec.migrations.context import MigrationContext

    conn: dict[str, Any] = {"project": "test-project", "instance_id": "inst", "database_id": "db"}
    litestar_ext: dict[str, Any] = {"session_table": "sess", "shard_count": 4, "migration_mode": "reconcile"}
    ext: ExtensionConfigs = {"litestar": litestar_ext}
    sync_cfg = SpannerSyncConfig(connection_config=conn, extension_config=ext)
    async_cfg = SpannerAsyncConfig(connection_config=conn, extension_config=ext)
    sync_store = SpannerSyncStore(sync_cfg)
    async_store = SpannerAsyncStore(async_cfg)

    cols = [
        {"column_name": "session_id", "data_type": "STRING(128)", "nullable": False},
        {"column_name": "data", "data_type": "BYTES(MAX)", "nullable": False},
        {"column_name": "expires_at", "data_type": "TIMESTAMP", "nullable": True},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "nullable": False},
        {"column_name": "updated_at", "data_type": "TIMESTAMP", "nullable": False},
        {"column_name": "shard_id", "data_type": "INT64", "nullable": True},
    ]

    sync_drv = MagicMock()
    sync_drv.data_dictionary.get_tables.return_value = [{"table_name": "sess"}]
    sync_drv.data_dictionary.get_columns.return_value = cols
    sync_drv.data_dictionary.get_indexes.return_value = []
    assert litestar_reconcile(MigrationContext(config=sync_cfg, driver=sync_drv)) is None
    sync_drv.execute_script.assert_called_once_with(sync_store._index_ddl())
    sync_drv.commit.assert_not_called()

    sync_drv.execute_script.reset_mock()
    sync_drv.data_dictionary.get_indexes.return_value = [{"index_name": "IDX_SESS_EXPIRES_AT", "table_name": "SESS"}]
    sync_store.reconcile_companion_objects_sync(sync_drv)
    sync_drv.execute_script.assert_not_called()

    async_drv = MagicMock()
    async_drv.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "sess"}])
    async_drv.data_dictionary.get_columns = AsyncMock(return_value=cols)
    async_drv.data_dictionary.get_indexes = AsyncMock(return_value=[])
    async_drv.execute_script = AsyncMock()
    async_drv.commit = AsyncMock()
    coro = litestar_reconcile(MigrationContext(config=async_cfg, driver=async_drv))
    assert inspect.isawaitable(coro)
    await coro
    async_drv.execute_script.assert_awaited_once_with(async_store._index_ddl())
    async_drv.commit.assert_not_awaited()

    async_drv.execute_script.reset_mock()
    async_drv.data_dictionary.get_indexes = AsyncMock(
        return_value=[{"index_name": "IDX_SESS_EXPIRES_AT", "table_name": "SESS"}]
    )
    await async_store.reconcile_companion_objects_async(async_drv)
    async_drv.execute_script.assert_not_awaited()


def test_spanner_litestar_sharded_schema_target_and_strict_refusal() -> None:
    """Sharded Spanner session target tracks PK/generated columns and refuses missing shard_id."""
    import pytest

    from sqlspec.adapters.spanner import SpannerSyncConfig
    from sqlspec.exceptions import MigrationError
    from sqlspec.migrations.schema import ensure_schema_sync

    cfg = SpannerSyncConfig(
        connection_config={"project": "test-project", "instance_id": "inst", "database_id": "db"},
        extension_config={
            "litestar": {
                "session_table": "sess",
                "shard_count": 4,
                "enable_row_deletion_policy": True,
                "table_options": "optimizer_version=1",
            }
        },
    )
    store = SpannerSyncStore(cfg)
    target = store.get_schema_targets()[0]

    assert target.constraint_columns == frozenset({"shard_id", "session_id"})
    assert "shard_id" in target.unsafe_add_columns
    assert "ROW DELETION POLICY" in str(target.create_statement)

    drv = MagicMock()
    drv.data_dictionary.get_tables.return_value = [{"table_name": "sess"}]
    drv.data_dictionary.get_columns.return_value = [
        {"column_name": "session_id", "data_type": "STRING(128)", "nullable": False},
        {"column_name": "data", "data_type": "BYTES(MAX)", "nullable": False},
        {"column_name": "expires_at", "data_type": "TIMESTAMP", "nullable": True},
        {"column_name": "created_at", "data_type": "TIMESTAMP", "nullable": False},
        {"column_name": "updated_at", "data_type": "TIMESTAMP", "nullable": False},
    ]
    with pytest.raises(MigrationError, match=r'shard_id.*migration_mode="versioned"'):
        ensure_schema_sync(drv, [target], manage_schema=True, create_schema=True, commit=False, strict=True)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("expires_in", "expect_future"),
    [
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(0, False, id="zero-expires-immediately"),
        pytest.param(-5, False, id="negative-expires-immediately"),
    ],
)
async def test_spanner_litestar_set_ttl_precision_and_immediate_expiry(
    mode: str, expires_in: int | timedelta, expect_future: bool
) -> None:
    """Spanner Litestar stores preserve sub-second TTLs and store past timestamps for non-positive expires_in."""
    captured_params: list[dict[str, Any]] = []
    before = datetime.now(UTC)

    if mode == "sync":

        class _SyncTxn:
            def execute_update(self, _sql: str, params: Any = None, param_types: Any = None) -> int:
                _ = param_types
                captured_params.append(dict(params or {}))
                return 1

        sync_db = MagicMock()
        sync_db.run_in_transaction = MagicMock(side_effect=lambda fn: fn(_SyncTxn()))
        sync_cfg = MagicMock()
        sync_cfg.extension_config = {"litestar": {"session_table": "sess"}}
        sync_cfg.get_database.return_value = sync_db
        sync_store = SpannerSyncStore(sync_cfg)
        sync_store._set("s1", b"payload", expires_in=expires_in)  # pyright: ignore[reportPrivateUsage]
    else:
        async_db = _mock_async_database()
        async_cfg = MagicMock()
        async_cfg.extension_config = {"litestar": {"session_table": "sess"}}
        async_cfg.get_database = AsyncMock(return_value=async_db)
        async_store = SpannerAsyncStore(async_cfg)
        await async_store.set("s1", b"payload", expires_in=expires_in)
        captured_params.append(dict(async_db._txn.execute_update.call_args.kwargs["params"]))

    after = datetime.now(UTC)
    bound_expires_at = captured_params[0]["expires_at"]
    assert isinstance(bound_expires_at, datetime)
    if expect_future:
        assert before + timedelta(milliseconds=200) <= bound_expires_at <= after + timedelta(milliseconds=300)
    else:
        assert before - timedelta(microseconds=1) <= bound_expires_at <= after - timedelta(microseconds=1)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("renew_for", "expected_txn_calls"),
    [
        pytest.param(None, 0, id="none"),
        pytest.param(0, 0, id="zero"),
        pytest.param(-1, 0, id="negative"),
        pytest.param(timedelta(0), 0, id="zero-timedelta"),
        pytest.param(30, 1, id="positive"),
        pytest.param(timedelta(milliseconds=200), 1, id="subsecond-positive"),
    ],
)
async def test_spanner_litestar_get_renews_only_for_positive_duration(
    mode: str, renew_for: int | timedelta | None, expected_txn_calls: int
) -> None:
    """Spanner Litestar get renews expires_at only when renew_for is positive."""
    future_time = datetime.now(UTC) + timedelta(seconds=120)
    row = {"data": bytes_to_spanner(b"abc"), "expires_at": future_time}

    if mode == "sync":
        sync_db = _mock_database()
        sync_driver = MagicMock()
        sync_driver.select_one_or_none.return_value = row
        sync_cfg = MagicMock()
        sync_cfg.extension_config = {"litestar": {"session_table": "sess"}}
        sync_cfg.get_database.return_value = sync_db
        sync_cfg.provide_read_session.side_effect = lambda: _context_manager_yielding(sync_driver)
        sync_store = SpannerSyncStore(sync_cfg)
        assert sync_store._should_renew(renew_for) is (expected_txn_calls == 1)  # pyright: ignore[reportPrivateUsage]
        result = sync_store._get("s1", renew_for=renew_for)  # pyright: ignore[reportPrivateUsage]
        assert result == b"abc"
        assert sync_db.run_in_transaction.call_count == expected_txn_calls
    else:
        async_db = _mock_async_database()
        async_driver = MagicMock()
        async_driver.select_one_or_none = AsyncMock(return_value=row)
        async_cfg = MagicMock()
        async_cfg.extension_config = {"litestar": {"session_table": "sess"}}
        async_cfg.get_database = AsyncMock(return_value=async_db)
        async_cfg.provide_read_session.side_effect = lambda: _async_context_manager_yielding(async_driver)
        async_store = SpannerAsyncStore(async_cfg)
        assert async_store._should_renew(renew_for) is (expected_txn_calls == 1)  # pyright: ignore[reportPrivateUsage]
        result = await async_store.get("s1", renew_for=renew_for)
        assert result == b"abc"
        assert async_db.run_in_transaction.await_count == expected_txn_calls
