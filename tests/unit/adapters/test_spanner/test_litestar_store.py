from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

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
    """Verify _delete_all uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._delete_all()  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


def test_delete_expired_uses_run_in_transaction() -> None:
    """Verify _delete_expired uses database.run_in_transaction for write operations."""
    mock_db = _mock_database()

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.get_database.return_value = mock_db

    store = SpannerSyncStore(config)
    store._delete_expired()  # pyright: ignore

    mock_db.run_in_transaction.assert_called_once()


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
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._get("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is None


def test_exists_uses_snapshot_session() -> None:
    """Verify _exists uses snapshot session for read operations."""
    driver = MagicMock()
    driver.select_one_or_none.return_value = {"1": 1}
    cm = _context_manager_yielding(driver)

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess"}}
    config.provide_session.return_value = cm

    store = SpannerSyncStore(config)
    result = store._exists("s1")  # pyright: ignore

    config.provide_session.assert_called_once_with()
    assert result is True


def _mock_async_database() -> MagicMock:
    db = MagicMock()
    txn = MagicMock()
    txn.execute_update = AsyncMock(return_value=1)

    async def _run_in_txn(func: Any) -> Any:
        return await func(txn)

    db.run_in_transaction = AsyncMock(side_effect=_run_in_txn)
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
    assert db.run_in_transaction.await_count == 3

    deleted = await store.delete_expired()
    assert deleted == 1
    assert db.run_in_transaction.await_count == 4


async def test_async_store_get_exists_expires_in() -> None:
    """Verify SpannerAsyncStore read operations use async provide_session and optional renewal."""
    db = _mock_async_database()
    future_time = datetime.now(timezone.utc) + timedelta(seconds=120)
    driver = MagicMock()
    driver.select_one_or_none = AsyncMock(return_value={"data": bytes_to_spanner(b"abc"), "expires_at": future_time})

    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "sess", "shard_count": 4}}
    config.get_database = AsyncMock(return_value=db)
    config.provide_session.side_effect = lambda: _async_context_manager_yielding(driver)

    store = SpannerAsyncStore(config)
    val = await store.get("s1", renew_for=60)
    assert val == b"abc"
    db.run_in_transaction.assert_awaited_once()

    exists = await store.exists("s1")
    assert exists is True

    ttl = await store.expires_in("s1")
    assert ttl in range(1, 121)
