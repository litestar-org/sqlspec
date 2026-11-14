"""MySQL pool acquisition, checkin rollback, eviction, and shutdown lifecycle regressions."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool


@pytest.mark.parametrize("config_type", [AiomysqlConfig, AsyncmyConfig])
async def test_cancelled_connection_hook_releases_pool_checkout(config_type: Any) -> None:
    connection = MagicMock(closed=False, get_autocommit=MagicMock(return_value=True))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=connection)
    context.__aexit__ = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.acquire.return_value = context
    hook = AsyncMock(side_effect=asyncio.CancelledError)
    config = config_type(connection_instance=pool, driver_features={"on_connection_create": hook})

    with pytest.raises(asyncio.CancelledError):
        async with config.provide_connection():
            pytest.fail("Cancelled hook must prevent connection delivery")

    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("config_type", [AiomysqlConfig, AsyncmyConfig])
async def test_pool_shutdown_failure_is_reported_and_pool_retained(config_type: Any) -> None:
    pool = MagicMock()
    pool.wait_closed = AsyncMock(side_effect=RuntimeError("shutdown failed"))
    config = config_type(connection_instance=pool)

    with pytest.raises(RuntimeError, match="shutdown failed"):
        await config.close_pool()

    assert config.connection_instance is pool


@pytest.mark.parametrize("adapter", ["pymysql", "aiomysql", "asyncmy", "mysqlconnector-sync", "mysqlconnector-async"])
@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit-off", "autocommit-on"])
async def test_mysql_checkin_rolls_back_iff_autocommit_off_ignoring_clear_status_flags(
    adapter: str, autocommit: bool
) -> None:
    """Outermost checkin rolls back whenever autocommit is off, even when server status flags are clear."""
    expected_calls = 0 if autocommit else 1

    if adapter == "pymysql":
        conn = MagicMock(server_status=0, unbuffered_active=False, get_autocommit=MagicMock(return_value=autocommit))
        pool = PyMysqlConnectionPool({}, connection_factory=lambda: conn)
        try:
            with pool.get_connection() as acquired:
                assert acquired is conn
            assert conn.rollback.call_count == expected_calls
            assert pool.checked_out() == 0
        finally:
            pool.close()
        return

    if adapter in {"aiomysql", "asyncmy"}:
        config_cls = AiomysqlConfig if adapter == "aiomysql" else AsyncmyConfig
        conn = MagicMock(
            closed=False,
            server_status=0,
            get_transaction_status=MagicMock(return_value=False),
            get_autocommit=MagicMock(return_value=autocommit),
            rollback=AsyncMock(),
        )
        ctx = MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=None))
        pool_mock = MagicMock(acquire=MagicMock(return_value=ctx))
        config = config_cls(connection_instance=pool_mock)
        async with config.provide_connection() as acquired:
            assert acquired is conn
        assert conn.rollback.await_count == expected_calls
        ctx.__aexit__.assert_awaited_once()
        return

    if adapter == "mysqlconnector-sync":
        raw_cnx = MagicMock()
        conn = MagicMock(_cnx=raw_cnx, autocommit=autocommit, in_transaction=False)
        pool_mock = MagicMock(get_connection=MagicMock(return_value=conn))
        config = MysqlConnectorSyncConfig(connection_config={"pool_reset_session": True}, connection_instance=pool_mock)
        with config.provide_connection() as acquired:
            assert acquired is conn
        assert conn.rollback.call_count == expected_calls
        conn.close.assert_called_once()
        raw_cnx.close.assert_not_called()
        return

    raw_cnx = MagicMock(close=AsyncMock())
    conn = MagicMock(_cnx=raw_cnx, autocommit=autocommit, in_transaction=False, rollback=AsyncMock(), close=AsyncMock())
    pool_mock = MagicMock(get_connection=AsyncMock(return_value=conn))
    config_async = MysqlConnectorAsyncConfig(
        connection_config={"pool_reset_session": True}, connection_instance=pool_mock
    )
    async with config_async.provide_connection() as acquired:
        assert acquired is conn
    assert conn.rollback.await_count == expected_calls
    conn.close.assert_awaited_once()
    raw_cnx.close.assert_not_awaited()


@pytest.mark.parametrize("adapter", ["pymysql", "aiomysql", "asyncmy", "mysqlconnector-sync", "mysqlconnector-async"])
@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "body-exception"])
async def test_mysql_checkin_rollback_failure_evicts_connection_and_preserves_caller_error(
    adapter: str, body_raises: bool
) -> None:
    """Failed rollback closes the physical connection, frees the pool slot, and propagates only on clean exit."""
    rollback_err = RuntimeError("rollback socket broken")
    body_err = ValueError("caller failure")
    expected_exc = ValueError if body_raises else RuntimeError

    if adapter == "pymysql":
        broken = MagicMock(
            server_status=0,
            unbuffered_active=False,
            get_autocommit=MagicMock(return_value=False),
            rollback=MagicMock(side_effect=rollback_err),
        )
        replacement = MagicMock(server_status=0, unbuffered_active=False, get_autocommit=MagicMock(return_value=True))
        pool = PyMysqlConnectionPool({}, connection_factory=MagicMock(side_effect=[broken, replacement]))
        try:
            with pytest.raises(expected_exc):
                with pool.get_connection():
                    if body_raises:
                        raise body_err
            broken.close.assert_called_once()
            assert pool.checked_out() == 0
            assert pool.acquire() is replacement
            pool.release(replacement)
        finally:
            pool.close()
        return

    if adapter in {"aiomysql", "asyncmy"}:
        config_cls = AiomysqlConfig if adapter == "aiomysql" else AsyncmyConfig
        conn = MagicMock(
            closed=False,
            get_autocommit=MagicMock(return_value=False),
            rollback=AsyncMock(side_effect=rollback_err),
            close=MagicMock(),
        )
        ctx = MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=None))
        pool_mock = MagicMock(acquire=MagicMock(return_value=ctx))
        config = config_cls(connection_instance=pool_mock)
        with pytest.raises(expected_exc):
            async with config.provide_session():
                if body_raises:
                    raise body_err
        conn.close.assert_called_once()
        ctx.__aexit__.assert_awaited_once()
        return

    if adapter == "mysqlconnector-sync":
        raw_cnx = MagicMock()
        conn = MagicMock(
            _cnx=raw_cnx, autocommit=False, in_transaction=False, rollback=MagicMock(side_effect=rollback_err)
        )
        pool_mock = MagicMock(get_connection=MagicMock(return_value=conn))
        config_sync = MysqlConnectorSyncConfig(connection_instance=pool_mock)
        with pytest.raises(expected_exc):
            with config_sync.provide_session():
                if body_raises:
                    raise body_err
        conn.close.assert_called_once()
        raw_cnx.close.assert_called_once()
        return

    raw_cnx = MagicMock(close=AsyncMock())
    conn = MagicMock(
        _cnx=raw_cnx,
        autocommit=False,
        in_transaction=False,
        rollback=AsyncMock(side_effect=rollback_err),
        close=AsyncMock(),
    )
    pool_mock = MagicMock(get_connection=AsyncMock(return_value=conn))
    config_async = MysqlConnectorAsyncConfig(connection_instance=pool_mock)
    with pytest.raises(expected_exc):
        async with config_async.provide_session():
            if body_raises:
                raise body_err
    conn.close.assert_awaited_once()
    raw_cnx.close.assert_awaited_once()


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_mysqlconnector_pooled_reset_failure_disconnects_raw_cnx(asynchronous: bool) -> None:
    """When pooled conn.close() raises during reset_session(), the captured _cnx socket is disconnected."""
    reset_err = RuntimeError("reset_session failed")
    if not asynchronous:
        raw_cnx = MagicMock()
        conn = MagicMock(_cnx=raw_cnx, autocommit=False, close=MagicMock(side_effect=reset_err))
        pool_mock = MagicMock(get_connection=MagicMock(return_value=conn))
        config = MysqlConnectorSyncConfig(connection_instance=pool_mock)
        with pytest.raises(RuntimeError, match="reset_session failed"), config.provide_connection():
            pass
        conn.rollback.assert_called_once()
        raw_cnx.close.assert_called_once()
        return

    raw_cnx = MagicMock(close=AsyncMock())
    conn = MagicMock(_cnx=raw_cnx, autocommit=False, rollback=AsyncMock(), close=AsyncMock(side_effect=reset_err))
    pool_mock = MagicMock(get_connection=AsyncMock(return_value=conn))
    config_async = MysqlConnectorAsyncConfig(connection_instance=pool_mock)
    with pytest.raises(RuntimeError, match="reset_session failed"):
        async with config_async.provide_connection():
            pass
    conn.rollback.assert_awaited_once()
    raw_cnx.close.assert_awaited_once()


def test_pymysql_nested_acquire_rolls_back_only_on_outermost_release() -> None:
    """Nested PyMysqlConnectionPool checkouts on the same thread roll back once on outermost release."""
    conn = MagicMock(server_status=0, unbuffered_active=False, get_autocommit=MagicMock(return_value=False))
    pool = PyMysqlConnectionPool({}, connection_factory=lambda: conn)
    try:
        outer = pool.acquire()
        inner = pool.acquire()
        assert outer is inner
        assert pool.checked_out() == 2

        pool.release(inner)
        assert pool.checked_out() == 1
        conn.rollback.assert_not_called()

        pool.release(outer)
        assert pool.checked_out() == 0
        conn.rollback.assert_called_once()
    finally:
        pool.close()
