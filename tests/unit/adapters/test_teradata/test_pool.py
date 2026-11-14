"""Exercise native pool ownership and cleanup through public leases."""

import os
import sys
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata.pool import TeradataConnectionPool
from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError
from sqlspec.utils.module_loader import reset_dependency_cache


@pytest.fixture
def sdk_connections(monkeypatch: pytest.MonkeyPatch) -> Generator[list[Any], None, None]:
    connections: list[Any] = []

    def connect(**kwargs: Any) -> Any:
        connection = MagicMock()
        connection.autocommit = True
        connections.append(connection)
        return connection

    monkeypatch.setitem(sys.modules, "teradatasql", SimpleNamespace(connect=connect))
    reset_dependency_cache("teradatasql")
    try:
        yield connections
    finally:
        reset_dependency_cache("teradatasql")


def test_pool_capacity_is_exclusive_and_times_out(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"}, max_size=1, timeout=0.01)
    connection = pool.acquire()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(pool.acquire)
            with pytest.raises(ConnectionTimeoutError, match="Timed out"):
                future.result(timeout=2)
        assert pool.size() == 1
        assert pool.checked_out() == 1
        assert len(sdk_connections) == 1
        pool.release(connection)
        assert pool.acquire() is connection
        pool.release(connection)
    finally:
        pool.close()


def test_pool_release_rolls_back_and_restores_autocommit(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"})
    connection = pool.acquire()
    connection.autocommit = False
    pool.release(connection)
    connection.rollback.assert_called_once_with()
    assert connection.autocommit is True
    assert pool.checked_out() == 0
    assert pool.acquire() is connection
    pool.release(connection)
    pool.close()
    connection.close.assert_called_once_with()


def test_pool_close_retires_leased_connections_on_return(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"})
    connection = pool.acquire()
    pool.close()
    connection.close.assert_not_called()
    assert pool.is_closed is True
    with pytest.raises(DatabaseConnectionError, match="closed"):
        pool.acquire()
    pool.release(connection)
    connection.close.assert_called_once_with()
    assert pool.size() == 0


def test_pool_refuses_forked_process_without_touching_connection(
    sdk_connections: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = TeradataConnectionPool({"host": "localhost"})
    connection = pool.acquire()
    parent_pid = os.getpid()
    with monkeypatch.context() as context:
        context.setattr(os, "getpid", lambda: parent_pid + 1)
        for operation in (pool.acquire, pool.close, lambda: pool.release(connection)):
            with pytest.raises(DatabaseConnectionError, match="forked"):
                operation()
    connection.close.assert_not_called()
    pool.release(connection)
    pool.close()


def test_pool_callback_failure_closes_handle_and_restores_capacity(sdk_connections: list[Any]) -> None:
    calls = 0

    def initialize(connection: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("initialization failed")

    pool = TeradataConnectionPool({"host": "localhost"}, max_size=1, on_connection_create=initialize)
    with pytest.raises(ValueError, match="initialization failed"):
        pool.acquire()
    sdk_connections[0].close.assert_called_once_with()
    assert pool.size() == 0
    connection = pool.acquire()
    assert connection is sdk_connections[1]
    pool.release(connection)
    pool.close()


def test_pool_failed_rollback_discards_connection(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"}, max_size=1)
    connection = pool.acquire()
    connection.autocommit = False
    connection.rollback.side_effect = RuntimeError("connection lost")
    pool.release(connection)
    connection.close.assert_called_once_with()
    assert pool.size() == 0
    replacement = pool.acquire()
    assert replacement is sdk_connections[1]
    pool.release(replacement)
    pool.close()


def test_pool_nested_context_reuses_lease_and_exception_discards(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"}, max_size=1)
    with pytest.raises(ValueError, match="application"):
        with pool.get_connection() as outer:
            with pool.get_connection() as inner:
                assert inner is outer
                assert pool.checked_out() == 1
            raise ValueError("application")
    sdk_connections[0].close.assert_called_once_with()
    assert pool.size() == 0
    pool.close()


def test_pool_close_prevents_nested_lease_reuse(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"host": "localhost"}, max_size=1)
    with pool.get_connection() as connection:
        pool.close()
        with pytest.raises(DatabaseConnectionError, match="retired"):
            with pool.get_connection():
                pytest.fail("closed pool yielded a retired lease")
        connection.close.assert_not_called()
    connection.close.assert_called_once_with()
    assert pool.size() == 0


@pytest.mark.parametrize(
    "options",
    [
        {"min_size": True},
        {"max_size": False},
        {"min_size": -1},
        {"max_size": 0},
        {"min_size": 3, "max_size": 2},
        {"timeout": float("nan")},
        {"timeout": -1},
        {"recycle_seconds": -1},
        {"health_check_interval": -1},
    ],
)
def test_pool_rejects_invalid_bounds(options: dict[str, Any]) -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    with pytest.raises(ImproperConfigurationError):
        TeradataConnectionPool({}, **options)


def test_pool_defers_missing_dependency_until_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.exceptions import MissingDependencyError

    monkeypatch.setitem(sys.modules, "teradatasql", None)
    reset_dependency_cache("teradatasql")
    try:
        pool = TeradataConnectionPool({})
        assert pool.size() == 0
        with pytest.raises(MissingDependencyError):
            pool.acquire()
        assert pool.size() == 0
        pool.close()
    finally:
        reset_dependency_cache("teradatasql")


@pytest.mark.parametrize("failed_ping", [False, True])
def test_pool_health_check_reuses_or_replaces_connection(
    sdk_connections: list[Any], monkeypatch: pytest.MonkeyPatch, failed_ping: bool
) -> None:
    clock = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    pool = TeradataConnectionPool({}, health_check_interval=5, recycle_seconds=0)
    first = pool.acquire()
    pool.release(first)
    cursor = first.cursor.return_value
    cursor.reset_mock()
    if failed_ping:
        cursor.execute.side_effect = RuntimeError("connection lost")
    clock[0] = 110.0
    connection = pool.acquire()
    cursor.execute.assert_called_once_with("SELECT 1")
    cursor.close.assert_called_once_with()
    if failed_ping:
        first.close.assert_called_once_with()
        assert connection is sdk_connections[1]
    else:
        assert connection is first
        cursor.fetchone.assert_called_once_with()
    cursor.execute.side_effect = None
    pool.release(connection)
    pool.close()


def test_pool_recycles_expired_connection_without_ping(
    sdk_connections: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    pool = TeradataConnectionPool({}, recycle_seconds=5, health_check_interval=1)
    first = pool.acquire()
    pool.release(first)
    first.cursor.reset_mock()
    clock[0] = 110.0
    second = pool.acquire()
    assert second is sdk_connections[1]
    first.close.assert_called_once_with()
    first.cursor.assert_not_called()
    pool.release(second)
    pool.close()


def test_pool_applies_query_band_and_manual_commit_baseline(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({"autocommit": False}, session_query_band={"app": "O'Reilly"})
    connection = pool.acquire()
    assert connection.autocommit is False
    connection.cursor.return_value.execute.assert_called_once_with("SET QUERY_BAND = 'app=O''Reilly;' FOR SESSION;")
    connection.cursor.return_value.close.assert_called_once_with()
    connection.commit.assert_called_once_with()
    pool.release(connection)
    connection.rollback.assert_called_once_with()
    assert connection.autocommit is False
    pool.close()


def test_pool_closing_during_initialization_retires_new_connection(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({}, on_connection_create=lambda connection: pool.close())
    with pytest.raises(DatabaseConnectionError, match="closed during checkout"):
        pool.acquire()
    sdk_connections[0].close.assert_called_once_with()
    assert pool.size() == 0


def test_pool_unknown_release_does_not_close_caller_owned_handle(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({})
    standalone = pool.new_connection()
    pool.release(standalone)
    assert pool.size() == 0
    pool.close()
    standalone.close.assert_not_called()
    standalone.close()


def test_waiting_checkout_succeeds_after_lease_return(sdk_connections: list[Any]) -> None:
    import threading

    pool = TeradataConnectionPool({}, max_size=1, timeout=2)
    first = pool.acquire()
    waiting = threading.Event()

    def checkout() -> Any:
        waiting.set()
        return pool.acquire()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(checkout)
        assert waiting.wait(timeout=1)
        pool.release(first)
        second = future.result(timeout=3)
    assert second is first
    assert pool.checked_out() == 1
    pool.release(second)
    pool.close()


def test_pool_query_band_none_resets_session(sdk_connections: list[Any]) -> None:
    pool = TeradataConnectionPool({}, session_query_band="NONE")
    connection = pool.acquire()
    connection.cursor.return_value.execute.assert_called_once_with("SET QUERY_BAND = NONE FOR SESSION;")
    pool.release(connection)
    pool.close()


@pytest.mark.parametrize(
    ("band", "expected_sql"),
    [
        (None, "SET QUERY_BAND = NONE FOR SESSION;"),
        ({"app": "sqlspec"}, "SET QUERY_BAND = 'app=sqlspec;' FOR SESSION;"),
    ],
)
@pytest.mark.parametrize("autocommit", [True, False])
def test_pool_release_resets_session_query_band_and_discards_on_failure(
    sdk_connections: list[Any], band: dict[str, str] | None, expected_sql: str, autocommit: bool
) -> None:
    """release() resets the session QUERY_BAND, commits under autocommit=False, and discards on reset error."""
    pool = TeradataConnectionPool({"autocommit": autocommit}, max_size=1, session_query_band=band)
    connection = pool.acquire()
    cursor = connection.cursor.return_value
    cursor.reset_mock()
    connection.commit.reset_mock()

    pool.release(connection)
    cursor.execute.assert_called_once_with(expected_sql)
    cursor.close.assert_called_once_with()
    if autocommit:
        connection.commit.assert_not_called()
    else:
        connection.commit.assert_called_once_with()

    reused = pool.acquire()
    assert reused is connection
    cursor.execute.side_effect = RuntimeError("query band reset failed")
    pool.release(reused)
    reused.close.assert_called_once_with()
    assert pool.size() == 0
    pool.close()


def test_teradata_new_connection_maps_driver_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native teradatasql connection failures map to DatabaseConnectionError while preserving cause."""

    class FakeTeradataError(Exception):
        pass

    class FakeOperationalError(FakeTeradataError):
        pass

    native = FakeOperationalError("[Error 1000] Unable to connect to host")

    def failing_connect(**_: Any) -> Any:
        raise native

    monkeypatch.setitem(
        sys.modules,
        "teradatasql",
        SimpleNamespace(connect=failing_connect, Error=FakeTeradataError, OperationalError=FakeOperationalError),
    )
    reset_dependency_cache("teradatasql")
    pool = TeradataConnectionPool({"host": "primary.invalid"}, max_size=1)
    try:
        with pytest.raises(DatabaseConnectionError) as exc_info:
            pool.acquire()
        assert exc_info.value.__cause__ is native
    finally:
        pool.close()
        reset_dependency_cache("teradatasql")
