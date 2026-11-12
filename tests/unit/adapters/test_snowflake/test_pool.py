"""Snowflake pool ownership and lifecycle tests with a connector boundary fake."""

import sys
import threading
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from sqlspec.adapters.snowflake import pool as pool_module
from sqlspec.adapters.snowflake.pool import SnowflakeConnectionPool
from sqlspec.exceptions import DatabaseConnectionError, ImproperConfigurationError, MissingDependencyError


def make_connector(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, list[Mock]]:
    connections: list[Mock] = []

    def connect(**_: Any) -> Mock:
        connection = Mock()
        connection.autocommit = False
        connection.is_closed.return_value = False
        connections.append(connection)
        return connection

    connect_mock = Mock(side_effect=connect)
    existing = sys.modules.get("snowflake.connector")
    if existing is None:
        try:
            import snowflake.connector as imported

            existing = imported
        except ImportError:
            snowflake_pkg = sys.modules.get("snowflake") or ModuleType("snowflake")
            existing = ModuleType("snowflake.connector")
            monkeypatch.setitem(sys.modules, "snowflake", snowflake_pkg)
            monkeypatch.setitem(sys.modules, "snowflake.connector", existing)
    monkeypatch.setattr(existing, "connect", connect_mock, raising=False)
    return Mock(connect=connect_mock), connections


def test_capacity_timeout_and_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({}, max_size=1, timeout=0.01)
    connection = cast("Mock", pool.acquire())
    with pytest.raises(DatabaseConnectionError, match="Timed out"):
        pool.acquire()
    pool.release(connection)
    assert pool.acquire() is connection
    assert len(connections) == 1
    assert pool.checked_out() == 1
    pool.release(connection)
    connection.rollback.assert_called()
    pool.close()
    connection.close.assert_called_once_with()


def test_nested_contexts_reuse_one_exclusive_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({}, max_size=1)
    with pool.get_connection() as outer:
        with pool.get_connection() as inner:
            assert inner is outer
            assert pool.checked_out() == 1
    assert pool.checked_out() == 0
    assert len(connections) == 1
    pool.close()


def test_nested_error_discards_handle_even_when_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({}, max_size=1)
    with pool.get_connection():
        with pytest.raises(ValueError, match="failed"), pool.get_connection():
            raise ValueError("failed")
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()


def test_failed_connect_and_hook_restore_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, connections = make_connector(monkeypatch)
    hook = Mock(side_effect=ValueError("hook failed"))
    pool = SnowflakeConnectionPool({}, max_size=1, on_connection_create=hook)
    with pytest.raises(ValueError, match="hook failed"):
        pool.acquire()
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    hook.side_effect = None
    connection = pool.acquire()
    assert connector.connect.call_count == 2
    pool.release(connection)
    pool.close()


def test_missing_connector_restores_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.utils.module_loader import reset_dependency_cache

    monkeypatch.setitem(sys.modules, "snowflake.connector", None)
    reset_dependency_cache("snowflake.connector")
    try:
        pool = SnowflakeConnectionPool({})
        with pytest.raises(MissingDependencyError, match=r"sqlspec\[snowflake\]"):
            pool.acquire()
        assert pool.size() == 0
    finally:
        reset_dependency_cache("snowflake.connector")


def test_rollback_failure_discards_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({}, max_size=1)
    connection = cast("Mock", pool.acquire())
    connection.rollback.side_effect = OSError("connection failed")
    pool.release(connection)
    assert pool.size() == 0
    connections[0].close.assert_called_once_with()
    replacement = pool.acquire()
    assert replacement is connections[1]
    pool.release(replacement)
    pool.close()


def test_fork_guard_precedes_all_pool_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({})
    connection = pool.acquire()
    monkeypatch.setattr(pool_module.os, "getpid", lambda: -1)
    for operation in (pool.acquire, lambda: pool.release(connection), pool.close):
        with pytest.raises(DatabaseConnectionError, match="process fork"):
            operation()


def test_refresh_credentials_per_physical_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, connections = make_connector(monkeypatch)
    tokens = iter(["one", "two"])
    pool = SnowflakeConnectionPool({"auth_type": "pat", "auth_token_provider": lambda: next(tokens)}, max_size=2)
    first, second = pool.acquire(), pool.acquire()
    assert [call.kwargs["token"] for call in connector.connect.call_args_list] == ["one", "two"]
    assert len(connections) == 2
    pool.release(first)
    pool.release(second)
    pool.close()


def test_close_drains_returning_leases(monkeypatch: pytest.MonkeyPatch) -> None:
    make_connector(monkeypatch)
    pool = SnowflakeConnectionPool({})
    connection = cast("Mock", pool.acquire())
    pool.close()
    assert pool.size() == 1
    pool.release(connection)
    connection.close.assert_called_once_with()
    assert pool.size() == 0
    with pytest.raises(DatabaseConnectionError, match="closed"):
        pool.acquire()


def test_recycles_expired_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(pool_module.time, "monotonic", lambda: clock.now)
    pool = SnowflakeConnectionPool({}, recycle_seconds=10)
    first = cast("Mock", pool.acquire())
    pool.release(first)
    clock.now = 11.0
    second = pool.acquire()
    assert second is connections[1]
    first.close.assert_called_once_with()
    pool.release(second)
    pool.close()


@pytest.mark.parametrize(
    "options",
    [
        {"min_size": -1},
        {"max_size": 0},
        {"max_size": True},
        {"min_size": 2, "max_size": 1},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"recycle_seconds": -1},
        {"health_check_interval": -1},
    ],
)
def test_invalid_pool_settings(options: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        SnowflakeConnectionPool({}, **options)


def test_min_size_warmup_and_warmup_failure_closes_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, connections = make_connector(monkeypatch)
    warm_pool = SnowflakeConnectionPool({}, min_size=2, max_size=2)
    assert warm_pool.size() == 2
    warm_pool.close()
    assert all(conn.close.call_count == 1 for conn in connections)

    attempts = 0

    def fail_second(**_: Any) -> Mock:
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise OSError("second warmup failed")
        conn = Mock()
        conn.autocommit = False
        conn.is_closed.return_value = False
        return conn

    connector.connect.side_effect = fail_second
    with pytest.raises(OSError, match="second warmup failed"):
        SnowflakeConnectionPool({}, min_size=2, max_size=2)


def test_health_check_unowned_release_and_close_destroy_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _, connections = make_connector(monkeypatch)
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(pool_module.time, "monotonic", lambda: clock.now)
    runtime = Mock()
    pool = SnowflakeConnectionPool(
        {}, max_size=2, recycle_seconds=0, health_check_interval=5.0, observability_runtime=runtime
    )
    first = cast("Mock", pool.acquire())
    pool.release(first)
    clock.now = 6.0
    first.is_closed.side_effect = OSError("probe error")
    second = cast("Mock", pool.acquire())
    assert second is connections[1]
    pool.release(second)

    with pytest.raises(DatabaseConnectionError, match="not an active lease"):
        pool.release(cast("Any", Mock()))

    runtime.emit_connection_destroy_sync.side_effect = RuntimeError("destroy error")
    with pytest.raises(RuntimeError, match="destroy error"):
        pool.close()
    second.close.assert_called_once_with()


def test_pool_closed_during_physical_connect_discards_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    pool_holder: list[SnowflakeConnectionPool] = []
    entered = threading.Event()
    proceed = threading.Event()

    def slow_connect(**_: Any) -> Mock:
        entered.set()
        proceed.wait(timeout=2.0)
        conn = Mock()
        conn.autocommit = False
        conn.is_closed.return_value = False
        return conn

    connector, _ = make_connector(monkeypatch)
    connector.connect.side_effect = slow_connect
    pool = SnowflakeConnectionPool({}, max_size=1)
    pool_holder.append(pool)
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            pool_holder[0].acquire()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=2.0)
    pool.close()
    proceed.set()
    thread.join(timeout=2.0)
    assert len(errors) == 1
    assert isinstance(errors[0], DatabaseConnectionError)
    assert "closed during acquisition" in str(errors[0])
