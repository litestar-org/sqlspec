"""Tests for IBM Db2 connection pool."""

import threading
import time
from typing import cast

import pytest

import sqlspec.adapters.db2.pool as pool_module
from sqlspec.adapters.db2.pool import Db2ConnectionPool
from sqlspec.exceptions import MissingDependencyError
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeDb2Cursor, FakeIbmDbDbiModule, FakeIbmDbModule

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]


def test_pool_connects_with_config_and_runs_hook(fake_ibm_db: FakeModules) -> None:
    """Pool should create connection lazily using generated DSN and execute creation hook."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    seen: list[object] = []

    pool = Db2ConnectionPool(
        {"database": "TESTDB", "hostname": "db2.local", "port": 50000, "user": "db2inst1", "password": "pwd"},
        recycle_seconds=0,
        health_check_interval=999.0,
        on_connection_create=seen.append,
    )

    acquired = pool.acquire()

    assert cast(object, acquired) is connection
    assert len(fake_module.connect_calls) == 1
    dsn_called = fake_module.connect_calls[0][0]
    assert "DATABASE=TESTDB" in dsn_called
    assert "HOSTNAME=db2.local" in dsn_called
    assert "PORT=50000" in dsn_called
    assert seen == [connection]
    assert pool.size() == 1
    assert pool.checked_out() == 0


def test_pool_connects_with_factory() -> None:
    """Custom connection factory takes precedence over driver module."""
    connection = FakeDb2Connection()
    factory_calls = 0

    def custom_factory() -> FakeDb2Connection:
        nonlocal factory_calls
        factory_calls += 1
        return connection

    pool = Db2ConnectionPool({"database": "TESTDB"}, connection_factory=custom_factory)

    acquired = pool.acquire()

    assert cast(object, acquired) is connection
    assert factory_calls == 1


def test_pool_connects_with_explicit_dsn(fake_ibm_db: FakeModules) -> None:
    """Pool passes explicit DSN directly to driver connect."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)

    explicit_dsn = "DATABASE=CUSTOM;HOSTNAME=custom.host;PORT=50000;PROTOCOL=TCPIP;"
    pool = Db2ConnectionPool({"dsn": explicit_dsn})

    acquired = pool.acquire()

    assert cast(object, acquired) is connection
    assert fake_module.connect_calls[0][0] == explicit_dsn


def test_pool_missing_dependency_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """MissingDependencyError is raised when ibm_db_dbi is not available and no factory is set."""
    monkeypatch.setattr(pool_module, "ibm_db_dbi", None)

    pool = Db2ConnectionPool({"database": "TESTDB"})

    with pytest.raises(MissingDependencyError) as exc_info:
        pool.acquire()

    assert "ibm_db" in str(exc_info.value)


def test_pool_recycles_failed_health_check(fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch) -> None:
    """Failed health check should close and replace thread connection."""
    _, fake_module = fake_ibm_db
    first = FakeDb2Connection()
    second = FakeDb2Connection()
    fake_module.pending_connections.extend([first, second])
    monkeypatch.setattr(Db2ConnectionPool, "_is_connection_alive", lambda *_: False)

    pool = Db2ConnectionPool({"database": "TESTDB"}, health_check_interval=-1.0)

    assert cast(object, pool.acquire()) is first
    assert cast(object, pool.acquire()) is second
    assert first.closed is True
    assert len(fake_module.connect_calls) == 2


def test_pool_recycles_exceeded_recycle_time(fake_ibm_db: FakeModules) -> None:
    """Connection exceeding recycle seconds threshold should be replaced."""
    _, fake_module = fake_ibm_db
    first = FakeDb2Connection()
    second = FakeDb2Connection()
    fake_module.pending_connections.extend([first, second])

    pool = Db2ConnectionPool({"database": "TESTDB"}, recycle_seconds=10)

    conn1 = pool.acquire()
    assert cast(object, conn1) is first

    pool._thread_local.created_at = time.time() - 20

    conn2 = pool.acquire()
    assert cast(object, conn2) is second
    assert first.closed is True


def test_pool_close_removes_thread_local_connection(fake_ibm_db: FakeModules) -> None:
    """Calling close() closes and clears current thread's connection."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    pool = Db2ConnectionPool({"database": "TESTDB"})

    assert cast(object, pool.acquire()) is connection
    pool.close()

    assert connection.closed is True
    assert pool.size() == 0


def test_pool_close_closes_connections_opened_on_other_threads(fake_ibm_db: FakeModules) -> None:
    """Calling close() reaches connections opened by worker threads."""
    pool = Db2ConnectionPool({"database": "TESTDB"})
    opened: list[FakeDb2Connection] = []
    barrier = threading.Barrier(3)

    def _open() -> None:
        opened.append(cast(FakeDb2Connection, pool.acquire()))
        barrier.wait()

    workers = [threading.Thread(target=_open) for _ in range(2)]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join()

    assert len({id(conn) for conn in opened}) == 2

    pool.close()

    assert [conn.closed for conn in opened] == [True, True]


def test_pool_registry_does_not_grow_across_replacements(
    fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Registry size remains bounded during connection replacement."""
    monkeypatch.setattr(Db2ConnectionPool, "_is_connection_alive", lambda *_: False)
    pool = Db2ConnectionPool({"database": "TESTDB"}, health_check_interval=-1.0)

    for _ in range(4):
        pool.acquire()

    assert len(pool._connection_registry) == 1

    pool.close()

    assert pool._connection_registry == set()


def test_pool_get_connection_context_manager(fake_ibm_db: FakeModules) -> None:
    """get_connection context manager yields connection and closes thread connection on error."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    pool = Db2ConnectionPool({"database": "TESTDB"})

    with pool.get_connection() as conn:
        assert cast(object, conn) is connection

    with pytest.raises(RuntimeError):
        with pool.get_connection():
            raise RuntimeError("forced failure")

    assert connection.closed is True
    assert pool.size() == 0


def test_pool_is_connection_alive_success() -> None:
    """_is_connection_alive returns True when ping query executes cleanly."""
    pool = Db2ConnectionPool({"database": "TESTDB"})
    connection = FakeDb2Connection([FakeDb2Cursor(rows=[(1,)])])

    result = pool._is_connection_alive(connection)

    assert result is True
    assert len(connection.cursors) == 1
    assert "SYSIBM.SYSDUMMY1" in connection.cursors[0].executed[0][0]
    assert connection.cursors[0].closed is True


def test_pool_is_connection_alive_failure() -> None:
    """_is_connection_alive returns False when cursor execute raises."""
    pool = Db2ConnectionPool({"database": "TESTDB"})
    broken_conn = FakeDb2Connection([FakeDb2Cursor(error=RuntimeError("Database error"))])

    result = pool._is_connection_alive(broken_conn)

    assert result is False
