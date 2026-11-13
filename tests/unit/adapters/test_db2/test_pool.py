"""Tests for IBM Db2 connection pool."""

import asyncio
import threading
import time
from typing import cast

import pytest

from sqlspec.adapters.db2.pool import Db2SyncConnectionPool
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeDb2Cursor, FakeIbmDbDbiModule, FakeIbmDbModule

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]


def test_pool_connects_with_config_and_runs_hook(fake_ibm_db: FakeModules) -> None:
    """Pool should create connection lazily using generated DSN and execute creation hook."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    seen: list[object] = []

    pool = Db2SyncConnectionPool(
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
    assert pool.checked_out() == 1

    with pool.get_connection():
        assert pool.checked_out() == 1


def test_pool_renders_dsn_and_passes_empty_connect_arguments(fake_ibm_db: FakeModules) -> None:
    """The pool renders the whole CLI string and never passes credentials as separate arguments."""
    _, fake_module = fake_ibm_db
    pool = Db2SyncConnectionPool({
        "database": "CUSTOM",
        "hostname": "custom.host",
        "port": 50000,
        "protocol": "TCPIP",
        "user": "u",
        "password": "p",
    })

    pool.new_connection()

    assert fake_module.connect_calls[0][:5] == (
        "DATABASE=CUSTOM;HOSTNAME=custom.host;PORT=50000;PROTOCOL=TCPIP;UID=u;PWD=p;",
        "",
        "",
        "",
        "",
    )


def test_pool_recycles_failed_health_check(fake_ibm_db: FakeModules) -> None:
    """Failed health check should close and replace thread connection."""
    _, fake_module = fake_ibm_db
    first = FakeDb2Connection([FakeDb2Cursor(error=RuntimeError("connection lost"))])
    second = FakeDb2Connection()
    fake_module.pending_connections.extend([first, second])

    pool = Db2SyncConnectionPool({"database": "TESTDB"}, health_check_interval=-1.0)

    assert cast(object, pool.acquire()) is first
    assert cast(object, pool.acquire()) is second
    assert first.closed is True
    assert first.cursors[0].closed is True
    assert len(fake_module.connect_calls) == 2


def test_pool_recycles_exceeded_recycle_time(fake_ibm_db: FakeModules) -> None:
    """Connection exceeding recycle seconds threshold should be replaced."""
    _, fake_module = fake_ibm_db
    first = FakeDb2Connection()
    second = FakeDb2Connection()
    fake_module.pending_connections.extend([first, second])

    pool = Db2SyncConnectionPool({"database": "TESTDB"}, recycle_seconds=10)

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
    pool = Db2SyncConnectionPool({"database": "TESTDB"})

    assert cast(object, pool.acquire()) is connection
    pool.close()

    assert connection.closed is True
    assert pool.size() == 0


def test_pool_close_closes_connections_opened_on_other_threads(fake_ibm_db: FakeModules) -> None:
    """Calling close() reaches connections opened by worker threads."""
    pool = Db2SyncConnectionPool({"database": "TESTDB"})
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
    assert pool.size() == 2

    pool.close()

    assert pool.size() == 0
    assert [conn.closed for conn in opened] == [True, True]


def test_pool_registry_does_not_grow_across_replacements(fake_ibm_db: FakeModules) -> None:
    """Registry size remains bounded during connection replacement."""
    _, module = fake_ibm_db
    connections = [FakeDb2Connection([FakeDb2Cursor(error=RuntimeError("connection lost"))]) for _ in range(4)]
    module.pending_connections.extend(connections)
    pool = Db2SyncConnectionPool({"database": "TESTDB"}, health_check_interval=-1.0)

    for _ in range(4):
        pool.acquire()

    assert pool.size() == 1
    assert len(module.connect_calls) == 4
    assert [connection.closed for connection in connections] == [True, True, True, False]

    pool.close()

    assert pool.size() == 0
    assert all(connection.closed for connection in connections)


def test_pool_get_connection_context_manager(fake_ibm_db: FakeModules) -> None:
    """get_connection context manager yields connection and closes thread connection on error."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    pool = Db2SyncConnectionPool({"database": "TESTDB"})

    with pool.get_connection() as conn:
        assert cast(object, conn) is connection

    with pytest.raises(RuntimeError):
        with pool.get_connection():
            raise RuntimeError("forced failure")

    assert connection.closed is True
    assert pool.size() == 0


def test_db2_pool_discards_thread_connection_on_cancelled_error(fake_ibm_db: FakeModules) -> None:
    """A forwarded cancellation must close and discard the thread connection."""
    _, fake_module = fake_ibm_db
    c1 = FakeDb2Connection()
    c2 = FakeDb2Connection()
    fake_module.pending_connections.extend([c1, c2])
    pool = Db2SyncConnectionPool({"database": "TESTDB"}, recycle_seconds=0, health_check_interval=0)

    try:
        with pytest.raises(asyncio.CancelledError):
            with pool.get_connection() as conn:
                assert cast(object, conn) is c1
                raise asyncio.CancelledError()

        assert c1.closed is True
        assert pool.size() == 0

        with pool.get_connection() as next_conn:
            assert cast(object, next_conn) is c2
            assert c2.closed is False
    finally:
        pool.close()


def test_db2_pool_clean_exit_preserves_thread_connection(fake_ibm_db: FakeModules) -> None:
    """A clean exit retains the connection for the thread."""
    _, fake_module = fake_ibm_db
    c1 = FakeDb2Connection()
    fake_module.pending_connections.append(c1)
    pool = Db2SyncConnectionPool({"database": "TESTDB"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pool.get_connection() as conn:
            assert cast(object, conn) is c1
        assert c1.closed is False
        assert pool.size() == 1
        with pool.get_connection() as next_conn:
            assert next_conn is conn
    finally:
        pool.close()


def test_db2_pool_nested_checkout_depth_guard(fake_ibm_db: FakeModules) -> None:
    """Nested get_connection preserves active outer connection when inner exception is caught and closes when uncaught."""
    _, fake_module = fake_ibm_db
    c1 = FakeDb2Connection()
    c2 = FakeDb2Connection()
    fake_module.pending_connections.extend([c1, c2])
    pool = Db2SyncConnectionPool({"database": "TESTDB"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pool.get_connection() as outer_conn:
            assert cast(object, outer_conn) is c1
            with pytest.raises(ValueError, match="inner error"):
                with pool.get_connection() as inner_conn:
                    assert inner_conn is outer_conn
                    raise ValueError("inner error")
            assert c1.closed is False

        assert c1.closed is False

        with pytest.raises(RuntimeError, match="uncaught inner"):
            with pool.get_connection() as outer_conn2:
                assert outer_conn2 is outer_conn
                with pool.get_connection():
                    raise RuntimeError("uncaught inner")

        assert c1.closed is True
        assert pool.size() == 0

        with pool.get_connection() as fresh_conn:
            assert cast(object, fresh_conn) is c2
            assert c2.closed is False
    finally:
        pool.close()


def test_pool_is_connection_alive_success() -> None:
    """_is_connection_alive returns True when ping query executes cleanly."""
    pool = Db2SyncConnectionPool({"database": "TESTDB"})
    connection = FakeDb2Connection([FakeDb2Cursor(rows=[(1,)])])

    result = pool._is_connection_alive(connection)

    assert result is True
    assert len(connection.cursors) == 1
    assert "SYSIBM.SYSDUMMY1" in connection.cursors[0].executed[0][0]
    assert connection.cursors[0].closed is True


def test_pool_is_connection_alive_failure() -> None:
    """_is_connection_alive returns False when cursor execute raises."""
    pool = Db2SyncConnectionPool({"database": "TESTDB"})
    broken_conn = FakeDb2Connection([FakeDb2Cursor(error=RuntimeError("Database error"))])

    result = pool._is_connection_alive(broken_conn)

    assert result is False


def test_failed_creation_hook_closes_unregistered_connection(fake_ibm_db: FakeModules) -> None:
    _, module = fake_ibm_db
    connection = FakeDb2Connection()
    module.pending_connections.append(connection)

    def fail_hook(_connection: object) -> None:
        raise RuntimeError("hook failed")

    pool = Db2SyncConnectionPool({"database": "TESTDB"}, on_connection_create=fail_hook)
    with pytest.raises(RuntimeError, match="hook failed"):
        pool.acquire()
    assert connection.closed is True
    assert pool.size() == 0


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_hadr_pool_passes_read_only_cli_options_without_mutation(
    fake_ibm_db: FakeModules, asynchronous: bool
) -> None:
    from sqlspec.adapters.db2.pool import Db2AsyncConnectionPool

    _, module = fake_ibm_db
    options = {999: 42}
    if asynchronous:
        pool = Db2AsyncConnectionPool({"database": "app", "read_only": True}, conn_options=options)
        connection = await pool.new_connection()
        await connection.close()
        await pool.close()
    else:
        sync_pool = Db2SyncConnectionPool({"database": "app", "read_only": True}, conn_options=options)
        connection = sync_pool.new_connection()
        connection.close()
        sync_pool.close()
    assert module.connect_calls[-1][5] == {999: 42, module.SQL_ATTR_AUTOCOMMIT: module.SQL_AUTOCOMMIT_ON, 101: 1}
    assert "READONLY" not in module.connect_calls[-1][0]
    assert options == {999: 42}
