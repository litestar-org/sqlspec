"""pymssql pool tests."""

import threading
from typing import Any, cast

import pytest

from tests.unit.adapters.test_pymssql._fakes import FakeConnection, FakeCursor, FakePymssqlModule


def test_pool_connects_with_config_and_runs_hook(monkeypatch) -> None:
    """The pool should create pymssql connections lazily and call the hook."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    connection = FakeConnection()
    fake_module = FakePymssqlModule(connection)
    seen: list[object] = []
    monkeypatch.setattr(pool_module, "pymssql", fake_module)

    pool = PymssqlConnectionPool(
        {"server": "sql.example.test", "user": "sa"},
        recycle_seconds=0,
        health_check_interval=999.0,
        on_connection_create=seen.append,
    )

    acquired = pool.acquire()

    assert cast(object, acquired) is connection
    assert fake_module.connect_calls == [{"server": "sql.example.test", "user": "sa"}]
    assert seen == [connection]
    assert pool.size() == 1


def test_pool_recycles_failed_health_check(monkeypatch) -> None:
    """A failed idle health check should close and replace the thread-local connection."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    first = FakeConnection(FakeCursor(error=OSError("connection lost")))
    second = FakeConnection()
    connections = [first, second]

    class SequencedModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            self.connect_calls.append(kwargs)
            return connections.pop(0)

    fake_module = SequencedModule()
    monkeypatch.setattr(pool_module, "pymssql", fake_module)

    pool = PymssqlConnectionPool({"server": "sql.example.test"}, health_check_interval=0.5)

    assert cast(object, pool.acquire()) is first
    pool._thread_local.last_used = 0.0  # pyright: ignore[reportPrivateUsage]
    assert cast(object, pool.acquire()) is second
    assert first.closed is True
    assert first.cursor_obj.closed is True
    assert len(fake_module.connect_calls) == 2


def test_pool_zero_health_check_interval_disables_health_check(monkeypatch) -> None:
    """health_check_interval=0 should disable idle health checks."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    connection = FakeConnection(FakeCursor(error=OSError("health checks should be disabled")))
    fake_module = FakePymssqlModule(connection)
    monkeypatch.setattr(pool_module, "pymssql", fake_module)
    pool = PymssqlConnectionPool({"server": "sql.example.test"}, health_check_interval=0)

    assert cast(object, pool.acquire()) is connection
    pool._thread_local.last_used = 0.0  # pyright: ignore[reportPrivateUsage]
    assert cast(object, pool.acquire()) is connection
    assert connection.cursor_obj.calls == []
    assert len(fake_module.connect_calls) == 1


def test_pool_close_removes_thread_local_connection(monkeypatch) -> None:
    """close() should close and forget the current thread's connection."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    connection = FakeConnection()
    monkeypatch.setattr(pool_module, "pymssql", FakePymssqlModule(connection))
    pool = PymssqlConnectionPool({"server": "sql.example.test"})

    assert cast(object, pool.acquire()) is connection
    pool.close()

    assert connection.closed is True
    assert pool.size() == 0


def test_pool_close_closes_connections_opened_on_other_threads(monkeypatch) -> None:
    """close() must reach connections opened by worker threads, not just the caller's."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    class SequencedModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            self.connect_calls.append(kwargs)
            return FakeConnection()

    monkeypatch.setattr(pool_module, "pymssql", SequencedModule())
    pool = PymssqlConnectionPool({"server": "sql.example.test"})
    opened: list[FakeConnection] = []
    release_barrier = threading.Barrier(3)

    def _open() -> None:
        opened.append(cast(FakeConnection, pool.acquire()))
        release_barrier.wait()

    workers = [threading.Thread(target=_open) for _ in range(2)]
    for worker in workers:
        worker.start()
    release_barrier.wait()
    for worker in workers:
        worker.join()

    assert len({id(connection) for connection in opened}) == 2

    pool.close()

    assert pool.size() == 0
    assert [connection.closed for connection in opened] == [True, True]


def test_pool_prunes_dead_thread_connections_and_tracks_checked_out(monkeypatch) -> None:
    """Dead worker threads should have their connections closed and pruned, and get_connection tracks checked_out."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    class SequencedModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            self.connect_calls.append(kwargs)
            return FakeConnection()

    monkeypatch.setattr(pool_module, "pymssql", SequencedModule())
    pool = PymssqlConnectionPool({"server": "sql.example.test"})
    worker_connections: list[FakeConnection] = []

    def _worker() -> None:
        with pool.get_connection() as conn:
            worker_connections.append(cast(FakeConnection, conn))
            assert pool.checked_out() == 1

    thread = threading.Thread(target=_worker)
    thread.start()
    thread.join()

    assert pool.checked_out() == 0
    assert pool.size() == 0
    assert worker_connections[0].closed is True


def test_pool_registry_does_not_grow_across_replacements(monkeypatch) -> None:
    """Recycle and health-check replacement must deregister the connection they discard."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    class SequencedModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            self.connect_calls.append(kwargs)
            return FakeConnection(FakeCursor(error=OSError("connection lost")))

    monkeypatch.setattr(pool_module, "pymssql", SequencedModule())
    pool = PymssqlConnectionPool({"server": "sql.example.test"}, health_check_interval=0.5)

    connections = []
    for _ in range(4):
        connections.append(cast("FakeConnection", pool.acquire()))
        pool._thread_local.last_used = 0.0  # pyright: ignore[reportPrivateUsage]

    assert pool.size() == 1
    assert [connection.closed for connection in connections] == [True, True, True, False]

    pool.close()

    assert pool.size() == 0
    assert all(connection.closed for connection in connections)


def test_pool_reentrant_thread_checkout(monkeypatch: Any) -> None:
    """Re-entrant get_connection calls on the same thread do not close the connection on inner error."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    connection = FakeConnection()
    fake_module = FakePymssqlModule(connection)
    monkeypatch.setattr(pool_module, "pymssql", fake_module)

    pool = PymssqlConnectionPool({"server": "sql.example.test"}, recycle_seconds=0)

    with pool.get_connection() as c1:
        assert cast(object, c1) is connection
        try:
            with pool.get_connection() as c2:
                assert cast(object, c2) is connection
                raise ValueError("inner failure")
        except ValueError:
            pass
        assert not connection.closed

    assert not connection.closed
    pool.close()
    assert connection.closed


def test_pool_recovers_after_failed_acquisition_and_retires_failed_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed connect must not prevent a later failed checkout from retiring its connection."""
    import sqlspec.adapters.pymssql.pool as pool_module
    from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool

    dirty_connection = FakeConnection()
    clean_connection = FakeConnection()
    attempts: list[OSError | FakeConnection] = [
        OSError("temporary connect failure"),
        dirty_connection,
        clean_connection,
    ]

    class RecoveringModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            outcome = attempts.pop(0)
            if isinstance(outcome, OSError):
                raise outcome
            return outcome

    monkeypatch.setattr(pool_module, "pymssql", RecoveringModule())
    pool = PymssqlConnectionPool({"server": "sql.example.test"}, recycle_seconds=0, health_check_interval=0)

    try:
        with pytest.raises(OSError, match="temporary connect failure"), pool.get_connection():
            pytest.fail("Failed acquisition must not enter the checkout body")

        assert pool.checked_out() == 0
        assert pool.size() == 0

        with pytest.raises(ValueError, match="failed transaction"), pool.get_connection() as connection:
            assert cast(object, connection) is dirty_connection
            dirty_connection.autocommit(False)
            raise ValueError("failed transaction")

        assert dirty_connection.closed is True
        assert pool.checked_out() == 0
        assert pool.size() == 0

        with pool.get_connection() as replacement:
            assert cast(object, replacement) is clean_connection
            assert clean_connection.closed is False
            assert clean_connection.autocommit_state is True
            assert pool.checked_out() == 1

        assert pool.checked_out() == 0
    finally:
        pool.close()
