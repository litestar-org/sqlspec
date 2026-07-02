"""pymssql pool tests."""

from typing import Any, cast

from tests.unit.adapters.test_pymssql.conftest import FakeConnection, FakePymssqlModule


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

    first = FakeConnection()
    second = FakeConnection()
    connections = [first, second]

    class SequencedModule(FakePymssqlModule):
        def connect(self, **kwargs: Any) -> FakeConnection:
            self.connect_calls.append(kwargs)
            return connections.pop(0)

    fake_module = SequencedModule()
    monkeypatch.setattr(pool_module, "pymssql", fake_module)
    monkeypatch.setattr(PymssqlConnectionPool, "_is_connection_alive", lambda *_: False)

    pool = PymssqlConnectionPool({"server": "sql.example.test"}, health_check_interval=-1.0)

    assert cast(object, pool.acquire()) is first
    assert cast(object, pool.acquire()) is second
    assert first.closed is True
    assert len(fake_module.connect_calls) == 2


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
