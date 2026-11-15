"""Thread-local sync pools hand each asyncio task its own connection and park finished handles."""

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest

from sqlspec.adapters.db2.pool import Db2SyncConnectionPool
from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.adapters.sqlanywhere.pool import SqlanywhereConnectionPool
from sqlspec.adapters.sybase.pool import SybaseConnectionPool

pytestmark = pytest.mark.anyio

POOLS: "dict[str, tuple[type[Any], dict[str, Any]]]" = {
    "pymysql": (PyMysqlConnectionPool, {"host": "localhost"}),
    "pymssql": (PymssqlConnectionPool, {"server": "localhost"}),
    "sybase": (
        SybaseConnectionPool,
        {"host": "localhost", "port": 5000, "user": "user", "password": "secret", "database": "db"},
    ),
    "sqlanywhere": (SqlanywhereConnectionPool, {"eng": "demo"}),
    "db2": (
        Db2SyncConnectionPool,
        {"database": "db", "hostname": "localhost", "port": 50000, "username": "user", "password": "secret"},
    ),
}


class FakeConnection:
    """Connection double that records closes and reports its transaction state."""

    autocommit_state = True
    conn_handler = object()

    def __init__(self) -> None:
        self.closed = False
        self.in_transaction = False

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        self.in_transaction = False

    def ping(self, reconnect: bool = False) -> None:
        return None

    def get_autocommit(self) -> bool:
        return True


@pytest.fixture(params=sorted(POOLS))
def pool_kind(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def made(pool_kind: str, monkeypatch: pytest.MonkeyPatch) -> "tuple[Any, list[FakeConnection]]":
    pool_cls, parameters = POOLS[pool_kind]
    created: list[FakeConnection] = []

    def new_connection(self: Any) -> FakeConnection:
        connection = FakeConnection()
        created.append(connection)
        return connection

    monkeypatch.setattr(pool_cls, "new_connection", new_connection)
    monkeypatch.setenv("TDSVER", "5.0")
    pool = pool_cls(parameters, health_check_interval=3600.0)
    return pool, created


async def run_tasks(*bodies: "Callable[[], Any]") -> "list[Any]":
    async def wrap(body: "Callable[[], Any]") -> Any:
        return body()

    return list(await asyncio.gather(*(asyncio.create_task(wrap(body)) for body in bodies)))


async def test_concurrent_tasks_get_distinct_connections(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, _ = made
    first_acquired = asyncio.Event()
    second_acquired = asyncio.Event()
    seen: dict[str, Any] = {}

    async def task_a() -> None:
        with pool.get_connection() as connection:
            seen["a"] = connection
            first_acquired.set()
            await second_acquired.wait()

    async def task_b() -> None:
        await first_acquired.wait()
        with pool.get_connection() as connection:
            seen["b"] = connection
            second_acquired.set()

    await asyncio.gather(task_a(), task_b())
    assert seen["a"] is not seen["b"]


async def test_nested_acquire_in_one_task_reuses_connection(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, created = made
    with pool.get_connection() as outer, pool.get_connection() as inner:
        assert inner is outer
    assert len(created) == 1


async def test_finished_task_handle_is_parked_and_reused(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, created = made
    handles: list[Any] = []

    async def use() -> None:
        with pool.get_connection() as connection:
            handles.append(connection)

    await asyncio.create_task(use())
    await asyncio.sleep(0)
    await asyncio.create_task(use())
    assert handles[0] is handles[1]
    assert len(created) == 1
    assert not created[0].closed


async def test_parking_is_capped_at_eight(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, created = made
    barrier_ready = asyncio.Event()
    arrived = 0

    async def use() -> None:
        nonlocal arrived
        with pool.get_connection():
            arrived += 1
            if arrived == 10:
                barrier_ready.set()
            await barrier_ready.wait()

    await asyncio.gather(*(asyncio.create_task(use()) for _ in range(10)))
    await asyncio.sleep(0)
    assert len(created) == 10
    assert sum(not connection.closed for connection in created) == 8
    assert sum(connection.closed for connection in created) == 2


async def test_checked_out_or_transaction_handle_is_retired(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, created = made
    release_task = asyncio.Event()
    held: list[Any] = []

    async def hold_checked_out() -> None:
        lease = pool.get_connection()
        lease.__enter__()
        held.append(lease)
        release_task.set()

    await asyncio.create_task(hold_checked_out())
    await asyncio.sleep(0)
    assert release_task.is_set()
    assert created[0].closed

    async def finish_in_transaction() -> None:
        with pool.get_connection() as connection:
            pass
        connection.in_transaction = True
        connection.server_status = 1

    await asyncio.create_task(finish_in_transaction())
    await asyncio.sleep(0)
    assert created[1].closed


def test_thread_local_behaviour_outside_event_loop(made: "tuple[Any, list[FakeConnection]]") -> None:
    pool, created = made
    results: list[Any] = []

    def work() -> None:
        with pool.get_connection() as first:
            results.append(first)
        with pool.get_connection() as second:
            results.append(second)

    thread = threading.Thread(target=work)
    thread.start()
    thread.join()
    assert results[0] is results[1]
    assert len(created) == 1
