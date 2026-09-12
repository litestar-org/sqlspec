"""Integration tests for DuckDB secrets declared through the connection pool."""

import threading
from typing import Any
from uuid import uuid4

import duckdb
import pytest

from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool, _create_secret

HTTP_SECRET_VALUE = {"bearer_token": "x"}


def _secret_count(connection: "duckdb.DuckDBPyConnection", name: str) -> "tuple[Any, ...] | None":
    return connection.execute("SELECT count(*) FROM duckdb_secrets() WHERE name = ?", (name,)).fetchone()


def _first_use_from_threads(
    pools: "list[DuckDBConnectionPool]", threads_per_pool: int
) -> "tuple[list[BaseException], list[tuple[str, ...]]]":
    barrier = threading.Barrier(len(pools) * threads_per_pool)
    errors: list[BaseException] = []
    recorded: list[tuple[str, ...]] = []
    guard = threading.Lock()

    def worker(pool: DuckDBConnectionPool) -> None:
        barrier.wait()
        try:
            connection = pool.acquire()
            settings = pool._storage_settings(connection)  # pyright: ignore[reportPrivateUsage]
            names = tuple(secret["name"] for secret in settings["_duckdb_storage_secrets"])
            with guard:
                recorded.append(names)
        except BaseException as exc:
            with guard:
                errors.append(exc)
        finally:
            pool.close()

    workers = [threading.Thread(target=worker, args=(pool,)) for pool in pools for _ in range(threads_per_pool)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join()
    return errors, recorded


def test_two_connections_same_database_create_secret() -> None:
    database = f":memory:sqlspec_secret_{uuid4().hex}"
    secret = {"name": "sqlspec_t", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": True}
    first = duckdb.connect(database)
    second = duckdb.connect(database)
    try:
        assert _create_secret(first, secret)
        assert _create_secret(second, secret)
        row = _secret_count(first, "sqlspec_t")
    finally:
        second.close()
        first.close()

    assert row == (1,)


def test_required_secret_with_invalid_parameter_raises() -> None:
    secret = {"name": "sqlspec_bad", "secret_type": "http", "value": {"nonsense_key": 1}, "required": True}
    connection = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.Error, match="nonsense_key"):
            _create_secret(connection, secret)
    finally:
        connection.close()


@pytest.mark.parametrize(("pool_count", "threads_per_pool"), [(1, 8), (2, 4)], ids=["one_pool", "two_pools"])
def test_concurrent_first_use_records_declared_secret(pool_count: int, threads_per_pool: int) -> None:
    database = f":memory:sqlspec_secret_{uuid4().hex}"
    secret = {"name": "sqlspec_c", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": True}
    anchor = duckdb.connect(database)
    try:
        pools = [
            DuckDBConnectionPool(connection_config={"database": database}, secrets=[secret]) for _ in range(pool_count)
        ]
        errors, recorded = _first_use_from_threads(pools, threads_per_pool)
        row = _secret_count(anchor, "sqlspec_c")
    finally:
        anchor.close()

    assert errors == []
    assert recorded == [("sqlspec_c",)] * (pool_count * threads_per_pool)
    assert row == (1,)
