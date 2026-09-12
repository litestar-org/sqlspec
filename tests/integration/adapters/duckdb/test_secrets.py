"""Integration tests for DuckDB secrets declared through the connection pool."""

import threading
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pytest

from sqlspec.adapters.duckdb.pool import _SECRET_LOCK, SECRET_CONFLICT_BACKOFF, DuckDBConnectionPool, _create_secret

pytestmark = [pytest.mark.xdist_group("duckdb"), pytest.mark.duckdb]

HTTP_SECRET_VALUE = {"bearer_token": "x"}


def _database() -> str:
    return f":memory:sqlspec_secret_{uuid4().hex}"


def _secret_count(connection: "duckdb.DuckDBPyConnection", name: str) -> "tuple[Any, ...] | None":
    return connection.execute("SELECT count(*) FROM duckdb_secrets() WHERE name = ?", (name,)).fetchone()


def _recorded_secret_names(pool: DuckDBConnectionPool, connection: "duckdb.DuckDBPyConnection") -> "tuple[str, ...]":
    settings = pool._storage_settings(connection)  # pyright: ignore[reportPrivateUsage]
    return tuple(secret["name"] for secret in settings["_duckdb_storage_secrets"])


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
            names = _recorded_secret_names(pool, pool.acquire())
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


def _open_uncommitted_secret(database: str, name: str) -> "duckdb.DuckDBPyConnection":
    connection = duckdb.connect(database)
    connection.execute("BEGIN")
    connection.execute(f"CREATE OR REPLACE SECRET {name} (TYPE http, BEARER_TOKEN 'other')")
    return connection


def test_two_connections_same_database_create_secret() -> None:
    database = _database()
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


def _secret_string(connection: "duckdb.DuckDBPyConnection", name: str, persistent: bool) -> str:
    row = connection.execute(
        "SELECT secret_string FROM duckdb_secrets(redact = false) WHERE name = ? AND persistent = ?", (name, persistent)
    ).fetchone()
    assert row is not None
    return str(row[0])


def test_existing_secret_with_same_type_is_kept_and_recorded() -> None:
    database = _database()
    connection_config = {"database": database, "config": {"allow_unredacted_secrets": True}}
    anchor = duckdb.connect(database, config={"allow_unredacted_secrets": True})
    anchor.execute("CREATE SECRET sqlspec_k (TYPE http, BEARER_TOKEN 'existing')")
    pool = DuckDBConnectionPool(
        connection_config=connection_config,
        secrets=[{"name": "sqlspec_k", "secret_type": "http", "value": {"bearer_token": "declared"}, "required": True}],
    )
    try:
        recorded = _recorded_secret_names(pool, pool.acquire())
        secret_string = _secret_string(anchor, "sqlspec_k", persistent=False)
        row = _secret_count(anchor, "sqlspec_k")
    finally:
        pool.close()
        anchor.close()

    assert recorded == ("sqlspec_k",)
    assert "bearer_token=existing" in secret_string
    assert row == (1,)


def test_existing_persistent_secret_is_reused_after_restart(tmp_path: Path) -> None:
    config = {"secret_directory": str(tmp_path), "allow_unredacted_secrets": True}
    first = duckdb.connect(":memory:", config=config)
    first.execute("CREATE PERSISTENT SECRET sqlspec_p (TYPE http, BEARER_TOKEN 'stored')")
    first.close()
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:", "config": config},
        secrets=[
            {
                "name": "sqlspec_p",
                "secret_type": "http",
                "persistent": True,
                "value": {"bearer_token": "declared"},
                "required": True,
            }
        ],
    )
    try:
        connection = pool.acquire()
        recorded = _recorded_secret_names(pool, connection)
        secret_string = _secret_string(connection, "sqlspec_p", persistent=True)
    finally:
        pool.close()

    assert recorded == ("sqlspec_p",)
    assert "bearer_token=stored" in secret_string


def _load_httpfs_or_skip(connection: "duckdb.DuckDBPyConnection") -> None:
    try:
        connection.load_extension("httpfs")
    except duckdb.Error:
        pytest.skip("httpfs extension is not installed")


def test_existing_secret_with_other_type_raises_when_required() -> None:
    database = _database()
    anchor = duckdb.connect(database)
    _load_httpfs_or_skip(anchor)
    anchor.execute("CREATE SECRET sqlspec_m (TYPE http, BEARER_TOKEN 'x')")
    pool = DuckDBConnectionPool(
        connection_config={"database": database},
        secrets=[{"name": "sqlspec_m", "secret_type": "s3", "value": {"key_id": "k"}, "required": True}],
    )
    try:
        with pytest.raises(RuntimeError, match=r"'sqlspec_m' exists as a temporary secret of type 'http'.*type 's3'"):
            pool.acquire()
    finally:
        pool.close()
        anchor.close()


def test_existing_secret_with_other_type_is_not_recorded_when_optional() -> None:
    database = _database()
    anchor = duckdb.connect(database)
    _load_httpfs_or_skip(anchor)
    anchor.execute("CREATE SECRET sqlspec_m (TYPE http, BEARER_TOKEN 'x')")
    pool = DuckDBConnectionPool(
        connection_config={"database": database},
        secrets=[{"name": "sqlspec_m", "secret_type": "s3", "value": {"key_id": "k"}, "required": False}],
    )
    try:
        recorded = _recorded_secret_names(pool, pool.acquire())
        row = anchor.execute("SELECT type FROM duckdb_secrets() WHERE name = 'sqlspec_m'").fetchall()
    finally:
        pool.close()
        anchor.close()

    assert recorded == ()
    assert row == [("http",)]


def test_replace_overwrites_existing_secret_value() -> None:
    database = _database()
    anchor = duckdb.connect(database, config={"allow_unredacted_secrets": True})
    anchor.execute("CREATE SECRET sqlspec_rv (TYPE http, BEARER_TOKEN 'existing')")
    pool = DuckDBConnectionPool(
        connection_config={"database": database, "config": {"allow_unredacted_secrets": True}},
        secrets=[
            {
                "name": "sqlspec_rv",
                "secret_type": "http",
                "value": {"bearer_token": "rotated"},
                "replace": True,
                "required": True,
            }
        ],
    )
    try:
        recorded = _recorded_secret_names(pool, pool.acquire())
        secret_string = _secret_string(anchor, "sqlspec_rv", persistent=False)
    finally:
        pool.close()
        anchor.close()

    assert recorded == ("sqlspec_rv",)
    assert "bearer_token=rotated" in secret_string


def test_replace_overwrites_stored_persistent_secret(tmp_path: Path) -> None:
    config = {"secret_directory": str(tmp_path), "allow_unredacted_secrets": True}
    first = duckdb.connect(":memory:", config=config)
    first.execute("CREATE PERSISTENT SECRET sqlspec_rp (TYPE http, BEARER_TOKEN 'stored')")
    first.close()
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:", "config": config},
        secrets=[
            {
                "name": "sqlspec_rp",
                "secret_type": "http",
                "persistent": True,
                "value": {"bearer_token": "rotated"},
                "replace": True,
                "required": True,
            }
        ],
    )
    try:
        recorded = _recorded_secret_names(pool, pool.acquire())
    finally:
        pool.close()
    restarted = duckdb.connect(":memory:", config=config)
    try:
        secret_string = _secret_string(restarted, "sqlspec_rp", persistent=True)
    finally:
        restarted.close()

    assert recorded == ("sqlspec_rp",)
    assert "bearer_token=rotated" in secret_string


def test_replace_overwrites_existing_secret_of_other_type() -> None:
    database = _database()
    anchor = duckdb.connect(database)
    _load_httpfs_or_skip(anchor)
    anchor.execute("CREATE SECRET sqlspec_rt (TYPE http, BEARER_TOKEN 'x')")
    pool = DuckDBConnectionPool(
        connection_config={"database": database},
        secrets=[
            {"name": "sqlspec_rt", "secret_type": "s3", "value": {"key_id": "k"}, "replace": True, "required": True}
        ],
    )
    try:
        recorded = _recorded_secret_names(pool, pool.acquire())
        row = anchor.execute("SELECT type FROM duckdb_secrets() WHERE name = 'sqlspec_rt'").fetchall()
    finally:
        pool.close()
        anchor.close()

    assert recorded == ("sqlspec_rt",)
    assert row == [("s3",)]


def test_persistent_write_write_conflict_raises_for_required_secret() -> None:
    database = _database()
    holder = _open_uncommitted_secret(database, "sqlspec_r")
    connection = duckdb.connect(database)
    secret = {"name": "sqlspec_r", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": True}
    try:
        with pytest.raises(duckdb.TransactionException, match="write-write conflict"):
            _create_secret(connection, secret)
    finally:
        connection.close()
        holder.close()


def test_persistent_write_write_conflict_skips_optional_secret() -> None:
    database = _database()
    holder = _open_uncommitted_secret(database, "sqlspec_r")
    connection = duckdb.connect(database)
    secret = {"name": "sqlspec_r", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": False}
    try:
        assert _create_secret(connection, secret) is False
    finally:
        connection.close()
        holder.close()


def test_transient_write_write_conflict_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    database = _database()
    holder = _open_uncommitted_secret(database, "sqlspec_r")
    connection = duckdb.connect(database)
    secret = {"name": "sqlspec_r", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": True}
    backoffs: list[float] = []

    def commit_holder_instead_of_sleeping(seconds: float) -> None:
        backoffs.append(seconds)
        holder.commit()

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.time.sleep", commit_holder_instead_of_sleeping)
    try:
        created = _create_secret(connection, secret)
        row = _secret_count(connection, "sqlspec_r")
    finally:
        connection.close()
        holder.close()

    assert created
    assert backoffs == [SECRET_CONFLICT_BACKOFF]
    assert row == (1,)


def test_pool_secret_creation_waits_for_process_secret_lock() -> None:
    database = _database()
    anchor = duckdb.connect(database)
    pool = DuckDBConnectionPool(
        connection_config={"database": database},
        secrets=[{"name": "sqlspec_l", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": True}],
    )
    recorded: list[tuple[str, ...]] = []

    def worker() -> None:
        try:
            recorded.append(_recorded_secret_names(pool, pool.acquire()))
        finally:
            pool.close()

    thread = threading.Thread(target=worker)
    try:
        with _SECRET_LOCK:
            thread.start()
            thread.join(timeout=0.1)
            blocked = thread.is_alive()
            row_while_locked = _secret_count(anchor, "sqlspec_l")
        thread.join()
        row_after_release = _secret_count(anchor, "sqlspec_l")
    finally:
        anchor.close()

    assert blocked
    assert row_while_locked == (0,)
    assert recorded == [("sqlspec_l",)]
    assert row_after_release == (1,)


@pytest.mark.parametrize("required", [True, False], ids=["required", "optional"])
@pytest.mark.parametrize(
    ("pool_count", "threads_per_pool"), [(1, 8), (2, 4), (8, 2)], ids=["one_pool", "two_pools", "many_pools"]
)
def test_concurrent_first_use_records_declared_secret(pool_count: int, threads_per_pool: int, required: bool) -> None:
    database = _database()
    secret = {"name": "sqlspec_c", "secret_type": "http", "value": HTTP_SECRET_VALUE, "required": required}
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
