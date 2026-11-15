"""Unit tests for the SQLite thread-local connection pool."""

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest

from sqlspec.adapters.sqlite import pool as sqlite_pool_module
from sqlspec.adapters.sqlite.core import end_transaction
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite._typing import SqliteConnection

pytest.importorskip("sqlite3", reason="SQLite adapter requires the stdlib sqlite3 module")


def _read_pragma(
    connection: "SqliteConnection",
    statement: Literal[
        "PRAGMA foreign_keys",
        "PRAGMA busy_timeout",
        "PRAGMA cache_size",
        "PRAGMA journal_mode",
        "PRAGMA synchronous",
        "PRAGMA temp_store",
    ],
) -> int | str:
    row = connection.execute(statement).fetchone()
    assert row is not None
    return cast("int | str", row[0])


def test_default_pool_uses_native_like_shared_pragma_profile() -> None:
    pool = SqliteConnectionPool({"database": ":memory:", "timeout": 30.0})
    try:
        with pool.get_connection() as connection:
            assert _read_pragma(connection, "PRAGMA foreign_keys") == 0
            assert _read_pragma(connection, "PRAGMA busy_timeout") == 5000
            assert _read_pragma(connection, "PRAGMA cache_size") == -16000
            assert _read_pragma(connection, "PRAGMA journal_mode") == "memory"
            assert _read_pragma(connection, "PRAGMA synchronous") == 0
            assert _read_pragma(connection, "PRAGMA temp_store") == 2
    finally:
        pool.close()


def test_default_memory_connections_do_not_read_uncommitted_rows() -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig()
    other = config.provide_pool().new_connection()
    try:
        with config.provide_connection() as connection:
            connection.execute("CREATE TABLE isolated_rows (value INTEGER)")
            connection.execute("BEGIN")
            connection.execute("INSERT INTO isolated_rows VALUES (1)")
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                other.execute("SELECT value FROM isolated_rows")
            connection.rollback()
            assert other.execute("SELECT COUNT(*) FROM isolated_rows").fetchone() == (0,)
    finally:
        other.close()
        config.close_pool()


def test_disable_optimizations_preserves_native_memory_profile() -> None:
    native_connection = sqlite3.connect(":memory:", timeout=30.0)
    pool = SqliteConnectionPool({"database": ":memory:", "timeout": 30.0}, enable_optimizations=False)
    try:
        with pool.get_connection() as connection:
            assert _read_pragma(connection, "PRAGMA journal_mode") == _read_pragma(
                native_connection, "PRAGMA journal_mode"
            )
            assert _read_pragma(connection, "PRAGMA synchronous") == _read_pragma(
                native_connection, "PRAGMA synchronous"
            )
            assert _read_pragma(connection, "PRAGMA temp_store") == _read_pragma(native_connection, "PRAGMA temp_store")
            assert _read_pragma(connection, "PRAGMA cache_size") == _read_pragma(native_connection, "PRAGMA cache_size")
            assert _read_pragma(connection, "PRAGMA busy_timeout") == _read_pragma(
                native_connection, "PRAGMA busy_timeout"
            )
            assert _read_pragma(connection, "PRAGMA foreign_keys") == _read_pragma(
                native_connection, "PRAGMA foreign_keys"
            )
    finally:
        pool.close()
        native_connection.close()


def test_file_pool_uses_wal_normal_and_shared_busy_timeout(tmp_path: Path) -> None:
    pool = SqliteConnectionPool({"database": tmp_path / "profile.db", "timeout": 30.0})
    try:
        with pool.get_connection() as connection:
            assert _read_pragma(connection, "PRAGMA journal_mode") == "wal"
            assert _read_pragma(connection, "PRAGMA synchronous") == 1
            assert _read_pragma(connection, "PRAGMA busy_timeout") == 5000
            assert _read_pragma(connection, "PRAGMA foreign_keys") == 0
    finally:
        pool.close()


def test_setup_failure_closes_raw_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlspec.adapters.sqlite import pool as pool_module

    class _FailingConnection:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, statement: str) -> None:
            if statement == "PRAGMA foreign_keys = ON":
                raise RuntimeError("foreign key setup failed")

        def close(self) -> None:
            self.closed = True

    connection = _FailingConnection()
    monkeypatch.setattr(pool_module.sqlite3, "connect", lambda **_: cast(Any, connection))
    pool = SqliteConnectionPool({"database": ":memory:"}, enable_optimizations=False, enable_foreign_keys=True)

    with pytest.raises(RuntimeError, match="foreign key setup failed"):
        pool._create_connection()

    assert connection.closed is True


@pytest.mark.parametrize(
    ("enable_optimizations", "enable_foreign_keys", "expected_foreign_keys", "expected_cache_size"),
    [(False, True, 1, None), (True, False, 0, -16000)],
)
def test_optimization_and_foreign_key_flags_are_independent(
    enable_optimizations: bool, enable_foreign_keys: bool, expected_foreign_keys: int, expected_cache_size: int | None
) -> None:
    pool = SqliteConnectionPool(
        {"database": ":memory:"}, enable_optimizations=enable_optimizations, enable_foreign_keys=enable_foreign_keys
    )
    try:
        with pool.get_connection() as connection:
            assert _read_pragma(connection, "PRAGMA foreign_keys") == expected_foreign_keys
            cache_size = _read_pragma(connection, "PRAGMA cache_size")
            if expected_cache_size is None:
                assert cache_size != -16000
            else:
                assert cache_size == expected_cache_size
    finally:
        pool.close()


def test_get_connection_rolls_back_open_transaction_on_exception() -> None:
    """A mid-transaction exception must not commit partial work.

    The pool is thread-local, so the connection survives the failed block; an
    open transaction must be rolled back (not committed) when the caller's
    with-block exits via exception.
    """
    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"}, enable_optimizations=False)
    with pool.get_connection() as connection:
        connection.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
        connection.commit()

    class _BoomError(RuntimeError):
        pass

    with pytest.raises(_BoomError), pool.get_connection() as connection:
        connection.execute("INSERT INTO widgets (id) VALUES (1)")
        assert connection.in_transaction
        raise _BoomError

    with pool.get_connection() as connection:
        count = connection.execute("SELECT COUNT(*) FROM widgets").fetchone()[0]
    assert count == 0

    pool.close()


def test_get_connection_commits_open_transaction_on_clean_exit() -> None:
    """A clean with-block exit commits the open transaction."""
    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"}, enable_optimizations=False)
    with pool.get_connection() as connection:
        connection.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
        connection.commit()

    with pool.get_connection() as connection:
        connection.execute("INSERT INTO widgets (id) VALUES (1)")

    with pool.get_connection() as connection:
        count = connection.execute("SELECT COUNT(*) FROM widgets").fetchone()[0]
    assert count == 1

    pool.close()


def test_get_connection_exits_cleanly_on_another_thread() -> None:
    """A scope entered on one thread can be exited on another without losing the commit."""
    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"}, enable_optimizations=False)
    scope = pool.get_connection()
    connection = scope.__enter__()
    connection.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
    connection.execute("INSERT INTO widgets (id) VALUES (1)")
    errors: list[BaseException] = []

    def _exit() -> None:
        try:
            scope.__exit__(None, None, None)
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=_exit)
    worker.start()
    worker.join(timeout=10)

    assert errors == []
    assert pool.checked_out() == 0
    assert connection.execute("SELECT COUNT(*) FROM widgets").fetchone()[0] == 1
    pool.close()


def test_close_closes_connections_opened_on_other_threads(tmp_path: Path) -> None:
    """Connections opened by worker threads must not survive pool shutdown."""
    pool = SqliteConnectionPool({"database": str(tmp_path / "threads.sqlite")})
    opened: list[SqliteConnection] = []
    barrier = threading.Barrier(3, timeout=10)
    errors: list[Exception] = []

    def _open() -> None:
        try:
            opened.append(pool.acquire())
        except Exception as exc:
            errors.append(exc)
        finally:
            barrier.wait()

    workers = [threading.Thread(target=_open) for _ in range(2)]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join(timeout=15)

    assert errors == []
    assert all(not worker.is_alive() for worker in workers)
    assert len({id(connection) for connection in opened}) == 2

    pool.close()

    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")


def test_new_connection_is_independent_of_the_thread_local_connection(tmp_path: Path) -> None:
    """A standalone connection must not be the one the pool hands out."""
    pool = SqliteConnectionPool({"database": str(tmp_path / "standalone.sqlite")})
    try:
        pooled = pool.acquire()
        standalone = pool.new_connection()

        assert standalone is not pooled

        standalone.close()

        assert pool.acquire() is pooled
        pooled.execute("SELECT 1")
    finally:
        pool.close()


class _AutocommitConnection:
    """Mimics a Python 3.12+ sqlite3 connection in autocommit mode."""

    def __init__(self) -> None:
        self.autocommit = True
        self.in_transaction = True
        self.statements: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def execute(self, sql: str, parameters: object = ()) -> None:
        _ = parameters
        self.statements.append(sql)

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


@pytest.mark.parametrize(("commit", "statement"), [(True, "COMMIT"), (False, "ROLLBACK")], ids=["commit", "rollback"])
def test_pool_ends_autocommit_transactions_with_an_explicit_statement(commit: bool, statement: str) -> None:
    """The pool's own commit is a no-op in autocommit mode, exactly like the driver's."""
    connection = _AutocommitConnection()

    end_transaction(cast("Any", connection), commit=commit, supports_autocommit=True)

    assert connection.statements == [statement]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 0


def test_pool_uses_the_dbapi_methods_outside_autocommit_mode() -> None:
    """Legacy connections keep the DB-API path."""
    connection = _AutocommitConnection()
    connection.autocommit = False

    end_transaction(cast("Any", connection), commit=True, supports_autocommit=True)

    assert connection.statements == []
    assert connection.commit_calls == 1


def test_pool_skips_ending_a_transaction_that_is_not_open() -> None:
    connection = _AutocommitConnection()
    connection.in_transaction = False

    end_transaction(cast("Any", connection), commit=True, supports_autocommit=True)

    assert connection.statements == []
    assert connection.commit_calls == 0


def test_close_does_not_leave_other_threads_holding_a_closed_connection(tmp_path: Path) -> None:
    """After shutdown a worker thread must get a fresh connection, not a dead one."""
    pool = SqliteConnectionPool({"database": str(tmp_path / "generation.sqlite")})
    results: list[str] = []
    opened = threading.Event()
    closed = threading.Event()

    def _worker() -> None:
        pool.acquire()
        opened.set()
        closed.wait(timeout=5)
        try:
            pool.acquire().execute("SELECT 1")
            results.append("ok")
        except Exception as exc:
            results.append(type(exc).__name__)

    worker = threading.Thread(target=_worker)
    worker.start()
    opened.wait(timeout=5)
    pool.close()
    closed.set()
    worker.join(timeout=5)
    pool.close()

    assert results == ["ok"]


def test_a_connection_from_a_retired_generation_is_closed_and_deregistered() -> None:
    """Dropping the handle without closing it would leak the connection and its file lock."""
    pool = SqliteConnectionPool({"database": ":memory:"})
    try:
        connection = pool.acquire()
        registry = pool._connection_registry  # pyright: ignore[reportPrivateUsage]
        assert connection in registry

        pool._generation += 1  # pyright: ignore[reportPrivateUsage]
        replacement = pool.acquire()

        assert replacement is not connection
        assert connection not in registry
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
    finally:
        pool.close()


def test_concurrent_first_acquire_on_new_file_database_succeeds(tmp_path: Path) -> None:
    for iteration in range(20):
        pool = SqliteConnectionPool({"database": str(tmp_path / f"concurrent-{iteration}.sqlite")})
        barrier = threading.Barrier(4, timeout=10)
        errors: list[Exception] = []
        modes: list[int | str] = []

        def acquire() -> None:
            try:
                barrier.wait()
                modes.append(_read_pragma(pool.acquire(), "PRAGMA journal_mode"))
            except Exception as exc:
                errors.append(exc)

        workers = [threading.Thread(target=acquire) for _ in range(4)]
        try:
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(timeout=15)
            assert all(not worker.is_alive() for worker in workers)
            assert errors == []
            assert modes == ["wal"] * 4
        finally:
            pool.close()


@pytest.mark.parametrize("failures", [2, 50])
def test_enable_wal_retries_locked_errors(monkeypatch: pytest.MonkeyPatch, failures: int) -> None:
    from unittest.mock import Mock

    from sqlspec.adapters.sqlite import pool as pool_module

    connection = Mock()
    connection.execute.side_effect = [sqlite3.OperationalError("database is locked")] * failures + [None]
    monkeypatch.setattr(pool_module.time, "sleep", lambda _: None)
    if failures == 50:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            pool_module._enable_wal(connection)
        assert connection.execute.call_count == 50
    else:
        pool_module._enable_wal(connection)
        assert connection.execute.call_count == 3


def test_enable_wal_reraises_other_operational_errors() -> None:
    from unittest.mock import Mock

    from sqlspec.adapters.sqlite import pool as pool_module

    connection = Mock()
    connection.execute.side_effect = sqlite3.OperationalError("disk I/O error")
    with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
        pool_module._enable_wal(connection)
    assert connection.execute.call_count == 1


def test_memory_pool_replaces_a_closed_connection() -> None:
    pool = SqliteConnectionPool({"database": ":memory:"}, health_check_interval=-1)
    try:
        with pool.get_connection() as connection:
            connection.close()
        with pool.get_connection() as replacement:
            assert replacement.execute("SELECT 1").fetchone() == (1,)
    finally:
        pool.close()


def test_nested_get_connection_reentrancy_depth() -> None:
    pool = SqliteConnectionPool({"database": ":memory:"})
    try:
        with pool.get_connection() as conn1:
            conn1.execute("CREATE TABLE test_depth (x INT)")
            conn1.commit()
            conn1.execute("INSERT INTO test_depth VALUES (1)")
            with pool.get_connection() as conn2:
                assert conn1 is conn2
                conn2.execute("INSERT INTO test_depth VALUES (2)")
            assert getattr(conn1, "in_transaction", False)
            conn1.commit()

        with pool.get_connection() as conn:
            rows = conn.execute("SELECT * FROM test_depth").fetchall()
            assert rows == [(1,), (2,)]
    finally:
        pool.close()


def test_sqlite_pool_checked_out_reflects_active_sessions_and_prunes_dead_threads() -> None:
    pool = SqliteConnectionPool({"database": ":memory:"})
    try:
        assert pool.checked_out() == 0
        with pool.get_connection():
            assert pool.checked_out() == 1
        assert pool.checked_out() == 0

        def worker() -> None:
            with pool.get_connection() as conn:
                conn.execute("SELECT 1")

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert not t.is_alive()
        assert pool.size() == 1
    finally:
        pool.close()


@pytest.mark.parametrize("path", ["get_connection_exception", "get_connection_commit_fallback", "release"])
def test_sqlite_pool_retires_thread_connection_when_rollback_fails(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    """Failed rollback retires the thread-local connection so the next checkout opens a fresh one."""
    pool = SqliteConnectionPool({"database": ":memory:"})
    original_end_tx = sqlite_pool_module.end_transaction
    try:
        with pool.get_connection() as first_conn:
            first_conn.execute("CREATE TABLE items (id INT)")
            first_conn.commit()

        def failing_end_tx(conn: Any, *, commit: bool) -> None:
            if commit:
                raise sqlite3.OperationalError("commit failed")
            raise sqlite3.OperationalError("rollback failed")

        monkeypatch.setattr(sqlite_pool_module, "end_transaction", failing_end_tx)

        if path == "get_connection_exception":
            with pytest.raises(ValueError, match="caller boom"), pool.get_connection() as conn:
                assert conn is first_conn
                raise ValueError("caller boom")
        elif path == "get_connection_commit_fallback":
            with pytest.raises(sqlite3.OperationalError, match="commit failed"), pool.get_connection() as conn:
                assert conn is first_conn
        else:
            acquired = pool.acquire()
            assert acquired is first_conn
            pool.release(acquired)

        monkeypatch.setattr(sqlite_pool_module, "end_transaction", original_end_tx)
        with pool.get_connection() as second_conn:
            assert second_conn is not first_conn
            assert second_conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        pool.close()


def test_sqlite_pool_isolates_connections_and_transactions_across_asyncio_tasks(tmp_path: Path) -> None:
    """Concurrent asyncio tasks on the same thread receive isolated connections and transactions."""
    db_path = tmp_path / "async_tasks.db"
    with sqlite3.connect(db_path) as setup_conn:
        setup_conn.execute("CREATE TABLE items (label TEXT)")
        setup_conn.commit()
    pool = SqliteConnectionPool({"database": str(db_path)})

    async def _run() -> None:
        task_a_in_tx = asyncio.Event()
        task_b_finished = asyncio.Event()
        seen_connections: list[int] = []
        task_b_saw_rows: list[Any] = []
        task_a_remained_in_tx: list[bool] = []

        async def committing_worker() -> int:
            with pool.get_connection() as conn:
                seen_connections.append(id(conn))
                conn.execute("INSERT INTO items VALUES ('committed')")
                task_a_in_tx.set()
                await task_b_finished.wait()
                task_a_remained_in_tx.append(bool(getattr(conn, "in_transaction", False)))
                return id(conn)

        async def failing_worker() -> int:
            await task_a_in_tx.wait()
            try:
                with pool.get_connection() as conn:
                    seen_connections.append(id(conn))
                    assert pool.size() == 2
                    assert pool.checked_out() == 2
                    task_b_saw_rows.extend(conn.execute("SELECT label FROM items").fetchall())
                    msg = "abort task b"
                    raise ValueError(msg)
            finally:
                task_b_finished.set()

        t1 = asyncio.create_task(committing_worker())
        t2 = asyncio.create_task(failing_worker())
        results = await asyncio.gather(t1, t2, return_exceptions=True)

        assert isinstance(results[0], int)
        assert isinstance(results[1], ValueError)
        assert len(set(seen_connections)) == 2
        assert task_b_saw_rows == []
        assert task_a_remained_in_tx == [True]
        assert pool.size() == 0

    try:
        asyncio.run(_run())
        with pool.get_connection() as verify_conn:
            rows = verify_conn.execute("SELECT label FROM items ORDER BY label").fetchall()
            assert rows == [("committed",)]
    finally:
        pool.close()


def test_sqlite_pool_respects_explicit_uri_false_for_memory_substrings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pool with uri=False treats file:-prefixed memory-like names as literal file paths."""
    monkeypatch.chdir(tmp_path)
    db_name = "file:disk_target?mode=memory&cache=shared"
    pool = SqliteConnectionPool({"database": db_name, "uri": False})
    try:
        assert pool._is_memory_db is False
        assert pool._is_shared_memory is False
        assert pool._database_name == db_name
        with pool.get_connection() as conn:
            conn.execute("CREATE TABLE t (id INT)")
        assert (tmp_path / db_name).exists()
    finally:
        pool.close()


def test_current_async_task_returns_none_without_running_loop() -> None:
    """_current_async_task returns None synchronously and the active Task inside an event loop."""
    assert sqlite_pool_module._current_async_task() is None

    async def _inside() -> bool:
        return sqlite_pool_module._current_async_task() is asyncio.current_task()

    assert asyncio.run(_inside()) is True


@pytest.mark.parametrize("db_kind", ["file", "shared_memory", "private_memory"])
def test_sqlite_pool_reuses_idle_connections_for_sequential_asyncio_tasks(
    tmp_path: Path, db_kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sequential asyncio tasks reuse idle task connections without opening N connections."""
    if db_kind == "file":
        pool = SqliteConnectionPool({"database": str(tmp_path / "sequential.db")})
    elif db_kind == "shared_memory":
        pool = SqliteConnectionPool({"database": f"file:seq_{id(tmp_path)}?mode=memory&cache=shared", "uri": True})
    else:
        pool = SqliteConnectionPool({"database": ":memory:"})

    original_new_connection = SqliteConnectionPool.new_connection
    created_connections: list[Any] = []

    def counting_new_connection(pool_self: SqliteConnectionPool) -> Any:
        conn = original_new_connection(pool_self)
        if pool_self is pool:
            created_connections.append(conn)
        return conn

    monkeypatch.setattr(SqliteConnectionPool, "new_connection", counting_new_connection)

    async def _run() -> list[int]:
        seen_ids: list[int] = []

        async def worker(idx: int) -> None:
            with pool.get_connection() as conn:
                seen_ids.append(id(conn))
                assert conn.execute("SELECT ?", (idx,)).fetchone() == (idx,)

        for idx in range(20):
            await asyncio.create_task(worker(idx))

        return seen_ids

    try:
        seen_ids = asyncio.run(_run())
        assert len(created_connections) == 1
        assert len(set(seen_ids)) == 1
        assert pool.size() == 0
        assert len(pool._idle_task_connections) == 1
    finally:
        pool.close()

    assert pool._idle_task_connections == []
    with pytest.raises(sqlite3.ProgrammingError):
        created_connections[0].execute("SELECT 1")


def test_sqlite_pool_caps_idle_task_connections_and_closes_dirty_connections(tmp_path: Path) -> None:
    """Concurrent tasks isolate connections, idle cache caps at 8, and dirty transactions close."""
    pool = SqliteConnectionPool({"database": str(tmp_path / "cap.db")})

    async def _run() -> None:
        all_entered = asyncio.Event()
        entered = 0
        seen_ids: list[int] = []

        async def concurrent_worker() -> None:
            nonlocal entered
            with pool.get_connection() as conn:
                seen_ids.append(id(conn))
                entered += 1
                if entered == 12:
                    all_entered.set()
                await all_entered.wait()

        tasks = [asyncio.create_task(concurrent_worker()) for _ in range(12)]
        await asyncio.gather(*tasks)
        assert len(set(seen_ids)) == 12
        assert pool.size() == 0
        assert len(pool._idle_task_connections) == 8

        dirty_conns: list[Any] = []

        async def dirty_worker() -> None:
            conn = pool.acquire()
            dirty_conns.append(conn)
            conn.execute("CREATE TABLE IF NOT EXISTS dirty_t (x INT)")
            conn.execute("INSERT INTO dirty_t VALUES (1)")
            assert conn.in_transaction

        await asyncio.create_task(dirty_worker())
        assert pool.size() == 0
        assert len(pool._idle_task_connections) == 7
        with pytest.raises(sqlite3.ProgrammingError):
            dirty_conns[0].execute("SELECT 1")

    try:
        asyncio.run(_run())
    finally:
        pool.close()
    assert pool._idle_task_connections == []
