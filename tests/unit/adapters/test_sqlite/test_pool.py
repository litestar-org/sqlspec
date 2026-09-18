"""Unit tests for the SQLite thread-local connection pool."""

import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest

from sqlspec.adapters.sqlite.pool import SqliteConnectionPool, _end_transaction

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


def test_close_closes_connections_opened_on_other_threads(tmp_path: Path) -> None:
    """Connections opened by worker threads must not survive pool shutdown."""
    pool = SqliteConnectionPool({"database": str(tmp_path / "threads.sqlite")})
    opened: list[SqliteConnection] = []
    barrier = threading.Barrier(3)

    def _open() -> None:
        opened.append(pool.acquire())
        barrier.wait()

    workers = [threading.Thread(target=_open) for _ in range(2)]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join()

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

    _end_transaction(cast("Any", connection), commit=commit, supports_autocommit=True)

    assert connection.statements == [statement]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 0


def test_pool_uses_the_dbapi_methods_outside_autocommit_mode() -> None:
    """Legacy connections keep the DB-API path."""
    connection = _AutocommitConnection()
    connection.autocommit = False

    _end_transaction(cast("Any", connection), commit=True, supports_autocommit=True)

    assert connection.statements == []
    assert connection.commit_calls == 1


def test_pool_skips_ending_a_transaction_that_is_not_open() -> None:
    connection = _AutocommitConnection()
    connection.in_transaction = False

    _end_transaction(cast("Any", connection), commit=True, supports_autocommit=True)

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
