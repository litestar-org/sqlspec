"""Unit tests for the SQLite thread-local connection pool."""

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest

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
