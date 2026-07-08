"""Unit tests for the SQLite thread-local connection pool."""

import pytest

from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

pytest.importorskip("sqlite3", reason="SQLite adapter requires the stdlib sqlite3 module")


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
