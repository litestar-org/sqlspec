"""Integration tests for SQLite-family exception mapping."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.exceptions import UniqueViolationError

pytestmark = pytest.mark.xdist_group("sqlite")


def test_sqlite_primary_key_conflict_raises_unique_violation(sqlite_session: SqliteDriver) -> None:
    """Duplicate primary key insert raises UniqueViolationError on sync SQLite."""
    sqlite_session.execute_script("CREATE TABLE test_pk (id INTEGER PRIMARY KEY, name TEXT)")
    sqlite_session.execute("INSERT INTO test_pk (id, name) VALUES (?, ?)", (1, "first"))
    with pytest.raises(UniqueViolationError):
        sqlite_session.execute("INSERT INTO test_pk (id, name) VALUES (?, ?)", (1, "second"))


async def test_aiosqlite_primary_key_conflict_raises_unique_violation(aiosqlite_session: AiosqliteDriver) -> None:
    """Duplicate primary key insert raises UniqueViolationError on async aiosqlite."""
    await aiosqlite_session.execute_script("CREATE TABLE test_pk_async (id INTEGER PRIMARY KEY, name TEXT)")
    await aiosqlite_session.execute("INSERT INTO test_pk_async (id, name) VALUES (?, ?)", (1, "first"))
    with pytest.raises(UniqueViolationError):
        await aiosqlite_session.execute("INSERT INTO test_pk_async (id, name) VALUES (?, ?)", (1, "second"))
