"""Global conftest.py for SQLSpec unit tests.

Provides fixtures for configuration, caching, SQL statements, mock databases,
and test isolation.
"""

import sqlite3
from collections.abc import AsyncGenerator, Generator

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteDriver

__all__ = ("aiosqlite_async_driver", "sqlite_sync_driver")


class FixtureSqliteDriver(SqliteDriver):
    """Test-friendly SQLite driver that allows patching."""

    pass


class FixtureAiosqliteDriver(AiosqliteDriver):
    """Test-friendly aiosqlite driver that allows patching."""

    pass


@pytest.fixture
def sqlite_sync_driver() -> Generator[FixtureSqliteDriver, None, None]:
    """Fixture for a real SQLite sync driver using in-memory database."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO users (name) VALUES ('test'), ('example')")
        conn.commit()
        yield FixtureSqliteDriver(conn)
    finally:
        conn.close()


@pytest.fixture
async def aiosqlite_async_driver() -> AsyncGenerator[FixtureAiosqliteDriver, None]:
    """Fixture for a real aiosqlite async driver using in-memory database."""
    conn = await aiosqlite.connect(":memory:")
    try:
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO users (name) VALUES ('test'), ('example')")
        await conn.commit()
        yield FixtureAiosqliteDriver(conn)
    finally:
        await conn.close()


def _coverage_is_active(config: pytest.Config) -> bool:
    if not config.pluginmanager.has_plugin("pytest_cov"):
        return False
    return bool(config.getoption("--cov", default=None))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "benchmark: wall-clock-sensitive perf test; skipped when --cov is active "
        "because sys.settrace overhead invalidates the timing threshold.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: "list[pytest.Item]") -> None:
    if not _coverage_is_active(config):
        return
    skip = pytest.mark.skip(reason="benchmark test skipped under --cov: coverage tracing skews wall-clock timing")
    for item in items:
        if "benchmark" in item.keywords:
            item.add_marker(skip)
