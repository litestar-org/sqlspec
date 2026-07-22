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
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig

__all__ = ("aiosqlite_async_driver", "sample_sql_statement", "sample_statement_config", "sqlite_sync_driver")


class FixtureSqliteDriver(SqliteDriver):
    """Test-friendly SQLite driver that allows patching."""

    pass


class FixtureAiosqliteDriver(AiosqliteDriver):
    """Test-friendly aiosqlite driver that allows patching."""

    pass


@pytest.fixture
def sample_statement_config() -> StatementConfig:
    """Return a sample SQLite statement configuration."""
    return StatementConfig(
        dialect="sqlite",
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
        ),
    )


@pytest.fixture
def sample_sql_statement(sample_statement_config: StatementConfig) -> SQL:
    """Return a sample parameterized SQL statement."""
    return SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=sample_statement_config)


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
