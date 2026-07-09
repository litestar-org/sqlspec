"""Unit tests for empty execute_many behavior in driver base classes."""

import pytest

from tests.unit.conftest import FixtureAiosqliteDriver, FixtureSqliteDriver


def test_sync_empty_execute_many_is_noop_zero(sqlite_sync_driver: FixtureSqliteDriver) -> None:
    """Empty execute_many should be a silent no-op returning rows_affected=0."""
    result = sqlite_sync_driver.execute_many("INSERT INTO users (name) VALUES (?)", [])
    assert result.rows_affected == 0

    remaining = sqlite_sync_driver.execute("SELECT COUNT(*) AS c FROM users").data
    assert remaining[0][0] == 2


@pytest.mark.anyio
async def test_async_empty_execute_many_is_noop_zero(aiosqlite_async_driver: FixtureAiosqliteDriver) -> None:
    """Empty execute_many should be a silent no-op returning rows_affected=0 on async drivers."""
    result = await aiosqlite_async_driver.execute_many("INSERT INTO users (name) VALUES (?)", [])
    assert result.rows_affected == 0

    remaining = (await aiosqlite_async_driver.execute("SELECT COUNT(*) AS c FROM users")).data
    assert remaining[0][0] == 2
