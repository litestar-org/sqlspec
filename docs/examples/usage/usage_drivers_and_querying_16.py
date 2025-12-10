"""Async transaction helper for drivers_and_querying guide."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver

__all__ = ("test_example_16_async_transactions",)


@pytest.mark.anyio
async def test_example_16_async_transactions() -> None:
    spec = SQLSpec()
    config = spec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))

    # start-example
    @asynccontextmanager
    async def transactional_scope(session: "AiosqliteDriver") -> "AsyncIterator[None]":
        await session.begin()
        try:
            yield
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit()

    async with spec.provide_session(config) as session:
        await session.execute("DROP TABLE IF EXISTS accounts")
        await session.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        await session.execute_many("INSERT INTO accounts (id, balance) VALUES (?, ?)", [(1, 200), (2, 100)])

        async with transactional_scope(session):
            await session.execute("UPDATE accounts SET balance = balance - 50 WHERE id = :id", id=1)
            await session.execute("UPDATE accounts SET balance = balance + 50 WHERE id = :id", id=2)
    # end-example

    async with spec.provide_session(config) as verification:
        result = await verification.execute("SELECT balance FROM accounts ORDER BY id")
        assert result.data is not None
        assert [row["balance"] for row in result.data] == [150, 150]

    await spec.close_pool(config)
