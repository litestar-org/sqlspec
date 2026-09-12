"""AsyncPG driver transaction blocks and nested service transactions."""

from collections.abc import AsyncIterator

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.exceptions import UniqueViolationError
from sqlspec.service import SQLSpecAsyncService

pytestmark = pytest.mark.xdist_group("postgres")


class _BoomError(Exception):
    pass


@pytest.fixture
async def accounts_table(asyncpg_config: AsyncpgConfig) -> AsyncIterator[str]:
    table = "test_service_nested_transactions"
    async with asyncpg_config.provide_session() as session:
        await session.execute_script(
            f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)"
        )
    yield table
    async with asyncpg_config.provide_session() as session:
        await session.execute_script(f"DROP TABLE IF EXISTS {table}")


async def _emails(observer: AsyncpgDriver, table: str) -> list[str]:
    rows = await observer.select(f"SELECT email FROM {table} ORDER BY id")
    return [row["email"] for row in rows]


async def test_driver_transaction_commits_on_exit(asyncpg_config: AsyncpgConfig, accounts_table: str) -> None:
    async with asyncpg_config.provide_session() as session, asyncpg_config.provide_session() as observer:
        async with session.transaction() as tx:
            assert tx is session
            await tx.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 1, "ada@example.com")
            assert await _emails(observer, accounts_table) == []
        assert not session.connection.is_in_transaction()
        assert await _emails(observer, accounts_table) == ["ada@example.com"]


async def test_driver_transaction_rolls_back_on_error(asyncpg_config: AsyncpgConfig, accounts_table: str) -> None:
    async with asyncpg_config.provide_session() as session, asyncpg_config.provide_session() as observer:
        with pytest.raises(_BoomError):
            async with session.transaction():
                await session.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 1, "ada@example.com")
                raise _BoomError
        assert not session.connection.is_in_transaction()
        assert await session.select_value(f"SELECT COUNT(*) FROM {accounts_table}") == 0
        assert await _emails(observer, accounts_table) == []


async def test_unique_violation_inside_nested_block(asyncpg_config: AsyncpgConfig, accounts_table: str) -> None:
    async with asyncpg_config.provide_session() as session:
        await session.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 1, "ada@example.com")

    service = SQLSpecAsyncService(config=asyncpg_config)
    async with service.begin_transaction() as outer:
        await outer.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 2, "grace@example.com")
        with pytest.raises(UniqueViolationError):
            async with service.begin_transaction() as inner:
                assert inner is outer
                await inner.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 3, "ada@example.com")
        existing = await service.get_one(f"SELECT id, email FROM {accounts_table} WHERE email = $1", "ada@example.com")
        assert existing == {"id": 1, "email": "ada@example.com"}
        await outer.execute(f"INSERT INTO {accounts_table} (id, email) VALUES ($1, $2)", 4, "linus@example.com")

    async with asyncpg_config.provide_session() as observer:
        assert await _emails(observer, accounts_table) == ["ada@example.com", "grace@example.com", "linus@example.com"]
