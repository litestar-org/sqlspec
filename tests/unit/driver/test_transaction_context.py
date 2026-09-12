"""Driver transaction() context manager contracts."""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig


class _BoomError(Exception):
    pass


@pytest.fixture
async def async_config(tmp_path: Path) -> AsyncIterator[AiosqliteConfig]:
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "transaction.sqlite")})
    async with config.provide_session() as session:
        await session.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        await session.commit()
    yield config
    await config.close_pool()


@pytest.fixture
def sync_config(tmp_path: Path) -> Iterator[SqliteConfig]:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "transaction.sqlite")})
    with config.provide_session() as session:
        session.execute_script("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        session.commit()
    yield config
    config.close_pool()


async def test_async_commit_on_exit(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        async with session.transaction() as tx:
            assert tx is session
            assert session.connection.in_transaction
            await tx.execute("INSERT INTO items (id) VALUES (1)")
        assert not session.connection.in_transaction

    async with async_config.provide_session() as session:
        assert await session.select_value("SELECT COUNT(*) FROM items") == 1


async def test_async_rollback_on_error(async_config: AiosqliteConfig) -> None:
    async with async_config.provide_session() as session:
        with pytest.raises(_BoomError):
            async with session.transaction():
                await session.execute("INSERT INTO items (id) VALUES (1)")
                raise _BoomError
        assert not session.connection.in_transaction
        assert await session.select_value("SELECT COUNT(*) FROM items") == 0

    async with async_config.provide_session() as session:
        assert await session.select_value("SELECT COUNT(*) FROM items") == 0


def test_sync_commit_on_exit(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        with session.transaction() as tx:
            assert tx is session
            assert session.connection.in_transaction
            tx.execute("INSERT INTO items (id) VALUES (1)")
        assert not session.connection.in_transaction

    with sync_config.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM items") == 1


def test_sync_rollback_on_error(sync_config: SqliteConfig) -> None:
    with sync_config.provide_session() as session:
        with pytest.raises(_BoomError):
            with session.transaction():
                session.execute("INSERT INTO items (id) VALUES (1)")
                raise _BoomError
        assert not session.connection.in_transaction
        assert session.select_value("SELECT COUNT(*) FROM items") == 0

    with sync_config.provide_session() as session:
        assert session.select_value("SELECT COUNT(*) FROM items") == 0
