"""Use SQLSpecSyncService and SQLSpecAsyncService for short helper sessions and explicit transactions."""

import asyncio
import tempfile
from pathlib import Path
from typing import NoReturn

from sqlspec import SQLFileLoader, sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

__all__ = ("main", "run_async_service_example", "run_sync_service_example")


def run_sync_service_example(db_path: Path) -> int:
    """Execute synchronous service helpers and atomic transaction blocks."""
    config = SqliteConfig(connection_config={"database": str(db_path)})
    service = SQLSpecSyncService(config=config, loader=SQLFileLoader())
    users = sql.select("name").from_("users")

    try:
        with service.begin_transaction() as session:
            session.execute("CREATE TABLE users (name TEXT)")
            session.execute("INSERT INTO users VALUES (:name)", name="Ada")

        assert service.get_one(users) == {"name": "Ada"}
        assert service.exists(users)
        assert service.paginate(users).total == 1

        with service.provide_session() as session:
            assert service.exists(users, session=session)
            assert service.get_one(users, session=session) == {"name": "Ada"}

        with service.begin_transaction() as session:
            session.execute("INSERT INTO users VALUES (:name)", name="Grace")
            assert service.exists(sql.select("name").from_("users").where_eq("name", "Grace"))
            assert service.paginate(users).total == 2

        try:
            with service.begin_transaction() as session:
                session.execute("INSERT INTO users VALUES (:name)", name="Rolled back")
                _abort_transaction()
        except ValueError:
            pass

        total = service.paginate(users).total
        assert total == 2
        return total
    finally:
        config.close_pool()


async def run_async_service_example(db_path: Path) -> int:
    """Execute asynchronous service helpers and atomic transaction blocks."""
    config = AiosqliteConfig(connection_config={"database": str(db_path)})
    service = SQLSpecAsyncService(config=config, loader=SQLFileLoader())
    users = sql.select("name").from_("users")

    try:
        async with service.begin_transaction() as session:
            await session.execute("CREATE TABLE users (name TEXT)")
            await session.execute("INSERT INTO users VALUES (:name)", name="Ada")

        assert await service.get_one(users) == {"name": "Ada"}
        assert await service.exists(users)
        assert (await service.paginate(users)).total == 1

        async with service.provide_session() as session:
            assert await service.exists(users, session=session)
            assert await service.get_one(users, session=session) == {"name": "Ada"}

        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO users VALUES (:name)", name="Grace")
            assert await service.exists(sql.select("name").from_("users").where_eq("name", "Grace"))
            assert (await service.paginate(users)).total == 2

        try:
            async with service.begin_transaction() as session:
                await session.execute("INSERT INTO users VALUES (:name)", name="Rolled back")
                _abort_transaction()
        except ValueError:
            pass

        total = (await service.paginate(users)).total
        assert total == 2
        return total
    finally:
        await config.close_pool()


async def main() -> tuple[int, int]:
    """Run both synchronous and asynchronous service examples in a temporary directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        sync_total = run_sync_service_example(base_dir / "sync_users.sqlite")
        async_total = await run_async_service_example(base_dir / "async_users.sqlite")
        return sync_total, async_total


def _abort_transaction() -> NoReturn:
    """Raise an error inside a transaction block to demonstrate automatic rollback."""
    msg = "Cancel this change"
    raise ValueError(msg)


if __name__ == "__main__":
    print(asyncio.run(main()))
