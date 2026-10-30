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
    # --8<-- [start:example]
    service = SQLSpecSyncService(config=config, loader=SQLFileLoader())
    users = sql.select("name").from_("users")

    with service.begin_transaction() as session:
        session.execute("CREATE TABLE users (name TEXT)")
        session.execute("INSERT INTO users VALUES (:name)", name="Ada")

    user = service.get_one(users)
    has_users = service.exists(users)
    paged = service.paginate(users)
    # --8<-- [end:example]

    try:
        assert user == {"name": "Ada"}
        assert has_users
        assert paged.total == 1

        with service.provide_session() as session:
            assert service.exists(users, session=session)
            assert service.get_one(users, session=session) == {"name": "Ada"}

        with service.begin_transaction() as session:
            session.execute("INSERT INTO users VALUES (:name)", name="Grace")
            grace_query = sql.select("name").from_("users").where_eq("name", "Grace")
            assert service.exists(grace_query)
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
        paged = await service.paginate(users)
        assert paged.total == 1

        async with service.provide_session() as session:
            assert await service.exists(users, session=session)
            assert await service.get_one(users, session=session) == {"name": "Ada"}

        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO users VALUES (:name)", name="Grace")
            grace_query = sql.select("name").from_("users").where_eq("name", "Grace")
            assert await service.exists(grace_query)
            paged = await service.paginate(users)
            assert paged.total == 2

        try:
            async with service.begin_transaction() as session:
                await session.execute("INSERT INTO users VALUES (:name)", name="Rolled back")
                _abort_transaction()
        except ValueError:
            pass

        paged = await service.paginate(users)
        total = paged.total
        assert total == 2
        return total
    finally:
        await config.close_pool()


def _abort_transaction() -> NoReturn:
    """Trigger a rollback within a begin_transaction context manager."""
    msg = "Forced transaction rollback"
    raise ValueError(msg)


def main() -> tuple[int, int]:
    """Execute both sync and async service flows against temporary databases."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        sync_total = run_sync_service_example(base_dir / "sync.db")
        async_total = asyncio.run(run_async_service_example(base_dir / "async.db"))
    assert sync_total == 2
    assert async_total == 2
    return sync_total, async_total


if __name__ == "__main__":
    print(main())
