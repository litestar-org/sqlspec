"""Config-built services keep helper sessions short and transactions explicit."""

from pathlib import Path

__all__ = ("test_async_service_config", "test_sync_service_config")


def test_sync_service_config(tmp_path: Path) -> None:
    # start-sync-example
    from sqlspec import sql
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.loader import SQLFileLoader
    from sqlspec.service import SQLSpecSyncService

    config = SqliteConfig(connection_config={"database": str(tmp_path / "users.sqlite")})
    service = SQLSpecSyncService(config=config, loader=SQLFileLoader())
    users = sql.select("name").from_("users")

    try:
        with service.begin_transaction() as session:
            session.execute("CREATE TABLE users (name TEXT)")
            session.execute("INSERT INTO users VALUES (:name)", name="Ada")

        # Each bare helper acquires and releases its own session.
        assert service.get_one(users) == {"name": "Ada"}
        assert service.exists(users)
        assert service.paginate(users).total == 1

        # Explicit reuse borrows this session without starting a transaction.
        with service.provide_session() as session:
            assert service.exists(users, session=session)
            assert service.get_one(users, session=session) == {"name": "Ada"}

        # Both helpers see the same uncommitted transaction.
        with service.begin_transaction() as session:
            session.execute("INSERT INTO users VALUES (:name)", name="Grace")
            assert service.exists(sql.select("name").from_("users").where_eq("name", "Grace"))
            assert service.paginate(users).total == 2

        try:
            with service.begin_transaction() as session:
                session.execute("INSERT INTO users VALUES (:name)", name="Rolled back")
                msg = "Cancel this change"
                raise ValueError(msg)  # noqa: TRY301 - Demonstrate an application error rolling back the block.
        except ValueError:
            pass

        assert service.paginate(users).total == 2
    finally:
        config.close_pool()
    # end-sync-example


async def test_async_service_config(tmp_path: Path) -> None:
    # start-async-example
    from sqlspec import sql
    from sqlspec.adapters.aiosqlite import AiosqliteConfig
    from sqlspec.loader import SQLFileLoader
    from sqlspec.service import SQLSpecAsyncService

    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "users.sqlite")})
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
                msg = "Cancel this change"
                raise ValueError(msg)  # noqa: TRY301 - Demonstrate an application error rolling back the block.
        except ValueError:
            pass

        assert (await service.paginate(users)).total == 2
    finally:
        await config.close_pool()
    # end-async-example
