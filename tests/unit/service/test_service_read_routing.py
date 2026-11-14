"""Services preserve writer causality with a dedicated reader configuration."""

from pathlib import Path
from typing import cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from sqlspec.utils.routing import RoutingContext


def test_sync_service_dedicated_reader_and_transaction_pinning(tmp_path: Path) -> None:
    writer = SqliteConfig(connection_config={"database": str(tmp_path / "writer.db")}, bind_key="orders")
    reader = SqliteConfig(connection_config={"database": str(tmp_path / "reader.db")}, bind_key="orders_ro")
    for config, label in ((writer, "writer"), (reader, "reader")):
        with config.provide_session() as session:
            session.execute("CREATE TABLE items (origin TEXT)")
            session.execute("INSERT INTO items VALUES (:origin)", {"origin": label})
            session.commit()
    service = SQLSpecSyncService(config=writer, read_config=reader)
    with RoutingContext.scope():
        assert service.get_one("SELECT origin FROM items") == {"origin": "reader"}
        with service.begin_transaction() as session:
            session.execute("INSERT INTO items VALUES ('new')")
            assert service.exists("SELECT origin FROM items WHERE origin = 'new'") is True
        assert service.get_one("SELECT origin FROM items WHERE origin = 'writer'") == {"origin": "writer"}
        assert RoutingContext.get_entry("analytics") is None
    with RoutingContext.primary("orders"):
        assert service.get_one("SELECT origin FROM items WHERE origin = 'writer'") == {"origin": "writer"}
    writer.close_pool()
    reader.close_pool()


async def test_async_service_dedicated_reader_and_rollback(tmp_path: Path) -> None:
    writer = AiosqliteConfig(connection_config={"database": str(tmp_path / "writer.db")}, bind_key="orders")
    reader = AiosqliteConfig(connection_config={"database": str(tmp_path / "reader.db")})
    for config, label in ((writer, "writer"), (reader, "reader")):
        async with config.provide_session() as session:
            await session.execute("CREATE TABLE items (origin TEXT)")
            await session.execute("INSERT INTO items VALUES (:origin)", {"origin": label})
            await session.commit()
    service = SQLSpecAsyncService(config=writer, read_config=reader)
    async with RoutingContext.scope():
        with pytest.raises(RuntimeError, match="abort"):
            async with service.begin_transaction() as session:
                await session.execute("INSERT INTO items VALUES ('new')")
                assert await service.exists("SELECT origin FROM items WHERE origin = 'new'") is True
                raise RuntimeError("abort")
        assert RoutingContext.get_entry("orders") is None
        assert await service.get_one("SELECT origin FROM items") == {"origin": "reader"}
        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO items VALUES ('committed')")
        assert await service.get_one("SELECT origin FROM items WHERE origin = 'committed'") == {"origin": "committed"}
    await writer.close_pool()
    await reader.close_pool()


def test_service_rejects_reader_execution_mode_mismatch() -> None:
    with pytest.raises(ImproperConfigurationError, match="execution mode"):
        SQLSpecSyncService(config=SqliteConfig(), read_config=cast("SqliteConfig", AiosqliteConfig()))


def test_session_service_commit_pins_only_its_writer_bind() -> None:
    config = SqliteConfig(bind_key="orders")
    with config.provide_session() as session:
        session.execute("CREATE TABLE items (id INTEGER)")
        session.commit()
        service = SQLSpecSyncService(session)
        with RoutingContext.auto("orders"):
            with service.begin_transaction() as transaction:
                transaction.execute("INSERT INTO items VALUES (1)")
                with service.begin_transaction() as nested:
                    nested.execute("INSERT INTO items VALUES (2)")
                nested_entry = RoutingContext.get_entry("orders")
                assert nested_entry is not None
                assert nested_entry.pinned_by_write is False
            entry = RoutingContext.get_entry("orders")
            assert entry is not None
            assert entry.pinned_by_write is True
            assert RoutingContext.get_entry("analytics") is None
            with service.provide_read_session() as selected:
                assert selected is session
    config.close_pool()


async def test_async_session_service_commit_pins_only_its_writer_bind() -> None:
    config = AiosqliteConfig(bind_key="orders")
    async with config.provide_session() as session:
        await session.execute("CREATE TABLE items (id INTEGER)")
        await session.commit()
        service = SQLSpecAsyncService(session)
        async with RoutingContext.auto("orders"):
            async with service.begin_transaction() as transaction:
                await transaction.execute("INSERT INTO items VALUES (1)")
            entry = RoutingContext.get_entry("orders")
            assert entry is not None
            assert entry.pinned_by_write is True
            assert RoutingContext.get_entry("analytics") is None
            async with service.provide_read_session() as selected:
                assert selected is session
    await config.close_pool()
