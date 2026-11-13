"""File-backed WAL readers coexist with a primary write transaction."""

import asyncio
import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ReadOnlyTransactionError
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from sqlspec.utils.routing import RoutingContext

pytestmark = [pytest.mark.integration, pytest.mark.sqlite]


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@asynccontextmanager
async def _enter(context: Any, *, asynchronous: bool) -> AsyncGenerator[Any]:
    if asynchronous:
        async with context as value:
            yield value
    else:
        with context as value:
            yield value


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_wal_readers_keep_committed_view_during_primary_write(tmp_path: Path, asynchronous: bool) -> None:
    config_type = AiosqliteConfig if asynchronous else SqliteConfig
    config = config_type(connection_config={"database": str(tmp_path / "wal.db")}, replicas=True)
    try:
        async with _enter(config.provide_session(), asynchronous=asynchronous) as writer:
            await _resolve(writer.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY)"))
            await _resolve(writer.execute("INSERT INTO entries VALUES (1)"))
            await _resolve(writer.commit())
            await _resolve(writer.begin())
            await _resolve(writer.execute("INSERT INTO entries VALUES (2)"))

            async def count() -> int:
                async with _enter(config.provide_read_session(), asynchronous=asynchronous) as reader:
                    assert await _resolve(reader.select_value("PRAGMA query_only")) == 1
                    with pytest.raises(ReadOnlyTransactionError):
                        await _resolve(reader.execute("INSERT INTO entries VALUES (3)"))
                    return int(await _resolve(reader.select_value("SELECT COUNT(*) FROM entries")))

            assert tuple(await asyncio.gather(count(), count())) == (1, 1)
            await _resolve(writer.commit())
        service = (
            SQLSpecAsyncService(config=config)
            if isinstance(config, AiosqliteConfig)
            else SQLSpecSyncService(config=config)
        )
        async with _enter(service.provide_session(read_only=True), asynchronous=asynchronous) as reader:
            assert await _resolve(reader.select_value("SELECT COUNT(*) FROM entries")) == 2
        with RoutingContext.scope():
            async with _enter(config.provide_routing_session(), asynchronous=asynchronous) as routing:
                assert await _resolve(routing.select_value("SELECT COUNT(*) FROM entries")) == 2
                await _resolve(routing.execute("INSERT INTO entries VALUES (3)"))
                await _resolve(routing.commit())
                assert await _resolve(routing.select_value("SELECT COUNT(*) FROM entries")) == 3
    finally:
        await _resolve(config.close_pool())


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_wal_adk_reads_use_reader_pool_and_mutations_keep_primary(tmp_path: Path, asynchronous: bool) -> None:
    pytest.importorskip("google.adk")
    from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
    from sqlspec.adapters.sqlite.adk import SqliteADKStore

    config_type = AiosqliteConfig if asynchronous else SqliteConfig
    config = config_type(
        connection_config={"database": str(tmp_path / "adk.db")},
        replicas=True,
        extension_config={"adk": {"read_config_enabled": True}},
    )
    try:
        store = AiosqliteADKStore(config) if isinstance(config, AiosqliteConfig) else SqliteADKStore(config)
        await _resolve(store.create_tables())
        await _resolve(store.create_session("first", "app", "user", {"value": 1}))
        first = await _resolve(store.get_session("app", "user", "first"))
        assert first["state"] == {"value": 1}
        await _resolve(store.create_session("second", "app", "user", {"value": 2}))
        assert {row["id"] for row in await _resolve(store.list_sessions("app", "user"))} == {"first", "second"}
        await _resolve(store.delete_session("app", "user", "second"))
        assert [row["id"] for row in await _resolve(store.list_sessions("app", "user"))] == ["first"]
    finally:
        await _resolve(config.close_pool())
