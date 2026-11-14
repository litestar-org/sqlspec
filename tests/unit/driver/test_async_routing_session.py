"""Async routing, lazy streams, and cancellation through real aiosqlite."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core import StatementStack
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.driver._routing import AsyncRoutingSession
from sqlspec.driver._stream import AsyncRowStream
from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError, SQLSpecError
from sqlspec.utils.routing import RoutingContext, RoutingEntry
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


@pytest.fixture
async def async_routing_config(tmp_path: Path) -> AsyncIterator[AiosqliteConfig]:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(connection_config={"database": primary}, replicas=[{"database": replica}], bind_key="shop")
    try:
        yield config
    finally:
        await config.close_pool()


async def test_async_idle_read_write_pin_and_closed(async_routing_config: AiosqliteConfig) -> None:
    async with RoutingContext.scope(), async_routing_config.provide_routing_session() as session:
        assert session.primary_session is None
        assert session.replica_session is None
        await session.commit()
        assert await session.select_value("SELECT origin FROM notes WHERE id=:id", id=1) == "replica"
        leaked_replica = session.replica_session
        assert leaked_replica is not None
        await session.execute("INSERT INTO notes VALUES (:id,:origin)", {"id": 4, "origin": "new"})
        leaked_primary = session.primary_session
        assert leaked_primary is not None
        await session.commit()
        assert session.pinned_to_primary
        assert cast(RoutingEntry, RoutingContext.get_entry("shop")).pinned_by_write
        assert await session.fetch_value("SELECT origin FROM notes WHERE id=1") == "primary"
    assert session.primary_session is None
    assert session.replica_session is None
    assert leaked_replica.connection is _DETACHED_CONNECTION
    assert leaked_primary.connection is _DETACHED_CONNECTION
    with pytest.raises(SQLSpecError, match="closed"):
        await session.select("SELECT 1")


async def test_async_stream_is_lazy_typed_and_native(async_routing_config: AiosqliteConfig) -> None:
    @dataclass
    class Note:
        origin: str

    async with AsyncRoutingSession(async_routing_config) as session:
        stream = session.select_stream("SELECT origin FROM notes ORDER BY id", schema_type=Note, chunk_size=2)
        assert isinstance(stream, AsyncRowStream)
        assert session.primary_session is None
        assert session.replica_session is None
        async with stream:
            rows = [row async for row in stream]
        assert rows == [Note("replica")] * 3
        assert session.primary_session is None
        async with session.fetch_stream("SELECT id FROM notes WHERE id=1") as aliases:
            assert [row async for row in aliases] == [{"id": 1}]
        with pytest.raises(ValueError, match="chunk_size"):
            session.select_stream("SELECT 1", chunk_size=0)


async def test_async_replica_transaction_conflict_and_locking_stream(async_routing_config: AiosqliteConfig) -> None:
    async with AsyncRoutingSession(async_routing_config) as session:
        async with session.transaction():
            primary = session.primary_session
            with RoutingContext.replica(), pytest.raises(RoutingContextError):
                await session.select_value("SELECT 1")
            with RoutingContext.replica(), pytest.raises(RoutingContextError):
                async with session.select_stream("SELECT 1") as stream:
                    await anext(stream)
            assert session.primary_session is primary
            assert session.replica_session is None
        with RoutingContext.replica(), pytest.raises(RoutingContextError):
            async with session.select_stream("SELECT * FROM notes FOR UPDATE") as stream:
                await anext(stream)
        assert session.replica_session is None


async def test_async_replica_refuses_write_batch_and_transaction(async_routing_config: AiosqliteConfig) -> None:
    async with AsyncRoutingSession(async_routing_config) as session:
        with RoutingContext.replica():
            with pytest.raises(RoutingContextError):
                await session.execute_many("INSERT INTO notes VALUES (?,?)", [(4, "new")])
            with pytest.raises(RoutingContextError):
                await session.begin()
            with pytest.raises(RoutingContextError):
                await session.execute("DELETE FROM notes")
        assert session.primary_session is None
        assert session.replica_session is None


async def test_async_cancellation_rolls_back_primary_and_releases_checkouts(
    async_routing_config: AiosqliteConfig,
) -> None:
    with pytest.raises(asyncio.CancelledError):
        async with AsyncRoutingSession(async_routing_config) as session:
            await session.select_value("SELECT origin FROM notes WHERE id=1")
            await session.begin()
            await session.execute("INSERT INTO notes VALUES (4,'cancelled')")
            raise asyncio.CancelledError
    async with async_routing_config.provide_session() as primary:
        assert await primary.select_value("SELECT count(*) FROM notes") == 3


async def test_async_stack_registry_and_dictionary(async_routing_config: AiosqliteConfig) -> None:
    registry = SQLSpec()
    config = registry.add_config(async_routing_config)
    async with registry.provide_routing_session(config) as session:
        assert session.primary_session is None
        pure = StatementStack().push_execute("SELECT 1")
        assert len(await session.execute_stack(pure)) == 1
        assert session.primary_session is None
        tables = await session.data_dictionary.get_tables(session)
        assert "notes" in [table["table_name"] for table in tables]
        mixed = pure.push_execute("INSERT INTO notes VALUES (4,'new')")
        assert len(await session.execute_stack(mixed)) == 2
        await session.rollback()
    with pytest.raises(ImproperConfigurationError):
        AsyncRoutingSession(config, tenant="tenant", bypass_tenant=True)


@pytest.mark.parametrize(
    ("method", "args", "domain"),
    [
        ("get_schemas", (), "schemas"),
        ("get_objects", (), "objects"),
        ("get_table_details", ("notes",), "tables"),
        ("get_constraints", ("notes",), "constraints"),
        ("get_views", (), "views"),
        ("get_routines", (), "routines"),
        ("get_privileges", (), "privileges"),
        ("get_dependencies", (), "dependencies"),
        ("get_sequences", (), "sequences"),
        ("get_types", (), "types"),
    ],
)
async def test_async_dictionary_domains_use_primary(
    async_routing_config: AiosqliteConfig, method: str, args: tuple[str, ...], domain: str
) -> None:
    async with AsyncRoutingSession(async_routing_config) as session:
        result = await getattr(session.data_dictionary, method)(session, *args)
        assert result.domain == domain
        assert session.pinned_to_primary
        assert session.replica_session is None


async def test_async_dictionary_schema_ddl_profile_and_capabilities(async_routing_config: AiosqliteConfig) -> None:
    async with AsyncRoutingSession(async_routing_config) as session:
        dictionary = session.data_dictionary
        profile = await dictionary.get_metadata_capabilities(session, domains=["tables", "columns"])
        assert profile.dialect == "sqlite"
        system = await dictionary.get_system_metadata_capabilities(session, domains=["sessions"])
        assert [item.domain for item in system] == ["sessions"]
        metadata = await dictionary.get_system_metadata(session, "sessions")
        assert metadata.request.domain == "sessions"
        columns = await dictionary.get_columns(session, "notes")
        assert [column["column_name"] for column in columns] == ["id", "origin"]
        assert await dictionary.get_indexes(session, "notes") == []
        assert await dictionary.get_foreign_keys(session, "notes") == []
        version = await dictionary.get_version(session)
        assert str(version).startswith("3.")
        assert await dictionary.get_feature_flag(session, "nonexistent_feature") is False
        assert await dictionary.get_optimal_type(session, "text") == "TEXT"
        ddl = await dictionary.get_ddl(session, "notes")
        assert "CREATE TABLE" in str(ddl.ddl).upper()
        object_ddl = await dictionary.get_object_ddl(session, "notes")
        assert object_ddl.ddl == ddl.ddl
        schema = await dictionary.get_schema_ddl(session, exclude_tables=())
        assert schema.domain == "ddl"
        profiles = await dictionary.get_table_profile(session, "notes", sample_size=2)
        assert {profile.column_name for profile in profiles} == {"id", "origin"}
        assert all(profile.row_count == 3 for profile in profiles)
        inferred = await dictionary.infer_table_ddl_from_data(session, "notes", target_table="notes_copy")
        assert "notes_copy" in str(inferred.ddl)


async def test_async_single_pool_aliases_and_savepoints(tmp_path: Path) -> None:
    primary, _ = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(connection_config={"database": primary})

    @dataclass
    class Note:
        origin: str

    try:
        async with AsyncRoutingSession(config) as session:
            assert session.is_async is True
            assert await session.select("SELECT origin FROM notes WHERE id=1", schema_type=Note) == [Note("primary")]
            assert await session.select_one("SELECT origin FROM notes WHERE id=1", schema_type=Note) == Note("primary")
            assert await session.select_one_or_none("SELECT id FROM notes WHERE id=99") is None
            assert await session.fetch_value_or_none("SELECT id FROM notes WHERE id=99") is None
            assert (await session.fetch_with_total("SELECT id FROM notes"))[1] == 3
            assert session.replica_session is None
            await session.begin()
            await session.create_savepoint("before")
            await session.execute_many("INSERT INTO notes VALUES (?,?)", [(4, "new")])
            await session.rollback_to_savepoint("before")
            await session.release_savepoint("before")
            await session.execute_script("INSERT INTO notes VALUES (5,'script');")
            await session.rollback()
            assert await session.select_value("SELECT count(*) FROM notes") == 3
            await session.aclose()
    finally:
        await config.close_pool()
