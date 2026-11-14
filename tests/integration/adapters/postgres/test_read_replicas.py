"""Native PostgreSQL read-pool, causal, facade, service, and ADK acceptance."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from google.adk.events import Event

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
from sqlspec.config import ReadReplicaGroup
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.core.routing import ReplicaSpec
from sqlspec.exceptions import ReadOnlyTransactionError
from sqlspec.extensions.adk.service import SQLSpecSessionService
from sqlspec.service import SQLSpecAsyncService
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.text import quote_identifier

pytestmark = pytest.mark.xdist_group("postgres")


def _reader(base: Any, adapter: str, name: str) -> Any:
    settings: dict[str, Any] = (
        {"server_settings": {"default_transaction_read_only": "on", "application_name": name}}
        if adapter == "asyncpg"
        else {"options": "-c default_transaction_read_only=on -c application_name=" + name}
    )
    if adapter == "psqlpy":
        settings.update(target_session_attrs="read-only", load_balance_hosts="random")
    return base.derive_pool(connection_config_overrides=settings, is_replica=True)


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg", "psqlpy"])
async def test_async_native_replica_balance_readonly_and_causal_helpers(
    adapter: str, asyncpg_config: Any, psycopg_async_config: Any, psqlpy_config: Any
) -> None:
    base = {"asyncpg": asyncpg_config, "psycopg": psycopg_async_config, "psqlpy": psqlpy_config}[adapter]
    replicas = [_reader(base, adapter, "reader1"), _reader(base, adapter, "reader2")]
    group: ReadReplicaGroup[Any] = ReadReplicaGroup([ReplicaSpec(replicas[0], weight=2), ReplicaSpec(replicas[1])])
    config = type(base)(connection_config=dict(base.connection_config), replicas=group)
    table = quote_identifier("routing_" + uuid4().hex)
    async with base.provide_session() as session:
        await session.execute(f"CREATE TABLE {table} (id integer)")
        await session.commit()
    try:
        selected = []
        for _ in range(6):
            async with config.provide_read_session() as session:
                assert await session.select_value("SHOW transaction_read_only") == "on"
                selected.append(await session.select_value("SHOW application_name"))
        assert selected.count("reader1") == 4
        assert selected.count("reader2") == 2
        async with config.provide_read_session() as session:
            with pytest.raises(ReadOnlyTransactionError):
                await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 1})
        if adapter != "psqlpy":
            async with base.provide_session() as session:
                lsn = await session.current_wal_lsn()
            async with config.provide_read_session() as session:
                assert await session.has_replayed_wal_lsn(lsn)
    finally:
        await config.close_pool()
        async with base.provide_session() as session:
            await session.execute(f"DROP TABLE {table}")
            await session.commit()


def test_sync_native_replica_readonly_and_wal_helpers(psycopg_sync_config: Any) -> None:
    config = psycopg_sync_config.derive_pool()
    reader = _reader(config, "psycopg", "sync_reader")
    primary = type(config)(connection_config=dict(config.connection_config), replicas=[reader])
    try:
        with config.provide_session() as session:
            lsn = session.current_wal_lsn()
        with primary.provide_read_session() as session:
            assert session.select_value("SHOW transaction_read_only") == "on"
            assert session.has_replayed_wal_lsn(lsn)
            with pytest.raises(ReadOnlyTransactionError):
                session.execute("CREATE TABLE forbidden_readonly_write (id integer)")
    finally:
        primary.close_pool()
        config.close_pool()


async def test_native_service_and_routing_facade_pin_transactions_and_writes(
    asyncpg_connection_config: dict[str, Any],
) -> None:
    base = AsyncpgConfig(connection_config=dict(asyncpg_connection_config))
    reader = _reader(base, "asyncpg", "service_reader")
    primary = AsyncpgConfig(connection_config=dict(asyncpg_connection_config), replicas=[reader])
    table = quote_identifier("service_routing_" + uuid4().hex)
    try:
        async with primary.provide_session() as session:
            await session.execute(f"CREATE TABLE {table} (id integer)")
            await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 1})
        service = SQLSpecAsyncService(config=primary)
        assert (await service.get_one("SELECT current_setting('transaction_read_only') AS mode"))["mode"] == "on"
        assert await service.exists("SELECT 1 WHERE current_setting('transaction_read_only') = 'on'")
        page = await service.paginate(f"SELECT id FROM {table}", LimitOffsetFilter(limit=10, offset=0))
        assert page.total == 1
        with RoutingContext.scope():
            async with service.begin_transaction():
                assert (await service.get_one("SELECT current_setting('transaction_read_only') AS mode"))[
                    "mode"
                ] == "off"
        with RoutingContext.scope():
            async with primary.provide_routing_session() as session:
                assert await session.select_value("SELECT current_setting('transaction_read_only')") == "on"
                await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 2})
                assert session.pinned_to_primary
                assert await session.select_value("SELECT current_setting('transaction_read_only')") == "off"
        async with primary.provide_session() as session:
            assert await session.select_value(f"SELECT count(*) FROM {table}") == 2
    finally:
        async with primary.provide_session() as session:
            await session.execute(f"DROP TABLE IF EXISTS {table}")
        await primary.close_pool()
        await base.close_pool()


async def test_native_adk_offloads_list_reads_and_keeps_append_event_on_primary(
    asyncpg_connection_config: dict[str, Any],
) -> None:
    queries: dict[str, list[str]] = {"primary": [], "replica": []}

    async def primary_connection(connection: Any) -> None:
        connection.add_query_logger(lambda record: queries["primary"].append(record.query))

    async def replica_connection(connection: Any) -> None:
        connection.add_query_logger(lambda record: queries["replica"].append(record.query))

    suffix = uuid4().hex
    tables = {
        key: "routing_" + key + "_" + suffix
        for key in ("session_table", "events_table", "app_state_table", "user_state_table", "metadata_table")
    }
    primary = AsyncpgConfig(
        connection_config=dict(asyncpg_connection_config),
        driver_features={"on_connection_create": primary_connection},
        extension_config={"adk": tables},
    )
    replica = primary.derive_pool(
        connection_config_overrides={"server_settings": {"default_transaction_read_only": "on"}},
        driver_features_overrides={"on_connection_create": replica_connection},
        is_replica=True,
    )
    store = AsyncpgADKStore(primary, read_config=replica)
    service = SQLSpecSessionService(store)
    try:
        await store.create_tables()
        session = await service.create_session(app_name="routing", user_id="user", session_id=suffix)
        await asyncio.sleep(0)
        queries["primary"].clear()
        sessions = await store.list_sessions("routing", "user")
        await asyncio.sleep(0)
        assert [item["id"] for item in sessions] == [suffix]
        assert any(tables["session_table"] in query for query in queries["replica"])
        assert queries["primary"] == []
        queries["replica"].clear()
        await service.append_event(session, Event(author="user", invocation_id="routing-append"))
        await asyncio.sleep(0)
        assert any(tables["events_table"] in query and "INSERT" in query for query in queries["primary"])
        assert queries["replica"] == []
    finally:
        async with primary.provide_session() as driver:
            for table in tables.values():
                await driver.execute("DROP TABLE IF EXISTS " + quote_identifier(table) + " CASCADE")
        await primary.close_pool()
        await replica.close_pool()


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg", "psqlpy"])
async def test_derived_host_retains_dsn_connection_identity(adapter: str, postgres_service: Any) -> None:
    from sqlspec.adapters.psqlpy import PsqlpyConfig
    from sqlspec.adapters.psycopg import PsycopgAsyncConfig

    config_type = {"asyncpg": AsyncpgConfig, "psycopg": PsycopgAsyncConfig, "psqlpy": PsqlpyConfig}[adapter]
    service = postgres_service
    dsn = (
        f"postgresql://{service.user}:{service.password}@writer.invalid:{service.port}/{service.database}"
        "?sslmode=disable&application_name=derived_dsn_identity"
    )
    primary = config_type(connection_config={"dsn": dsn})
    reader = primary.derive_pool(host=service.host, port=service.port)
    try:
        async with reader.provide_session() as session:
            assert await session.select_value("SELECT current_user") == service.user
            assert await session.select_value("SELECT current_database()") == service.database
            assert await session.select_value("SHOW application_name") == "derived_dsn_identity"
    finally:
        await reader.close_pool()
