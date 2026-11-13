"""Native historical CockroachDB reads and explicit primary consistency."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.exceptions import ReadOnlyTransactionError
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.text import quote_identifier

pytestmark = pytest.mark.xdist_group("cockroach")


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg"])
async def test_async_follower_snapshots_strong_pins_and_write_refusal(
    adapter: str, cockroach_asyncpg_config: Any, cockroach_psycopg_async_config: Any
) -> None:
    base = cockroach_asyncpg_config if adapter == "asyncpg" else cockroach_psycopg_async_config
    config = base.derive_pool()
    table = quote_identifier("follower_routing_" + uuid4().hex)
    try:
        async with config.provide_session() as session:
            await session.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY)")
            await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 1})
            await session.commit()
        await asyncio.sleep(5)
        async with config.provide_read_session() as session:
            await session.begin()
            assert await session.select_value(f"SELECT count(*) FROM {table}") == 1
            assert await session.select_value(f"SELECT max(id) FROM {table}") == 1
            with pytest.raises(ReadOnlyTransactionError):
                await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 2})
            await session.rollback()
        with RoutingContext.primary():
            async with config.provide_read_session(staleness="'-10s'") as session:
                assert await session.select_value(f"SELECT count(*) FROM {table}") == 1
        async with RoutingContext.scope():
            RoutingContext.mark_write()
            async with config.provide_read_session(staleness="'-10s'") as session:
                assert await session.select_value(f"SELECT count(*) FROM {table}") == 1
        async with config.provide_session() as session:
            await session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 2})
            await session.commit()
        await asyncio.sleep(0.15)
        async with config.provide_read_session(staleness="'-100ms'") as session:
            await session.begin()
            assert await session.select_value(f"SELECT count(*) FROM {table}") == 2
            assert await session.select_value(f"SELECT max(id) FROM {table}") == 2
            await session.rollback()
    finally:
        async with config.provide_session() as session:
            await session.execute(f"DROP TABLE IF EXISTS {table}")
            await session.commit()
        await config.close_pool()


def test_sync_follower_snapshot_and_strong_primary_override(cockroach_psycopg_sync_config: Any) -> None:
    config = cockroach_psycopg_sync_config.derive_pool()
    table = quote_identifier("sync_follower_" + uuid4().hex)
    try:
        with config.provide_session() as session:
            session.execute(f"CREATE TABLE {table} (id integer)")
            session.execute(f"INSERT INTO {table} (id) VALUES (:id)", {"id": 1})
            session.commit()
        with config.provide_read_session() as session:
            session.begin()
            assert session.select_value("SELECT 1") == 1
            assert session.select_value("SELECT 2") == 2
            with pytest.raises(ReadOnlyTransactionError):
                session.execute("CREATE TABLE forbidden_cockroach_readonly (id integer)")
            session.rollback()
        with RoutingContext.primary(), config.provide_read_session(staleness="'-10s'") as session:
            assert session.select_value(f"SELECT count(*) FROM {table}") == 1
    finally:
        with config.provide_session() as session:
            session.execute(f"DROP TABLE IF EXISTS {table}")
            session.commit()
        config.close_pool()
