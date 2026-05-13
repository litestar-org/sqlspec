# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
"""Cross-adapter table-backed event queue contract tests."""

import asyncio
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from google.api_core import exceptions as api_exceptions

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb.events import OracleAsyncEventQueueStore, OracleSyncEventQueueStore
from sqlspec.adapters.spanner.events import SpannerSyncEventQueueStore
from tests.integration.adapters._events_helpers import setup_async_event_channel, setup_sync_event_channel
from tests.integration.adapters.contracts._helpers import (
    ASYNC_ADAPTERS,
    SPANNER_LOCAL_SKIP,
    close_config,
    make_config,
    maybe_await,
    provide_driver,
)


@dataclass(frozen=True)
class EventQueueCase:
    """Per-adapter table queue setup."""

    adapter: str
    queue_table: str
    is_async: bool = False
    direct_store: bool = False


EVENT_QUEUE_CASES = [
    pytest.param(
        EventQueueCase("sqlite", "eq_sqlite"),
        marks=[pytest.mark.sqlite, pytest.mark.xdist_group("sqlite")],
        id="sqlite",
    ),
    pytest.param(
        EventQueueCase("aiosqlite", "eq_aiosqlite", is_async=True),
        marks=[pytest.mark.sqlite, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")],
        id="aiosqlite",
    ),
    pytest.param(
        EventQueueCase("aiomysql", "eq_aiomysql", is_async=True),
        marks=[pytest.mark.mysql, pytest.mark.aiomysql, pytest.mark.xdist_group("mysql")],
        id="aiomysql",
    ),
    pytest.param(
        EventQueueCase("asyncmy", "eq_asyncmy", is_async=True),
        marks=[pytest.mark.mysql, pytest.mark.asyncmy, pytest.mark.xdist_group("mysql")],
        id="asyncmy",
    ),
    pytest.param(
        EventQueueCase("mysqlconnector-sync", "eq_mysqlconnector_sync"),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-sync",
    ),
    pytest.param(
        EventQueueCase("mysqlconnector-async", "eq_mysqlconnector_async", is_async=True),
        marks=[pytest.mark.mysql, pytest.mark.mysql_connector, pytest.mark.xdist_group("mysql")],
        id="mysqlconnector-async",
    ),
    pytest.param(
        EventQueueCase("pymysql", "eq_pymysql"),
        marks=[pytest.mark.mysql, pytest.mark.pymysql, pytest.mark.xdist_group("mysql")],
        id="pymysql",
    ),
    pytest.param(
        EventQueueCase("duckdb", "eq_duckdb"),
        marks=[pytest.mark.duckdb, pytest.mark.xdist_group("duckdb")],
        id="duckdb",
    ),
    pytest.param(
        EventQueueCase("psqlpy", "eq_psqlpy", is_async=True),
        marks=[pytest.mark.postgres, pytest.mark.psqlpy, pytest.mark.xdist_group("postgres")],
        id="psqlpy",
    ),
    pytest.param(
        EventQueueCase("psycopg-sync", "eq_psycopg_sync"),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-sync",
    ),
    pytest.param(
        EventQueueCase("psycopg-async", "eq_psycopg_async", is_async=True),
        marks=[pytest.mark.postgres, pytest.mark.psycopg, pytest.mark.xdist_group("postgres")],
        id="psycopg-async",
    ),
    pytest.param(
        EventQueueCase("oracle-sync", "EQ_ORACLE_SYNC", direct_store=True),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-sync",
    ),
    pytest.param(
        EventQueueCase("oracle-async", "EQ_ORACLE_ASYNC", is_async=True, direct_store=True),
        marks=[pytest.mark.oracle, pytest.mark.oracledb, pytest.mark.xdist_group("oracle")],
        id="oracle-async",
    ),
    pytest.param(
        EventQueueCase("spanner", "eq_spanner", direct_store=True),
        marks=[pytest.mark.spanner, pytest.mark.google_spanner, pytest.mark.xdist_group("spanner"), SPANNER_LOCAL_SKIP],
        id="spanner",
    ),
]


async def _setup_channel(case: EventQueueCase, config: Any) -> tuple[SQLSpec, Any, Any | None]:
    if case.direct_store:
        spec = SQLSpec()
        spec.add_config(config)
        if case.adapter == "spanner":
            spanner_store = SpannerSyncEventQueueStore(config)
            with contextlib.suppress(api_exceptions.NotFound):
                spanner_store.drop_table()
            spanner_store.create_table()
            return spec, spec.event_channel(config), spanner_store
        if case.adapter == "oracle-async":
            oracle_async_store = OracleAsyncEventQueueStore(config)
            with contextlib.suppress(Exception):
                await oracle_async_store.drop_table()
            await oracle_async_store.create_table()
            return spec, spec.event_channel(config), oracle_async_store
        if case.adapter == "oracle-sync":
            oracle_sync_store = OracleSyncEventQueueStore(config)
            with contextlib.suppress(Exception):
                oracle_sync_store.drop_table()
            oracle_sync_store.create_table()
            return spec, spec.event_channel(config), oracle_sync_store
        msg = f"Unhandled direct event queue store adapter: {case.adapter}"
        raise ValueError(msg)

    if case.adapter in ASYNC_ADAPTERS:
        spec, channel = await setup_async_event_channel(config)
    else:
        spec, channel = setup_sync_event_channel(config)
    return spec, channel, None


async def _drop_direct_store(store: Any) -> None:
    if store is None:
        return
    with contextlib.suppress(Exception):
        result = store.drop_table()
        if asyncio.iscoroutine(result):
            await result


async def _publish_consume_ack(case: EventQueueCase, channel: Any) -> tuple[str, Any]:
    if case.is_async:
        event_id = await channel.publish("notifications", {"action": case.adapter})
        iterator = channel.iter_events("notifications", poll_interval=0.05)
        try:
            message = await asyncio.wait_for(iterator.__anext__(), timeout=10)
        finally:
            await iterator.aclose()
        await channel.ack(message.event_id)
        return event_id, message

    event_id = channel.publish("notifications", {"action": case.adapter})
    iterator = channel.iter_events("notifications", poll_interval=0.05)
    try:
        message = next(iterator)
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    channel.ack(message.event_id)
    return event_id, message


async def _queue_status(case: EventQueueCase, config: Any, event_id: str) -> str:
    sql = (
        f"SELECT status FROM {case.queue_table} WHERE event_id = @event_id"
        if case.adapter == "spanner"
        else f"SELECT status FROM {case.queue_table} WHERE event_id = :event_id"
    )
    async with provide_driver(case.adapter, config) as driver:
        row = await maybe_await(driver.select_one(sql, {"event_id": event_id}))
    return str(row["status"])


@pytest.mark.parametrize("case", EVENT_QUEUE_CASES)
async def test_table_backed_event_queue_publish_consume_ack(
    case: EventQueueCase, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    """Table-backed event queues publish, consume, and ack events across adapters."""
    migration_dir = tmp_path / f"{case.adapter.replace('-', '_')}_events"
    migration_dir.mkdir()
    config = make_config(
        case.adapter,
        request,
        tmp_path,
        migration_config={
            "script_location": str(migration_dir),
            "include_extensions": ["events"],
            "version_table_name": f"event_migrations_{case.adapter.replace('-', '_')}",
        },
        extension_config={"events": {"backend": "table_queue", "queue_table": case.queue_table}},
    )
    store = None
    try:
        _spec, channel, store = await _setup_channel(case, config)
        event_id, message = await _publish_consume_ack(case, channel)

        assert message.event_id == event_id
        assert message.payload["action"] == case.adapter
        assert message.channel == "notifications"
        assert await _queue_status(case, config, event_id) == "acked"
    finally:
        await _drop_direct_store(store)
        await close_config(config)
