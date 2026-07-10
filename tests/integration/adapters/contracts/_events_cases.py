"""Case records for shared event-channel queue-backend contract tests."""

from dataclasses import dataclass
from typing import Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from tests.integration.adapters.contracts._cases import (
    ARROW_ODBC_MARK,
    DUCKDB_XDIST_MARK,
    MSSQL_MARK,
    MSSQL_XDIST_MARK,
    MYSQL_XDIST_MARK,
    ORACLE_XDIST_MARK,
    POSTGRES_XDIST_MARK,
    SQLITE_XDIST_MARK,
)


@dataclass(frozen=True)
class EventsCase:
    """Config-factory and capability metadata for event-channel contract tests."""

    id: str
    factory_fixture: str
    adapter: str
    mode: Literal["sync", "async"]
    marks: tuple[Mark | MarkDecorator, ...] = ()
    force_poll_queue: bool = False


@dataclass(frozen=True)
class EventsCaseContext:
    """Resolved config factory paired with its case metadata."""

    case: EventsCase
    make_config: object


@dataclass(frozen=True)
class ListenNotifyCase:
    """Config-factory metadata for native PostgreSQL LISTEN/NOTIFY contracts."""

    id: str
    factory_fixture: str
    adapter: str
    mode: Literal["sync", "async"]
    marks: tuple[Mark | MarkDecorator, ...] = ()


@dataclass(frozen=True)
class ListenNotifyCaseContext:
    """Resolved native LISTEN/NOTIFY config factory paired with case metadata."""

    case: ListenNotifyCase
    make_config: object


SYNC_EVENTS_CASES = (
    EventsCase("sqlite-sync", "events_config_sqlite", "sqlite", "sync", marks=(SQLITE_XDIST_MARK,)),
    EventsCase("duckdb-sync", "events_config_duckdb", "duckdb", "sync", marks=(DUCKDB_XDIST_MARK,)),
    EventsCase(
        "arrow-odbc-sync",
        "events_config_arrow_odbc_mssql",
        "arrow_odbc",
        "sync",
        marks=(MSSQL_MARK, MSSQL_XDIST_MARK, ARROW_ODBC_MARK),
    ),
    EventsCase("pymysql-sync", "events_config_pymysql", "pymysql", "sync", marks=(MYSQL_XDIST_MARK,)),
    EventsCase(
        "psycopg-sync",
        "events_config_psycopg_sync",
        "psycopg",
        "sync",
        marks=(POSTGRES_XDIST_MARK,),
        force_poll_queue=True,
    ),
    EventsCase(
        "oracledb-sync",
        "events_config_oracle_sync",
        "oracledb",
        "sync",
        marks=(ORACLE_XDIST_MARK,),
        force_poll_queue=True,
    ),
)

ASYNC_EVENTS_CASES = (
    EventsCase(
        "aiosqlite-async", "events_config_aiosqlite", "aiosqlite", "async", marks=(SQLITE_XDIST_MARK, pytest.mark.anyio)
    ),
    EventsCase(
        "asyncmy-async", "events_config_asyncmy", "asyncmy", "async", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)
    ),
    EventsCase(
        "aiomysql-async", "events_config_aiomysql", "aiomysql", "async", marks=(MYSQL_XDIST_MARK, pytest.mark.anyio)
    ),
    EventsCase(
        "mysqlconnector-async",
        "events_config_mysqlconnector_async",
        "mysqlconnector",
        "async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
    ),
    EventsCase(
        "psqlpy-async",
        "events_config_psqlpy",
        "psqlpy",
        "async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        force_poll_queue=True,
    ),
    EventsCase(
        "psycopg-async",
        "events_config_psycopg_async",
        "psycopg",
        "async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        force_poll_queue=True,
    ),
    EventsCase(
        "oracledb-async",
        "events_config_oracle_async",
        "oracledb",
        "async",
        marks=(ORACLE_XDIST_MARK, pytest.mark.anyio),
        force_poll_queue=True,
    ),
)

ACTIVE_EVENTS_CASES = SYNC_EVENTS_CASES + ASYNC_EVENTS_CASES
SYNC_EVENTS_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_EVENTS_CASES)
ASYNC_EVENTS_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_EVENTS_CASES)

SYNC_LISTEN_NOTIFY_CASES = (
    ListenNotifyCase(
        "psycopg-sync", "listen_notify_config_psycopg_sync", "psycopg", "sync", marks=(POSTGRES_XDIST_MARK,)
    ),
)

ASYNC_LISTEN_NOTIFY_CASES = (
    ListenNotifyCase(
        "asyncpg-async",
        "listen_notify_config_asyncpg",
        "asyncpg",
        "async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
    ListenNotifyCase(
        "psqlpy-async", "listen_notify_config_psqlpy", "psqlpy", "async", marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio)
    ),
    ListenNotifyCase(
        "psycopg-async",
        "listen_notify_config_psycopg_async",
        "psycopg",
        "async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
    ),
)

SYNC_LISTEN_NOTIFY_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_LISTEN_NOTIFY_CASES)
ASYNC_LISTEN_NOTIFY_PARAMS = tuple(
    pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_LISTEN_NOTIFY_CASES
)
