"""CockroachDB AsyncPG adapter type definitions."""

from typing import TYPE_CHECKING

import asyncpg as cockroach_asyncpg_module
from asyncpg import InterfaceError as CockroachAsyncpgInterfaceError
from asyncpg import Pool
from asyncpg import PostgresError as CockroachAsyncpgPostgresError
from asyncpg import Record as CockroachAsyncpgRecord
from asyncpg import connect as cockroach_asyncpg_connect
from asyncpg import create_pool as cockroach_asyncpg_create_pool
from asyncpg.pool import PoolConnectionProxy

from sqlspec.driver._session import AsyncDriverSessionContext

if TYPE_CHECKING:
    from typing import TypeAlias

    from asyncpg import Connection, Record

    from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver

    CockroachAsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    CockroachAsyncpgPool: TypeAlias = Pool[Record]
else:
    CockroachAsyncpgConnection = PoolConnectionProxy
    CockroachAsyncpgPool = Pool

__all__ = (
    "CockroachAsyncpgConnection",
    "CockroachAsyncpgInterfaceError",
    "CockroachAsyncpgPool",
    "CockroachAsyncpgPostgresError",
    "CockroachAsyncpgRecord",
    "CockroachAsyncpgSessionContext",
    "cockroach_asyncpg_connect",
    "cockroach_asyncpg_create_pool",
    "cockroach_asyncpg_module",
)


class CockroachAsyncpgSessionContext(AsyncDriverSessionContext):
    """Async context manager for CockroachDB AsyncPG sessions."""

    __slots__ = ()

    if TYPE_CHECKING:

        async def __aenter__(self) -> "CockroachAsyncpgDriver": ...

    def _resolve_driver_class(self) -> "type[CockroachAsyncpgDriver]":
        from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver

        return CockroachAsyncpgDriver
