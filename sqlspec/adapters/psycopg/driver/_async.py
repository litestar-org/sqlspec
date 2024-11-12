from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlspec.adapters.psycopg.driver._base import BasePsycopgAdapter
from sqlspec.types.protocols import AsyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from psycopg import AsyncConnection, AsyncCursor
    from psycopg_pool import AsyncConnectionPool

__all__ = ("PsycopgAsyncAdapter",)


class ManagedConnection:
    """Context manager for handling connection acquisition from pools or direct connections."""

    def __init__(self, client: AsyncConnection | AsyncConnectionPool) -> None:
        self.client = client
        self._managed_conn: AsyncConnection | None = None

    async def __aenter__(self) -> AsyncConnection:
        if hasattr(self.client, "connection"):
            self._managed_conn = await self.client.connection()
            return self._managed_conn
        return self.client

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._managed_conn is not None:
            await self._managed_conn.close()


class PsycopgAsyncAdapter(BasePsycopgAdapter, AsyncDriverAdapterProtocol):
    """An asynchronous Psycopg SQLSpec Adapter suitable for PostgreSQL-style parameter binding."""

    is_async: bool = True

    async def _cursor(self, connection: AsyncConnection) -> AsyncCursor:
        """Get a cursor from a connection."""
        return connection.cursor()

    async def select(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT."""
        async with ManagedConnection(connection) as conn, conn.cursor() as cur:
            await cur.execute(sql, parameters)
            if record_class is None:
                async for row in cur:
                    yield row
            else:
                column_names = [desc.name for desc in cur.description]
                async for row in cur:
                    yield self._process_row(row, column_names, record_class)

    async def select_one(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        """Handle a single-row-returning SELECT."""
        async with ManagedConnection(connection) as conn, conn.cursor() as cur:
            await cur.execute(sql, parameters)
            row = await cur.fetchone()
            if row is None:
                return None
            if record_class is None:
                return row
            column_names = [desc.name for desc in cur.description]
            return self._process_row(row, column_names, record_class)

    async def select_scalar(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        """Handle a scalar-returning SELECT."""
        async with ManagedConnection(connection) as conn, conn.cursor() as cur:
            await cur.execute(sql, parameters)
            row = await cur.fetchone()
            return row[0] if row else None

    async def with_cursor(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
    ) -> AsyncGenerator[AsyncCursor, None]:
        """Execute a query and yield the cursor."""
        async with ManagedConnection(connection) as conn, conn.cursor() as cur:
            await cur.execute(sql, parameters)
            yield cur

    async def insert_update_delete(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle an INSERT, UPDATE, or DELETE."""
        async with ManagedConnection(connection) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, parameters)
                return cur.rowcount

    async def insert_update_delete_returning(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Any:
        """Handle an INSERT, UPDATE, or DELETE with RETURNING clause."""
        return await self.select_one(connection, sql, parameters, record_class)

    async def execute_script(
        self,
        connection: AsyncConnection | AsyncConnectionPool,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> Any:
        """Execute a SQL script."""
        async with ManagedConnection(connection) as conn:
            async with conn.cursor() as cur:
                if parameters:
                    await cur.execute(sql, parameters)
                else:
                    await cur.execute(sql)
                return cur.statusmessage
