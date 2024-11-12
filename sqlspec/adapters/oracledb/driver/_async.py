from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlspec.types.protocols import AsyncDriverAdapterProtocol, StatementType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from oracledb import AsyncConnection, AsyncCursor

__all__ = ("OracleAsyncAdapter",)


class OracleAsyncAdapter(AsyncDriverAdapterProtocol):
    """An asynchronous Oracle SQLSpec Adapter suitable for `named` parameter style and DB-API compliant connections."""

    is_async: bool = True

    def __init__(self, driver=None) -> None:
        self._driver = driver

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Preprocess SQL query."""
        return sql

    def _cursor(self, connection: AsyncConnection) -> AsyncCursor:
        """Get a cursor from a connection."""
        return connection.cursor()

    async def select(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT."""
        cur = self._cursor(connection)
        try:
            await cur.execute(sql, parameters)
            column_names = [desc[0] for desc in cur.description]
            cur.rowfactory = lambda *args: dict(zip(column_names, args))
            data = await cur.fetchall()
            if record_class is None:
                async for row in cur:
                    yield row
            else:
                column_names = [desc[0] for desc in cur.description]
                cur.rowfactory = lambda *args: dict(zip(column_names, args))
                async for row in cur:
                    yield record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})
        finally:
            cur.close()

    async def select_one(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        """Handle a single-row-returning SELECT."""
        cur = await self._cursor(connection)
        try:
            await cur.execute(sql, parameters)
            row = await cur.fetchone()
            if row is None or record_class is None:
                return row
            column_names = [desc[0] for desc in cur.description]
            return record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})
        finally:
            await cur.close()

    async def select_scalar(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        """Handle a scalar-returning SELECT."""
        cur = await self._cursor(connection)
        try:
            await cur.execute(sql, parameters)
            row = await cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()

    async def with_cursor(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
    ) -> AsyncGenerator[AsyncCursor, None]:
        """Execute a query and yield the cursor."""
        cur = self._cursor(connection)
        try:
            await cur.execute(sql, parameters)
            yield cur
        finally:
            cur.close()

    async def insert_update_delete(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle an INSERT, UPDATE, or DELETE."""
        cur = self._cursor(connection)
        try:
            await cur.execute(sql, parameters)
            return cur.rowcount
        finally:
            cur.close()

    async def insert_update_delete_returning(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Any:
        """Handle an INSERT, UPDATE, or DELETE with RETURNING clause."""
        return await self.select_one(connection, sql, parameters, record_class)

    async def execute_script(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> Any:
        """Execute a SQL script."""
        cur = await self._cursor(connection)
        try:
            if parameters:
                await cur.execute(sql, parameters)
            else:
                await cur.execute(sql)
            return await cur.fetchall()
        finally:
            await cur.close()
