from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlspec.extensions.loader.protocols import AsyncDriverAdapterProtocol, StatementType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from aiosqlite import Connection
__all__ = ("AiosqliteAdapter",)


class AiosqliteAdapter(AsyncDriverAdapterProtocol):
    is_async = True

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Pass through function because the ``aiosqlite`` driver can already handle the
        ``:var_name`` format used by aiosql and doesn't need any additional processing.

        Args:
        op_type (SQLOperationType): The type of SQL operation performed by the query.
        sql (str): The sql as written before processing.

        Returns:
        - str: Original SQL text unchanged.
        """
        return sql

    async def select(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        async with connection.execute(sql, parameters) as cur:
            results = await cur.fetchall()
            if record_class is not None:
                column_names = [c[0] for c in cur.description]
                results = [record_class(**dict(zip(column_names, row, strict=False))) for row in results]  # pyright: ignore[reportCallIssue,reportReturnType]
        return results

    async def select_one(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        async with connection.execute(sql, parameters) as cur:
            result = await cur.fetchone()
            if result is not None and record_class is not None:
                column_names = [c[0] for c in cur.description]
                result = record_class(**dict(zip(column_names, result, strict=False)))  # pyright: ignore[reportCallIssue,reportReturnType]
        return result

    async def select_scalar(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        async with connection.execute(sql, parameters) as cur:
            result = await cur.fetchone()
        return result[0] if result else None

    async def with_cursor(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> AsyncGenerator[Any, None]:
        async with connection.execute(sql, parameters) as cur:
            yield cur

    async def insert_update_delete(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        async with connection.execute(sql, parameters) as cur:
            return cur.rowcount

    async def insert_update_delete_returning(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any:
        async with connection.execute(sql, parameters) as cur:
            return cur.lastrowid

    async def insert_update_delete_many(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        cur = await connection.executemany(sql, parameters)
        await cur.close()
        return cur.rowcount

    async def insert_update_delete_many_returning(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        cur = await connection.executemany(sql, parameters)
        res = await cur.fetchall()
        await cur.close()
        if record_class is not None:
            column_names = [c[0] for c in cur.description]
            res = [record_class(**dict(zip(column_names, row, strict=False))) for row in res]  # pyright: ignore[reportCallIssue,reportReturnType]
        return res

    async def execute_script(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> str:
        await connection.executescript(sql)
        return "DONE"
