from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast

from sqlspec.sql.patterns import VAR_REF
from sqlspec.types.protocols import AsyncDriverAdapterProtocol, StatementType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from asyncpg import Connection, Pool
    from asyncpg.cursor import Cursor, CursorFactory

__all__ = ("AsyncpgAdapter",)


class ManagedConnection:
    """Context manager for handling connection acquisition from pools or direct connections."""

    def __init__(self, client: Connection | Pool) -> None:
        self.client = client
        self._managed_conn: Connection | None = None

    async def __aenter__(self) -> Connection:
        if "acquire" in dir(self.client):
            self._managed_conn = await self.client.acquire()  # pyright: ignore[reportAttributeAccessIssue]
            return cast("Connection", self._managed_conn)
        self._managed_conn = None
        return cast("Connection", self.client)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._managed_conn is not None:
            await self.client.release(self._managed_conn)  # pyright: ignore[reportAttributeAccessIssue]


class AsyncpgAdapter(AsyncDriverAdapterProtocol):
    """An AsyncPG SQLSpec Adapter suitable for PostgreSQL-style parameter binding."""

    is_async: bool = True

    def __init__(self) -> None:
        self.var_sorted = defaultdict(list)

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Preprocess SQL query to convert named parameters to positional parameters.

        Args:
            op_type: The type of SQL operation being performed
            sql: The SQL query string

        Returns:
            The processed SQL query string with positional parameters
        """
        adj = 0
        query_name = str(op_type)

        for match in VAR_REF.finditer(sql):
            gd = match.groupdict()
            if gd["dquote"] is not None or gd["squote"] is not None:
                continue

            var_name = gd["var_name"]
            if var_name in self.var_sorted[query_name]:
                replacement = f"${self.var_sorted[query_name].index(var_name) + 1}"
            else:
                replacement = f"${len(self.var_sorted[query_name]) + 1}"
                self.var_sorted[query_name].append(var_name)

            start = match.start() + len(gd["lead"]) + adj
            end = match.end() + adj
            sql = sql[:start] + replacement + sql[end:]
            adj += len(replacement) - len(var_name) - 1

        return sql

    def _order_parameters(self, op_type: StatementType, parameters: list | dict) -> list:
        """Order parameters based on their appearance in the SQL query.

        Args:
            op_type: The type of SQL operation
            parameters: Query parameters

        Returns:
            Ordered list of parameters
        """
        query_name = str(op_type)
        if isinstance(parameters, dict):
            return [parameters[key] for key in self.var_sorted[query_name]]
        if isinstance(parameters, (list, tuple)):
            return list(parameters)
        msg = f"Parameters expected to be dict, list or tuple, received {type(parameters)}"
        raise ValueError(msg)

    async def select(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT."""
        async with ManagedConnection(connection) as conn:
            results = await conn.fetch(sql, *parameters)
            if record_class is not None:
                return [record_class(**dict(rec)) for rec in results]
            return results

    async def select_one(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        """Handle a single-row-returning SELECT."""
        async with ManagedConnection(connection) as conn:
            result = await conn.fetchrow(sql, *parameters)
            if result is not None and record_class is not None:
                return record_class(**dict(result))
            return result

    async def select_scalar(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        """Handle a scalar-returning SELECT."""
        async with ManagedConnection(connection) as conn:
            return await conn.fetchval(sql, *parameters)

    async def with_cursor(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
    ) -> AsyncGenerator[Cursor | CursorFactory, None]:
        """Execute a query and yield the cursor."""
        async with ManagedConnection(connection) as conn:
            stmt = await conn.prepare(sql)
            async with conn.transaction():
                yield stmt.cursor(*parameters)

    async def insert_update_delete(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle an INSERT, UPDATE, or DELETE."""
        async with ManagedConnection(connection) as conn:
            result = await conn.execute(sql, *parameters)
            if isinstance(result, str):
                return int(result.split()[-1])
            return 0

    async def insert_update_delete_returning(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Any:
        """Handle an INSERT, UPDATE, or DELETE with RETURNING clause."""
        return await self.select_one(connection, sql, parameters, record_class)

    async def execute_script(
        self,
        connection: Connection | Pool,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> Any:
        """Execute a SQL script."""
        async with ManagedConnection(connection) as conn:
            if parameters:
                result = await conn.fetch(sql, *parameters)
            else:
                result = await conn.fetch(sql)
            if record_class is not None:
                return [record_class(**dict(rec)) for rec in result]
            return result
