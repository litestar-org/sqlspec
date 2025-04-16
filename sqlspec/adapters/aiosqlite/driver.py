from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import asynccontextmanager
from typing import Any, Optional, Union, cast

from aiosqlite import Connection, Cursor

from sqlspec.base import AsyncDriverAdapterProtocol, T
from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AiosqliteDriver",)


class AiosqliteDriver(AsyncDriverAdapterProtocol[Connection]):
    """SQLite Async Driver Adapter."""

    connection: Connection
    results_as_dict: bool = True

    def __init__(self, connection: Connection, results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    async def _cursor(connection: Connection, *args: Any, **kwargs: Any) -> Cursor:
        return await connection.cursor(*args, **kwargs)

    @asynccontextmanager
    async def _with_cursor(self, connection: Connection) -> AsyncGenerator[Cursor, None]:
        cursor = await self._cursor(connection)
        try:
            yield cursor
        finally:
            await cursor.close()

    async def select(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch data from the database.

        Returns:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async def _fetch_results() -> AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]:
            async with self._with_cursor(connection) as cursor:
                await cursor.execute(sql, parameters)

                # Get column names once
                column_names = [c[0] for c in cursor.description or []]
                results = await cursor.fetchall()

                for row in results:
                    if schema_type is not None:
                        yield cast("ModelDTOT", schema_type(**dict(zip(column_names, row))))
                    elif self.results_as_dict:
                        yield dict(zip(column_names, row))
                    else:
                        yield tuple(row)

        return _fetch_results()

    async def select_one(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None and self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result))
            if schema_type is not None:
                column_names = [c[0] for c in cursor.description or []]
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportCallIssue]
            return tuple(result)

    async def select_value(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            if returning is False:
                return cursor.rowcount if hasattr(cursor, "rowcount") else -1
            result = await cursor.fetchall()
            if len(list(result)) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, iter(result))))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, iter(result)))
            return tuple(iter(result))

    async def execute_script(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            if returning is False:
                return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
            result = await cursor.fetchall()
            if len(list(result)) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, iter(result))))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, iter(result)))
            return tuple(iter(result))
