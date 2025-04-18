# type: ignore
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import AsyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from asyncmy import Connection
    from asyncmy.cursors import Cursor

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AsyncmyDriver",)


class AsyncmyDriver(AsyncDriverAdapterProtocol["Connection"]):
    """Asyncmy MySQL/MariaDB Driver Adapter."""

    connection: "Connection"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    @staticmethod
    async def _cursor(connection: "Connection") -> "Cursor":
        return await connection.cursor()

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "Connection") -> AsyncGenerator["Cursor", None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            results = await cursor.fetchall()
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return [dict(zip(column_names, row)) for row in results]
            return [schema_type(**dict(zip(column_names, row))) for row in results]

    async def select_one(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)

            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()

            if result is None:
                return None

            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return cursor.rowcount

    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        column_names: list[str] = []

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))
            return dict(zip(column_names, result))

    async def execute_script(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return "DONE"

    async def execute_script_returning(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        connection: Optional["Connection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a script and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))
            return dict(zip(column_names, result))
