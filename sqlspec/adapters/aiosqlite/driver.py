from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import AsyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from aiosqlite import Connection, Cursor

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("AiosqliteDriver",)


class AiosqliteDriver(AsyncDriverAdapterProtocol["Connection"]):
    """SQLite Async Driver Adapter."""

    connection: "Connection"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    @staticmethod
    async def _cursor(connection: "Connection", *args: Any, **kwargs: Any) -> "Cursor":
        return await connection.cursor(*args, **kwargs)

    @asynccontextmanager
    async def _with_cursor(self, connection: "Connection") -> "AsyncGenerator[Cursor, None]":
        cursor = await self._cursor(connection)
        try:
            yield cursor
        finally:
            await cursor.close()

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name) to positional parameters (?) for SQLite.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).

        Returns:
            A tuple containing the processed SQL string and the processed parameters.
        """
        if not isinstance(parameters, dict) or not parameters:
            # If parameters are not a dict, or empty dict, assume positional/no params
            # Let the underlying driver handle tuples/lists directly
            return sql, parameters

        # Convert named parameters to positional parameters
        processed_sql = sql
        processed_params: list[Any] = []
        for key, value in parameters.items():
            # Replace :key with ? in the SQL
            processed_sql = processed_sql.replace(f":{key}", "?")
            processed_params.append(value)

        return processed_sql, tuple(processed_params)

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = await cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if schema_type is None:
                return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType]
            result = self.check_not_found(result)
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1  # pyright: ignore[reportUnknownVariableType, reportGeneralTypeIssues]

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

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = list(await cursor.fetchall())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:  # Check if empty
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, results[0]))))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return dict(zip(column_names, results[0]))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]

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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = list(await cursor.fetchall())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:  # Check if empty
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, results[0]))))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return dict(zip(column_names, results[0]))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
