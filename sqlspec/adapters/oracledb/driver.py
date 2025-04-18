from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("OracleAsyncDriver", "OracleSyncDriver")


class OracleSyncDriver(SyncDriverAdapterProtocol["Connection"]):
    """Oracle Sync Driver Adapter."""

    connection: "Connection"

    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "Connection") -> "Generator[Cursor, None, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore

            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore

    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def select_value(
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

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def select_value_or_none(
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

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType]

    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[Connection]" = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]


class OracleAsyncDriver(AsyncDriverAdapterProtocol["AsyncConnection"]):
    """Oracle Async Driver Adapter."""

    connection: "AsyncConnection"

    def __init__(self, connection: "AsyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncConnection") -> "AsyncGenerator[AsyncCursor, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore

            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore

    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType]

    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    async def execute_script_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncConnection]" = None,
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
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
