from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterable, Generator, Iterable

    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

    from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("OracleAsyncDriver", "OracleSyncDriver")


class OracleSyncDriver(SyncDriverAdapterProtocol["Connection"]):
    """Oracle Sync Driver Adapter."""

    connection: "Connection"
    results_as_dict: bool = True

    def __init__(self, connection: "Connection", results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    def _handle_statement_parameters(
        parameters: "StatementParameterType",
    ) -> "Union[list[Any], tuple[Any, ...]]":
        if isinstance(parameters, dict):
            return cast("list[Any]", parameters.values())
        if isinstance(parameters, tuple):
            return parameters
        msg = f"Parameters expected to be dict or tuple, received {parameters}"
        raise TypeError(msg)

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
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Iterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            for row in cursor:  # pyright: ignore[reportUnknownVariableType]
                if schema_type is not None:
                    yield cast("ModelDTOT", schema_type(**dict(zip(column_names, row))))  # pyright: ignore[reportUnknownArgumentType]
                elif self.results_as_dict:
                    yield dict(zip(column_names, row))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                else:
                    yield row

    def select_one(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            if self.results_as_dict:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return result  # type: ignore[no-any-return]

    def select_value(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()

            if result is None:
                return None

            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        with self._with_cursor(connection) as cursor:
            if returning:
                cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
                result = cursor.fetchone()

                if result is None:
                    return None

                # Get column names
                column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return result
            cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount

    def execute_script(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[Connection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        with self._with_cursor(connection) as cursor:
            if returning:
                cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
                result = cursor.fetchone()

                if result is None:
                    return None

                # Get column names
                column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return result
            cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount


class OracleAsyncDriver(AsyncDriverAdapterProtocol["AsyncConnection"]):
    """Oracle Async Driver Adapter."""

    connection: "AsyncConnection"
    results_as_dict: bool = True

    def __init__(self, connection: "AsyncConnection", results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    def _handle_statement_parameters(
        parameters: "StatementParameterType",
    ) -> "Union[list[Any], tuple[Any, ...]]":
        if isinstance(parameters, dict):
            return cast("list[Any]", parameters.values())
        if isinstance(parameters, tuple):
            return parameters
        msg = f"Parameters expected to be dict or tuple, received {parameters}"
        raise TypeError(msg)

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
        parameters: "StatementParameterType",
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch data from the database.

        Returns:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async def _fetch_results() -> "AsyncIterable[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
            async with self._with_cursor(connection) as cursor:
                await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]

                # Get column names
                column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                async for row in cursor:  # pyright: ignore[reportUnknownVariableType]
                    if schema_type is not None:
                        yield cast("ModelDTOT", schema_type(**dict(zip(column_names, row))))  # pyright: ignore[reportUnknownArgumentType]
                    elif self.results_as_dict:
                        yield dict(zip(column_names, row))  # pyright: ignore[reportUnknownArgumentType]
                    else:
                        yield row

        return _fetch_results()

    async def select_one(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            if self.results_as_dict:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return result  # type: ignore[no-any-return]

    async def select_value(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()

            if result is None:
                return None

            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async with self._with_cursor(connection) as cursor:
            if returning:
                await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
                result = await cursor.fetchone()

                if result is None:
                    return None

                # Get column names
                column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return result
            await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount

    async def execute_script(
        self,
        sql: str,
        parameters: "StatementParameterType",
        /,
        connection: "Optional[AsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any, ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}

        async with self._with_cursor(connection) as cursor:
            if returning:
                await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
                result = await cursor.fetchone()

                if result is None:
                    return None

                # Get column names
                column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

                if schema_type is not None:
                    return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
                if self.results_as_dict:
                    return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
                return result
            await cursor.execute(sql, self._handle_statement_parameters(parameters))  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount
