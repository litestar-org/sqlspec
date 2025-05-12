import logging
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.mixins import (
    AsyncArrowBulkOperationsMixin,
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowBulkOperationsMixin,
)
from sqlspec.statement import SQLStatement
from sqlspec.typing import ArrowTable, StatementParameterType, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Sequence

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = logging.getLogger("sqlspec")


class OracleDriverBase:
    """Base class for Oracle drivers with common functionality."""

    dialect: str = "oracle"

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], dict[str, Any]]]]":
        """Process SQL and parameters using SQLStatement with dialect support.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to bind to the statement.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            A tuple of (sql, parameters) ready for execution.
        """
        # Special case: Oracle treats empty dicts as None
        if isinstance(parameters, dict) and not parameters and not kwargs:
            return sql, None

        # Create a SQLStatement with appropriate dialect
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, _ = statement.process()
        if processed_params is None:
            return processed_sql, None
        if isinstance(processed_params, dict):
            return processed_sql, processed_params
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)
        return processed_sql, (processed_params,)  # type: ignore[unreachable]


class OracleSyncDriver(
    OracleDriverBase,
    SyncArrowBulkOperationsMixin["OracleSyncConnection"],
    SQLTranslatorMixin["OracleSyncConnection"],
    SyncDriverAdapterProtocol["OracleSyncConnection"],
    ResultConverter,
):
    """Oracle Sync Driver Adapter."""

    connection: "OracleSyncConnection"

    def __init__(self, connection: "OracleSyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "OracleSyncConnection") -> "Generator[Cursor, None, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database or return None if no rows found.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first row of the query results, or None if no results found.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional type to convert the result to.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first value of the first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value or None if not found.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional type to convert the result to.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first value of the first row of the query results, or None if no results found.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Execute an insert, update, or delete statement.

        Args:
            sql: The SQL statement to execute.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The number of rows affected by the statement.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a SQL script.

        Args:
            sql: The SQL script to execute.
            parameters: The parameters for the script (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            A success message.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        results = connection.fetch_df_all(sql, parameters)
        return cast("ArrowTable", ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names()))  # pyright: ignore

    def _connection(self, connection: "Optional[OracleSyncConnection]" = None) -> "OracleSyncConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection


class OracleAsyncDriver(
    OracleDriverBase,
    AsyncArrowBulkOperationsMixin["OracleAsyncConnection"],
    SQLTranslatorMixin["OracleAsyncConnection"],
    AsyncDriverAdapterProtocol["OracleAsyncConnection"],
    ResultConverter,
):
    """Oracle Async Driver Adapter."""

    connection: "OracleAsyncConnection"

    def __init__(self, connection: "OracleAsyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "OracleAsyncConnection") -> "AsyncGenerator[AsyncCursor, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional type to convert the result to.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first value of the first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value or None if not found.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional type to convert the result to.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first value of the first row of the query results, or None if no results found.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Execute an insert, update, or delete statement.

        Args:
            sql: The SQL statement to execute.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The number of rows affected by the statement.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType]

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a SQL script.

        Args:
            sql: The SQL script to execute.
            parameters: The parameters for the script (dict, tuple, list, or None).
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            A success message.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    async def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query asynchronously and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        results = await connection.fetch_df_all(sql, parameters)
        return ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names())  # pyright: ignore

    def _connection(self, connection: "Optional[OracleAsyncConnection]" = None) -> "OracleAsyncConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
