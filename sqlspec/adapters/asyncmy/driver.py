# type: ignore
import logging
import re
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from asyncmy import Connection

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import is_dict

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncmyDriver",)

AsyncmyConnection = Connection

logger = logging.getLogger("sqlspec")

# Pattern to identify MySQL-style placeholders (%s) for proper conversion
MYSQL_PLACEHOLDER_PATTERN = re.compile(r"(?<!%)%s")


class AsyncmyDriver(
    SQLTranslatorMixin["AsyncmyConnection"],
    AsyncDriverAdapterProtocol["AsyncmyConnection"],
    ResultConverter,
):
    """Asyncmy MySQL/MariaDB Driver Adapter."""

    connection: "AsyncmyConnection"
    dialect: str = "mysql"

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncmyConnection") -> AsyncGenerator["Cursor", None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters using SQLStatement with dialect support.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to bind to the statement.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Raises:
            ParameterStyleMismatchError: If the parameter style is not supported.

        Returns:
            A tuple of (sql, parameters) ready for execution.
        """
        # Handle MySQL-specific placeholders (%s) which SQLGlot doesn't parse well
        # If %s placeholders are present, handle them directly
        mysql_placeholders_count = len(MYSQL_PLACEHOLDER_PATTERN.findall(sql))

        if mysql_placeholders_count > 0:
            # For MySQL format placeholders, minimal processing is needed
            if parameters is None:
                if mysql_placeholders_count > 0:
                    msg = f"asyncmy: SQL statement contains {mysql_placeholders_count} format placeholders ('%s'), but no parameters were provided. SQL: {sql}"
                    raise ParameterStyleMismatchError(msg)
                return sql, None

            # Convert dict to tuple if needed
            if is_dict(parameters):
                # MySQL's %s placeholders require positional params
                msg = "asyncmy: Dictionary parameters provided with '%s' placeholders. MySQL format placeholders require tuple/list parameters."
                raise ParameterStyleMismatchError(msg)

            # Convert to tuple (handles both scalar and sequence cases)
            if not isinstance(parameters, (list, tuple)):
                # Scalar parameter case
                return sql, (parameters,)

            # Sequence parameter case - ensure appropriate length
            if len(parameters) != mysql_placeholders_count:
                msg = f"asyncmy: Parameter count mismatch. SQL expects {mysql_placeholders_count} '%s' placeholders, but {len(parameters)} parameters were provided. SQL: {sql}"
                raise ParameterStyleMismatchError(msg)

            return sql, tuple(parameters)

        # Create a SQLStatement with MySQL dialect
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        # Process the statement for execution
        processed_sql, processed_params, _ = statement.process()

        # Convert parameters to the format expected by MySQL
        if processed_params is None:
            return processed_sql, None

        # For MySQL, ensure parameters are in the right format
        if is_dict(processed_params):
            # Dictionary parameters are not well supported by asyncmy
            msg = "asyncmy: Dictionary parameters are not supported for MySQL placeholders. Use sequence parameters."
            raise ParameterStyleMismatchError(msg)

        # For sequence parameters, ensure they're a tuple
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)

        # For scalar parameter, wrap in a tuple
        return processed_sql, (processed_params,)

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            results = await cursor.fetchall()
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]

            # Convert to dicts first
            dict_results = [dict(zip(column_names, row)) for row in results]
            return self.to_schema(dict_results, schema_type=schema_type)

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
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
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return f"Script executed successfully. Rows affected: {cursor.rowcount}"

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
