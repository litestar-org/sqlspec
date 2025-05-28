# type: ignore
import logging

# import re # No longer needed
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from asyncmy import Connection
from sqlglot import exp  # Add exp import

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import SQLParsingError

# from sqlspec.exceptions import ParameterStyleMismatchError # Might still be raised by SQLStatement
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql import (
    Query,  # Add Query and ParameterStyle
    SQLStatement,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from asyncmy.cursors import Cursor

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, Statement, StatementParameterType, T
    # from sqlspec.typing import Statement as SQLStatementTyping # Remove alias if present, ensure direct import

__all__ = ("AsyncmyDriver",)

AsyncmyConnection = Connection

logger = logging.getLogger("sqlspec")

# REMOVED: MYSQL_PLACEHOLDER_PATTERN = re.compile(r"(?<!%)%s")


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
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Union[list[Any], dict[str, Any]]]":  # Conform to base return type
        """Process SQL and parameters for asyncmy (MySQL/MariaDB).

        Leverages SQLStatement or Query to parse/process SQL and parameters.
        The AST is then rendered to MySQL-compatible SQL (using
        `%(name)s` for named params, `%s` for positional), which asyncmy expects.
        The appropriate parameter collection (list or dict) is returned.

        Args:
            sql: The SQL statement, sqlglot Expression, or Query object to process.
            parameters: The parameters to bind to the statement (for raw Statement).
            *filters: Statement filters (logged as not applied for raw Statement).
            **kwargs: Additional keyword arguments for SQLStatement.

        Raises:
            SQLParsingError: If SQL parsing fails.

        Returns:
            A tuple of (sql_string, parameters_for_asyncmy) ready for execution.
        """
        final_sql_str: str
        raw_params_for_adapter: Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]
        active_parameters_exist = False  # Flag to help with logging

        if isinstance(sql, Query):
            temp_sql, temp_params_from_query, _ = sql.process()
            # For asyncmy, the SQL from Query.process() needs to be in mysql (pyformat-like) style.
            # We re-parse with SQLStatement to ensure correct dialect output and param extraction.
            stmt_for_mysql = SQLStatement(temp_sql, temp_params_from_query, dialect=self.dialect)

            if not stmt_for_mysql._parsed_expression:
                msg = f"Asyncmy: Failed to parse SQL from Query.process() output: {temp_sql}"
                raise SQLParsingError(msg)
            final_sql_str = stmt_for_mysql._parsed_expression.sql(dialect=self.dialect)  # MySQL dialect for pyformat

            active_parameters_exist = bool(stmt_for_mysql.parameter_info)
            if not active_parameters_exist:
                raw_params_for_adapter = None
            elif isinstance(stmt_for_mysql.merged_parameters, dict):
                raw_params_for_adapter = stmt_for_mysql.merged_parameters
            elif isinstance(stmt_for_mysql.merged_parameters, (list, tuple)):
                raw_params_for_adapter = tuple(stmt_for_mysql.merged_parameters)  # Keep as tuple for now
            elif stmt_for_mysql.merged_parameters is not None:  # Scalar
                raw_params_for_adapter = (stmt_for_mysql.merged_parameters,)
            else:
                raw_params_for_adapter = None

        else:  # Handle raw Statement
            if filters:
                logger.warning(
                    "Filters are provided but `sql` is a raw Statement; filters will not be applied by AsyncmyDriver._process_sql_params."
                )

            statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)
            if not statement._parsed_expression:
                raw_sql_fallback = str(sql) if not isinstance(sql, exp.Expression) else sql.sql(dialect=self.dialect)
                msg = f"Asyncmy: SQLStatement failed to parse SQL: {raw_sql_fallback}"
                raise SQLParsingError(msg)

            final_sql_str = statement._parsed_expression.sql(dialect=self.dialect)  # MySQL dialect for pyformat
            active_parameters_exist = bool(statement.parameter_info)

            if not active_parameters_exist:
                raw_params_for_adapter = None
            elif isinstance(statement.merged_parameters, dict):
                raw_params_for_adapter = statement.merged_parameters
            elif isinstance(statement.merged_parameters, (list, tuple)):
                raw_params_for_adapter = tuple(statement.merged_parameters)  # Keep as tuple
            elif statement.merged_parameters is not None:  # Scalar
                # SQLStatement with convert_parameters should handle scalars appropriately based on context
                # Forcing to tuple for safety if it reaches here as scalar and style is not clear
                raw_params_for_adapter = (statement.merged_parameters,)
            else:
                raw_params_for_adapter = None

        # Conform to Union[list[Any], dict[str, Any]] for return type
        final_params_for_adapter: Union[list[Any], dict[str, Any]]
        if raw_params_for_adapter is None:
            final_params_for_adapter = []
        elif isinstance(raw_params_for_adapter, dict):
            final_params_for_adapter = raw_params_for_adapter
        elif isinstance(raw_params_for_adapter, tuple):  # Convert tuple to list
            final_params_for_adapter = list(raw_params_for_adapter)
        elif isinstance(raw_params_for_adapter, list):
            final_params_for_adapter = raw_params_for_adapter
        else:  # Should not happen
            logger.warning(
                f"Asyncmy: Unexpected type for raw_params_for_adapter: {type(raw_params_for_adapter)}. Defaulting to empty list."
            )
            final_params_for_adapter = []

        # Logging
        if not final_params_for_adapter and active_parameters_exist:
            style_hint = "named (%(name)s)" if isinstance(raw_params_for_adapter, dict) else "positional (%%s)"
            logger.warning(
                f"asyncmy: SQL may expect {style_hint} parameters, but an empty collection is being passed. SQL: {final_sql_str}"
            )

        return final_sql_str, final_params_for_adapter

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            results = await cursor.fetchall()
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]
            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    async def select_one(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    @overload
    async def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            result = await cursor.fetchone()
            if result is None:
                return None
            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    async def insert_update_delete(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
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
        final_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(final_sql, processed_params)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    async def execute_script(
        self,
        sql: "Union[Statement, Query]",  # Allow Query type
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        final_sql, processed_params = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(
                final_sql, processed_params
            )  # asyncmy execute can handle multi-statement scripts with parameters for the first
            return f"Script executed successfully. Rows affected: {cursor.rowcount}"

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
