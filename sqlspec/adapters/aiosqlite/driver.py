import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, overload

import aiosqlite
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import is_dict

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AiosqliteConnection", "AiosqliteDriver")
AiosqliteConnection = aiosqlite.Connection

logger = logging.getLogger("sqlspec")


class AiosqliteDriver(
    SQLTranslatorMixin["AiosqliteConnection"],
    AsyncDriverAdapterProtocol["AiosqliteConnection"],
    ResultConverter,
):
    """SQLite Async Driver Adapter."""

    connection: "AiosqliteConnection"
    dialect: str = "sqlite"

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection

    @staticmethod
    async def _cursor(connection: "AiosqliteConnection", *args: Any, **kwargs: Any) -> "aiosqlite.Cursor":
        return await connection.cursor(*args, **kwargs)

    @asynccontextmanager
    async def _with_cursor(self, connection: "AiosqliteConnection") -> "AsyncGenerator[aiosqlite.Cursor, None]":
        cursor = await self._cursor(connection)
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
        """Process SQL and parameters for aiosqlite using SQLStatement.

        aiosqlite supports both named (:name) and positional (?) parameters.
        This method processes the SQL with dialect-aware parsing and handles
        parameters appropriately for aiosqlite.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            Tuple of processed SQL and parameters.
        """
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, parsed_expr = statement.process()
        if processed_params is None:
            return processed_sql, None

        if is_dict(processed_params):
            # For dict parameters, we need to use ordered ? placeholders
            # but only if we have a parsed expression to work with
            if parsed_expr:
                # Collect named parameters in the order they appear in the SQL
                named_params = []
                for node in parsed_expr.find_all(exp.Parameter, exp.Placeholder):
                    if isinstance(node, exp.Parameter) and node.name and node.name in processed_params:
                        named_params.append(node.name)
                    elif (
                        isinstance(node, exp.Placeholder)
                        and isinstance(node.this, str)
                        and node.this in processed_params
                    ):
                        named_params.append(node.this)

                if named_params:
                    # Transform SQL to use ? placeholders
                    def _convert_to_qmark(node: exp.Expression) -> exp.Expression:
                        if (isinstance(node, exp.Parameter) and node.name and node.name in processed_params) or (
                            isinstance(node, exp.Placeholder)
                            and isinstance(node.this, str)
                            and node.this in processed_params
                        ):
                            return exp.Placeholder()  # ? placeholder
                        return node

                    return parsed_expr.transform(_convert_to_qmark, copy=True).sql(dialect=self.dialect), tuple(
                        processed_params[name] for name in named_params
                    )
            return processed_sql, processed_params
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)
        return processed_sql, (processed_params,)

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        results = await cursor.fetchall()
        if not results:
            return []

        # Get column names
        column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        # Convert to dict and then use ResultConverter
        dict_result = dict(zip(column_names, result))
        return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Get column names
        column_names = [column[0] for column in cursor.description]

        # Convert to dict and then use ResultConverter
        dict_result = dict(zip(column_names, result))
        return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return schema_type(result_value)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        await connection.commit()
        return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        await connection.commit()
        await cursor.close()

        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        # Convert to dict and then use ResultConverter
        dict_result = dict(zip(column_names, result))
        return self.to_schema(dict_result, schema_type=schema_type)

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AiosqliteConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the script
        await connection.executescript(sql)
        await connection.commit()
        return "Script executed successfully."

    def _connection(self, connection: "Optional[AiosqliteConnection]" = None) -> "AiosqliteConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
