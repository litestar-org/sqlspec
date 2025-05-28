"""Psqlpy Driver Implementation."""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from psqlpy import Connection, QueryResult
from psqlpy.exceptions import RustPSQLDriverPyBaseError
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.filters import StatementFilter
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql.statement import SQLStatement
from sqlspec.typing import Statement

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from psqlpy import QueryResult

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    SQLTranslatorMixin["PsqlpyConnection"],
    AsyncDriverAdapterProtocol["PsqlpyConnection"],
    ResultConverter,
):
    """Psqlpy Postgres Driver Adapter."""

    connection: "PsqlpyConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "PsqlpyConnection") -> None:
        self.connection = connection

    def _process_sql_params(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for psqlpy.

        Leverages SQLStatement to parse the SQL, validate parameters, and obtain
        a sqlglot AST. This method then transforms the AST to use PostgreSQL-style
        $N placeholders (e.g., $1, $2) as required by psqlpy.

        Args:
            sql: SQL statement (string or sqlglot expression).
            parameters: Query parameters (data or StatementFilter).
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments for SQLStatement.

        Returns:
            A tuple containing the processed SQL string (with $N placeholders)
            and an ordered tuple of parameter values for psqlpy.
        """
        data_params_for_statement: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None
        combined_filters_list: list[StatementFilter] = list(filters)

        if parameters is not None:
            if isinstance(parameters, StatementFilter):
                combined_filters_list.insert(0, parameters)
            else:
                data_params_for_statement = parameters

        statement = SQLStatement(sql, data_params_for_statement, kwargs=kwargs, dialect=self.dialect)

        for filter_obj in combined_filters_list:
            statement = statement.apply_filter(filter_obj)

        parsed_expr, final_ordered_params, placeholder_nodes_in_order = statement.process()

        if not placeholder_nodes_in_order:
            return parsed_expr.sql(dialect=self.dialect), None  # psqlpy can take None or empty list

        placeholder_map: dict[int, exp.Expression] = {
            id(p_node): exp.Parameter(this=exp.Identifier(this=str(i + 1)))
            for i, p_node in enumerate(placeholder_nodes_in_order)
        }

        def replace_with_pg_style(node: exp.Expression) -> exp.Expression:
            return placeholder_map.get(id(node), node)

        transformed_expr = parsed_expr.transform(replace_with_pg_style, copy=True)
        final_sql = transformed_expr.sql(dialect=self.dialect)

        # final_ordered_params is already a tuple, psqlpy expects a list or tuple.
        # Ensure it's a list for psqlpy if not None, or an empty list if None but params expected.
        processed_params: Optional[list[Any]] = None
        if final_ordered_params is not None:
            processed_params = list(final_ordered_params)
        elif placeholder_nodes_in_order:
            processed_params = []

        return final_sql, processed_params

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        # psqlpy expects parameters as a list or None.
        params_for_psqlpy = processed_params if processed_params is not None else []

        results: QueryResult = await connection.fetch(processed_sql, parameters=params_for_psqlpy)

        # Convert to dicts and use ResultConverter
        dict_results = results.result()
        return self.to_schema(dict_results, schema_type=schema_type)

    @overload
    async def select_one(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []

        result = await connection.fetch(processed_sql, parameters=params_for_psqlpy)

        # Convert to dict and use ResultConverter
        dict_results = result.result()
        if not dict_results:
            self.check_not_found(None)

        return self.to_schema(dict_results[0], schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []

        result = await connection.fetch(processed_sql, parameters=params_for_psqlpy)
        dict_results = result.result()

        if not dict_results:
            return None

        return self.to_schema(dict_results[0], schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []

        value = await connection.fetch_val(processed_sql, parameters=params_for_psqlpy)
        value = self.check_not_found(value)

        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []
        try:
            value = await connection.fetch_val(processed_sql, parameters=params_for_psqlpy)
        except RustPSQLDriverPyBaseError:
            return None

        if value is None:
            return None
        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
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
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []

        await connection.execute(processed_sql, parameters=params_for_psqlpy)
        # For INSERT/UPDATE/DELETE, psqlpy returns an empty list but the operation succeeded
        # if no error was raised. We assume 1 row affected if no error, though this might not be accurate.
        # psqlpy's execute doesn't directly return row count for non-SELECT.
        # Consider returning a more generic success/failure or 0/1 if actual count isn't available.
        # For now, keeping previous behavior of assuming 1 if successful.
        # If psqlpy is updated to return row counts, this should be updated.
        return 1  # Placeholder, as psqlpy execute does not return row count for DML

    @overload
    async def insert_update_delete_returning(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Insert, update, or delete data with RETURNING clause.

        Args:
            sql: The SQL statement with RETURNING clause.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        params_for_psqlpy = processed_params if processed_params is not None else []

        result = await connection.fetch(processed_sql, parameters=params_for_psqlpy)
        dict_results = result.result()
        if not dict_results:
            self.check_not_found(None)

        return self.to_schema(dict_results[0], schema_type=schema_type)

    async def execute_script(
        self,
        sql: Statement,
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[PsqlpyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script. psqlpy's execute method can handle multi-statement scripts.

        Args:
            sql: SQL statement or script.
            parameters: Query parameters (typically not used for scripts with psqlpy).
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        # Parameters are not typically used with execute for scripts in psqlpy in the same way as single statements.
        # SQLStatement processing here primarily ensures the SQL is valid and dialect-specific.
        # The `execute` method of psqlpy itself will handle the script execution.
        # We process to get the final SQL string; parameters might be an empty list or None.
        final_sql, _processed_params = self._process_sql_params(sql, parameters, **kwargs)

        await connection.execute(
            final_sql, parameters=None
        )  # psqlpy execute for scripts doesn't use parameters in the list argument
        return "DONE"  # Assuming success if no exception

    def _connection(self, connection: "Optional[PsqlpyConnection]" = None) -> "PsqlpyConnection":
        """Get the connection to use.

        Args:
            connection: Optional connection to use. If not provided, use the default connection.

        Returns:
            The connection to use.
        """
        return connection or self.connection
