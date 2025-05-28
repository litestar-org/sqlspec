import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from duckdb import DuckDBPyConnection
from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import SQLParsingError
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.sql import ParameterStyle, Query, SQLStatement
from sqlspec.typing import ArrowTable

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ArrowTable, ModelDTOT, Statement, StatementParameterType, T

__all__ = ("DuckDBConnection", "DuckDBDriver")

logger = logging.getLogger("sqlspec")

DuckDBConnection = DuckDBPyConnection


class DuckDBDriver(
    SyncArrowBulkOperationsMixin["DuckDBConnection"],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncDriverAdapterProtocol["DuckDBConnection"],
    ResultConverter,
):
    """DuckDB Sync Driver Adapter."""

    connection: "DuckDBConnection"
    use_cursor: bool = True
    dialect: str = "duckdb"

    def __init__(self, connection: "DuckDBConnection", use_cursor: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor

    def _cursor(self, connection: "DuckDBConnection") -> "DuckDBConnection":
        if self.use_cursor:
            return connection.cursor()
        return connection

    @contextmanager
    def _with_cursor(self, connection: "DuckDBConnection") -> "Generator[DuckDBConnection, None, None]":
        if self.use_cursor:
            cursor = self._cursor(connection)
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            yield connection

    def _process_sql_params(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, list[Any]]":
        """Process SQL and parameters for DuckDB.

        Leverages SQLStatement or Query to parse/process SQL. The AST is then
        transformed to use 'qmark' placeholders, and an ordered list of parameters
        is returned, as expected by DuckDB's Python API.

        Args:
            sql: SQL statement (string, sqlglot expression, or Query object).
            parameters: Query parameters (for raw Statement).
            *filters: Statement filters (logged as not applied for raw Statement).
            **kwargs: Additional keyword arguments for SQLStatement.

        Returns:
            A tuple containing the processed SQL string (with '?' placeholders)
            and an ordered list of parameter values for DuckDB.
        """
        processed_sql_str: str
        ordered_params_tuple: Optional[tuple[Any, ...]] = None

        if isinstance(sql, Query):
            temp_sql, temp_params_from_query, _ = sql.process()
            # We still need to convert temp_sql to qmark and get ordered params
            # The temp_params_from_query (dict or list) will be passed to SQLStatement
            stmt_for_qmark = SQLStatement(temp_sql, temp_params_from_query, dialect=self.dialect)

            if not stmt_for_qmark._parsed_expression:
                msg = f"DuckDB: Failed to parse SQL from Query.process() output: {temp_sql}"
                raise SQLParsingError(msg)

            parsed_expr_to_transform = stmt_for_qmark._parsed_expression
            # SQLStatement.merged_parameters should reflect temp_params_from_query accurately
            # SQLStatement.parameter_info should give us the placeholders in order from temp_sql
            ordered_params_tuple = self._get_ordered_params_from_statement(stmt_for_qmark)
            placeholder_nodes_in_order = stmt_for_qmark.parameter_info  # these are ParameterInfo objects

        else:  # Handle raw Statement
            if filters:
                logger.warning(
                    "Filters are provided but `sql` is a raw Statement; filters will not be applied by DuckDBDriver._process_sql_params."
                )

            statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)
            if not statement._parsed_expression:
                raw_sql_fallback = str(sql) if not isinstance(sql, exp.Expression) else sql.sql(dialect=self.dialect)
                msg = f"DuckDB: SQLStatement failed to parse SQL: {raw_sql_fallback}"
                raise SQLParsingError(msg)

            parsed_expr_to_transform = statement._parsed_expression
            ordered_params_tuple = self._get_ordered_params_from_statement(statement)
            placeholder_nodes_in_order = statement.parameter_info

        # Transform to qmark SQL
        if not placeholder_nodes_in_order:  # No params, no transformation needed
            processed_sql_str = parsed_expr_to_transform.sql(dialect=self.dialect)
        else:
            # Create a map from original placeholder AST node IDs to qmark placeholders
            # We need the actual placeholder *expressions* from the parsed_expr_to_transform
            # that correspond to parameter_info entries.
            # This part is tricky as parameter_info are descriptors, not the live AST nodes.
            # Simpler: sqlglot can often convert to qmark style directly in .sql()
            # by using a dialect that defaults to qmark or by specific transform.
            # DuckDB dialect in sqlglot might already do this or accept named params that map to qmark internally.
            # Let's try a specific transform to qmark if placeholders exist.

            # Find all placeholders in the expression to be transformed
            # These are the actual sqlglot placeholder nodes
            ast_placeholders = list(parsed_expr_to_transform.find_all(exp.Placeholder))

            if ast_placeholders:  # Only transform if there are placeholders in the AST
                placeholder_map: dict[int, exp.Expression] = {
                    id(p_node): exp.Placeholder() for p_node in ast_placeholders
                }

                def replace_with_qmark(node: exp.Expression) -> exp.Expression:
                    return placeholder_map.get(id(node), node)

                transformed_expr = parsed_expr_to_transform.transform(replace_with_qmark, copy=True)
                processed_sql_str = transformed_expr.sql(dialect=self.dialect)
            else:  # No AST placeholders found, though parameter_info might exist (e.g. from original string parsing)
                # This implies a mismatch or that params are not used in the final expression.
                processed_sql_str = parsed_expr_to_transform.sql(dialect=self.dialect)
                if placeholder_nodes_in_order:  # ParamInfo existed, but no placeholders in final AST for qmark.
                    logger.warning(
                        "DuckDB: ParameterInfo existed but no placeholders found in the final AST to convert to qmark. SQL may not be as expected."
                    )

        final_params_list = list(ordered_params_tuple) if ordered_params_tuple is not None else []
        return processed_sql_str, final_params_list

    def _get_ordered_params_from_statement(self, statement: SQLStatement) -> Optional[tuple[Any, ...]]:
        """Helper to extract ordered parameters from a SQLStatement object for DuckDB."""
        if not statement.parameter_info:  # No parameters described
            return None

        # DuckDB uses positional parameters. SQLStatement.merged_parameters might be a dict
        # if named parameters were provided. We need to order them according to parameter_info.

        if statement.parameter_style == ParameterStyle.NAMED_COLON and isinstance(statement.merged_parameters, dict):
            ordered_values = []
            for p_info in statement.parameter_info:
                if p_info.name in statement.merged_parameters:
                    ordered_values.append(statement.merged_parameters[p_info.name])
                else:
                    # This implies a named placeholder in SQL (from ParameterInfo) was not in the provided dict.
                    # SQLStatement's convert_parameters with validate=True should catch this.
                    msg = f"DuckDB: Missing parameter value for named placeholder '{p_info.name}'."
                    raise ValueError(msg)
            return tuple(ordered_values)

        if isinstance(statement.merged_parameters, (list, tuple)):
            return tuple(statement.merged_parameters)

        if statement.merged_parameters is not None:  # Scalar
            if len(statement.parameter_info) == 1:
                return (statement.merged_parameters,)
            msg = "DuckDB: Scalar parameter provided for query with multiple placeholders."
            raise ValueError(msg)

        # If merged_parameters is None but parameter_info exists, it means params were expected but not given.
        # SQLStatement validation should ideally catch this. If we reach here, means it wasn't caught or not validated.
        if statement.parameter_info:
            msg = "DuckDB: Query expects parameters, but none were resolved from SQLStatement."
            raise ValueError(msg)

        return None  # No parameters found or expected

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            results = cursor.fetchall()
            if not results:
                return []
            column_names = [column[0] for column in cursor.description or []]
            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results, or None if no results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            result = cursor.fetchone()
            if result is None:
                return None
            column_names = [column[0] for column in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            result = cursor.fetchone()
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            return getattr(cursor, "rowcount", -1)

    @overload
    def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            result = cursor.fetchall()
            result = self.check_not_found(result)
            column_names = [col[0] for col in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result[0])), schema_type=schema_type)

    def execute_script(
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            return cast("str", getattr(cursor, "statusmessage", "DONE"))

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: "Union[Statement, Query]",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            *filters: Optional filters to apply to the SQL statement.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(processed_sql, processed_params)
            return cast("ArrowTable", cursor.fetch_arrow_table())

    def _connection(self, connection: "Optional[DuckDBConnection]" = None) -> "DuckDBConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
