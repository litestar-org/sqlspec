import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.typing import ArrowTable, StatementParameterType

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("DuckDBConnection", "DuckDBDriver")

logger = logging.getLogger("sqlspec")

DuckDBConnection = DuckDBPyConnection


class DuckDBDriver(
    SyncArrowBulkOperationsMixin["DuckDBConnection"],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncDriverAdapterProtocol["DuckDBConnection"],
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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for DuckDB.

        DuckDB supports both named (:name, $name) and positional (?) parameters.
        This method merges parameters, validates them, and ensures SQL is in a
        consistent format for the duckdb driver using sqlglot.
        """
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...], Any]] = None  # Allow Any for scalar

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for DuckDB driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters
        # else merged_params remains None

        # Special case: if merged_params is an empty dict, treat it as None for parameterless queries
        if isinstance(merged_params, dict) and not merged_params:
            merged_params = None

        # 2. SQLGlot Parsing
        try:
            # self.dialect is "duckdb"
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"duckdb: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # Traditional named parameters (e.g., @name, $name) parsed as Parameter nodes
        sql_named_param_nodes = [node for node in parsed_expression.find_all(exp.Parameter) if node.name]

        # Named placeholders parsed as Placeholder nodes (e.g., :name, $name in some dialects)
        named_placeholder_nodes = [
            node
            for node in parsed_expression.find_all(exp.Placeholder)
            if isinstance(node.this, str) and not node.this.isdigit()
        ]

        # Anonymous placeholders (?) parsed as Placeholder nodes with this=None
        qmark_placeholder_nodes = [node for node in parsed_expression.find_all(exp.Placeholder) if node.this is None]

        # DuckDB also uses $N for positional, sqlglot might parse these as exp.Parameter without name and numeric .this
        sql_dollar_numeric_nodes = [
            node
            for node in parsed_expression.find_all(exp.Parameter)
            if not node.name and node.this and isinstance(node.this, str) and node.this.isdigit()
        ]

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or named_placeholder_nodes or qmark_placeholder_nodes or sql_dollar_numeric_nodes:
                placeholder_types = set()
                if sql_named_param_nodes or named_placeholder_nodes:
                    placeholder_types.add("named (e.g., :name, $name)")
                if qmark_placeholder_nodes:
                    placeholder_types.add("qmark ('?')")
                if sql_dollar_numeric_nodes:
                    placeholder_types.add("numeric ($N)")
                msg = (
                    f"duckdb: SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            return sql, None  # DuckDB can take None

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None

        if isinstance(merged_params, dict):
            # Dictionary parameters. DuckDB client handles :name and $name if SQL uses them.
            # sqlglot's "duckdb" dialect should preserve/generate these.
            if qmark_placeholder_nodes or sql_dollar_numeric_nodes:
                msg = "duckdb: Dictionary parameters provided, but SQL uses positional placeholders ('?' or $N). Use named placeholders."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes and not named_placeholder_nodes:
                msg = "duckdb: Dictionary parameters provided, but no named placeholders (e.g., :name, $name) found by sqlglot."
                raise ParameterStyleMismatchError(msg)

            # Collect parameter names from both types of nodes
            sql_param_names_in_ast = set()

            # Get names from Parameter nodes
            sql_param_names_in_ast.update(node.name for node in sql_named_param_nodes if node.name)

            # Get names from Placeholder nodes
            sql_param_names_in_ast.update(node.this for node in named_placeholder_nodes if isinstance(node.this, str))

            provided_keys = set(merged_params.keys())

            missing_keys = sql_param_names_in_ast - provided_keys
            if missing_keys:
                msg = f"duckdb: Named parameters {missing_keys} found in SQL but not provided. SQL: {sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - sql_param_names_in_ast
            if extra_keys:
                logger.warning(
                    f"duckdb: Parameters {extra_keys} provided but not found in SQL. DuckDB might ignore them. SQL: {sql}"
                )

            # Generate SQL with duckdb dialect to ensure consistent named param style if input was varied.
            # e.g. if input was :name, output for duckdb might be $name or vice-versa, sqlglot handles this.
            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = merged_params  # Pass the dict directly to DuckDB

        elif isinstance(merged_params, (list, tuple)):
            # Sequence parameters. DuckDB uses '?'.
            # sqlglot's "duckdb" dialect should generate '?' from exp.Placeholder.
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = (
                    "duckdb: Sequence parameters provided, but SQL uses named placeholders. Use '?' or $N placeholders."
                )
                raise ParameterStyleMismatchError(msg)

            # If SQL already has $N, it's a mismatch for unnamed sequence params
            if sql_dollar_numeric_nodes:
                msg = "duckdb: Sequence parameters provided, but SQL uses $N style. Use '?' placeholders for unnamed sequences."
                raise ParameterStyleMismatchError(msg)

            if len(qmark_placeholder_nodes) != len(merged_params):
                msg = (
                    f"duckdb: Parameter count mismatch. SQL expects {len(qmark_placeholder_nodes)} '?' placeholders, "
                    f"but {len(merged_params)} were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)  # Ensures qmark style from placeholders
            final_params = tuple(merged_params)

        elif merged_params is not None:  # Scalar parameter
            if sql_named_param_nodes or named_placeholder_nodes or sql_dollar_numeric_nodes:
                msg = "duckdb: Scalar parameter provided, but SQL uses named or $N placeholders. Use a single '?'."
                raise ParameterStyleMismatchError(msg)

            if len(qmark_placeholder_nodes) != 1:
                msg = (
                    f"duckdb: Scalar parameter provided, but SQL expects {len(qmark_placeholder_nodes)} '?' placeholders. "
                    f"Expected 1. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = (merged_params,)

        else:  # Should be caught by 'merged_params is None' earlier
            final_sql = sql
            final_params = None

        return final_sql, final_params

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            results = cursor.fetchall()
            if not results:
                return []
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return [dict(zip(column_names, row)) for row in results]
            return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            column_names = [column[0] for column in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return getattr(cursor, "rowcount", -1)

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            result = cursor.fetchall()
            result = self.check_not_found(result)
            column_names = [col[0] for col in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result[0]))))
            return dict(zip(column_names, result[0]))

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("str", getattr(cursor, "statusmessage", "DONE"))

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("ArrowTable", cursor.fetch_arrow_table())

    def _connection(self, connection: "Optional[DuckDBConnection]" = None) -> "DuckDBConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
