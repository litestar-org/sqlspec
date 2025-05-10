import logging
import sqlite3
from contextlib import contextmanager
from sqlite3 import Cursor
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from sqlglot import exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SQLTranslatorMixin["SqliteConnection"],
    SyncDriverAdapterProtocol["SqliteConnection"],
):
    """SQLite Sync Driver Adapter."""

    connection: "SqliteConnection"
    dialect: str = "sqlite"

    def __init__(self, connection: "SqliteConnection") -> None:
        self.connection = connection

    @staticmethod
    def _cursor(connection: "SqliteConnection", *args: Any, **kwargs: Any) -> Cursor:
        return connection.cursor(*args, **kwargs)  # type: ignore[no-any-return]

    @contextmanager
    def _with_cursor(self, connection: "SqliteConnection") -> "Generator[Cursor, None, None]":
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            cursor.close()

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for SQLite.

        SQLite supports both named (:name) and positional (?) parameters.
        This method merges parameters, validates them, and ensures SQL is in a consistent
        format for the sqlite driver using sqlglot.
        """
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...], Any]] = None  # Allow Any for scalar

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for SQLite driver."
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
            # self.dialect is "sqlite"
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"sqlite: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # Traditional named parameters (e.g., @name)
        sql_named_param_nodes = [node for node in parsed_expression.find_all(exp.Parameter) if node.name]

        # Named placeholders parsed as Placeholder nodes (e.g., :name in some dialects)
        named_placeholder_nodes = [
            node
            for node in parsed_expression.find_all(exp.Placeholder)
            if isinstance(node.this, str) and not node.this.isdigit()
        ]

        # Anonymous placeholders (?)
        qmark_placeholder_nodes = [node for node in parsed_expression.find_all(exp.Placeholder) if node.this is None]

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or named_placeholder_nodes or qmark_placeholder_nodes:
                placeholder_types = set()
                if sql_named_param_nodes or named_placeholder_nodes:
                    placeholder_types.add("named (e.g., :name, @name)")
                if qmark_placeholder_nodes:
                    placeholder_types.add("qmark ('?')")
                msg = (
                    f"sqlite: SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            return sql, None  # SQLite can take None

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None

        if isinstance(merged_params, dict):
            # Dictionary parameters. SQLite client handles :name natively.
            if qmark_placeholder_nodes:
                msg = "sqlite: Dictionary parameters provided, but SQL uses positional placeholders ('?'). Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes and not named_placeholder_nodes:
                msg = (
                    "sqlite: Dictionary parameters provided, but no named placeholders (e.g., :name) found by sqlglot."
                )
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
                msg = f"sqlite: Named parameters {missing_keys} found in SQL but not provided. SQL: {sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - sql_param_names_in_ast
            if extra_keys:
                msg = f"sqlite: Parameters {extra_keys} provided but not found in SQL. SQLite might ignore them. SQL: {sql}"
                logger.warning(msg)

            # Generate SQL with sqlite dialect for named params
            final_sql = parsed_expression.sql(dialect=self.dialect)  # Ensures consistent named param style
            final_params = merged_params  # SQLite handles dict directly

        elif isinstance(merged_params, (list, tuple)):
            # Sequence parameters. SQLite uses '?'.
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "sqlite: Sequence parameters provided, but SQL uses named placeholders. Use '?' for sequence parameters."
                raise ParameterStyleMismatchError(msg)

            if len(qmark_placeholder_nodes) != len(merged_params):
                msg = (
                    f"sqlite: Parameter count mismatch. SQL expects {len(qmark_placeholder_nodes)} '?' placeholders, "
                    f"but {len(merged_params)} were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)  # Ensures '?' style
            final_params = tuple(merged_params)  # SQLite can take a tuple

        elif merged_params is not None:  # Scalar parameter
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "sqlite: Scalar parameter provided, but SQL uses named placeholders. Use a single '?'."
                raise ParameterStyleMismatchError(msg)

            if len(qmark_placeholder_nodes) != 1:
                msg = (
                    f"sqlite: Scalar parameter provided, but SQL expects {len(qmark_placeholder_nodes)} '?' placeholders. "
                    f"Expected 1. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = (merged_params,)  # SQLite needs a tuple for a scalar

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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            results = cursor.fetchall()
            if not results:
                return []

            # Get column names
            column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(sql, parameters or [])
        result = cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            if result is None:
                return None

            # Get column names
            column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            result = self.check_not_found(result)

            # Return first value from the row
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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            if result is None:
                return None

            # Return first value from the row
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            connection.commit()
            return cursor.rowcount

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            result = self.check_not_found(result)
            connection.commit()
            # Get column names
            column_names = [column[0] for column in cursor.description]

            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[SqliteConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.executescript(sql)
            connection.commit()
            return "Script executed successfully."

    def _connection(self, connection: "Optional[SqliteConnection]" = None) -> "SqliteConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
