import contextlib
import logging
import re
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast, overload

from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp as sqlglot_exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.filters import StatementFilter
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import ArrowTable, StatementParameterType, is_dict

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection

# SQLite named parameter pattern - simple pattern to find parameter references
SQLITE_PARAM_PATTERN = re.compile(r"(?::|\$|@)([a-zA-Z0-9_]+)")

# Patterns to identify comments and string literals
SQL_COMMENT_PATTERN = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
SQL_STRING_PATTERN = re.compile(r"'[^']*'|\"[^\"]*\"")


class AdbcDriver(
    SyncArrowBulkOperationsMixin["AdbcConnection"],
    SQLTranslatorMixin["AdbcConnection"],
    SyncDriverAdapterProtocol["AdbcConnection"],
    ResultConverter,
):
    """ADBC Sync Driver Adapter."""

    connection: AdbcConnection
    __supports_arrow__: ClassVar[bool] = True
    dialect: str = "adbc"

    def __init__(self, connection: "AdbcConnection") -> None:
        """Initialize the ADBC driver adapter."""
        self.connection = connection
        self.dialect = self._get_dialect(connection)  # Store detected dialect

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        driver_name = connection.adbc_get_info()["vendor_name"].lower()
        if "postgres" in driver_name:
            return "postgres"
        if "bigquery" in driver_name:
            return "bigquery"
        if "sqlite" in driver_name:
            return "sqlite"
        if "duckdb" in driver_name:
            return "duckdb"
        if "mysql" in driver_name:
            return "mysql"
        if "snowflake" in driver_name:
            return "snowflake"
        return "postgres"  # default to postgresql dialect

    @staticmethod
    def _cursor(connection: "AdbcConnection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "AdbcConnection") -> Generator["Cursor", None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _process_sql_params(  # noqa: C901, PLR0912, PLR0915
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[tuple[Any, ...]]]":  # Always returns tuple or None for params
        """Process SQL and parameters for ADBC.

        ADBC drivers generally use positional parameters with '?' placeholders.
        This method processes the SQL statement and transforms parameters into the format
        expected by ADBC drivers.

        Args:
            sql: The SQL statement to process.
            parameters: The parameters to bind to the statement.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Raises:
            ParameterStyleMismatchError: If positional parameters are mixed with keyword arguments.
            SQLParsingError: If the SQL statement cannot be parsed.

        Returns:
            A tuple of (sql, parameters) ready for execution.
        """
        # Special handling for SQLite with non-dict parameters and named placeholders
        if self.dialect == "sqlite" and parameters is not None and not is_dict(parameters):
            # First mask out comments and strings to avoid detecting parameters in those
            comments = list(SQL_COMMENT_PATTERN.finditer(sql))
            strings = list(SQL_STRING_PATTERN.finditer(sql))

            all_matches = [(m.start(), m.end(), "comment") for m in comments] + [
                (m.start(), m.end(), "string") for m in strings
            ]
            all_matches.sort(reverse=True)

            for start, end, _ in all_matches:
                sql = sql[:start] + " " * (end - start) + sql[end:]

            # Find named parameters in clean SQL
            named_params = list(SQLITE_PARAM_PATTERN.finditer(sql))

            if named_params:
                param_positions = [(m.start(), m.end()) for m in named_params]
                param_positions.sort(reverse=True)
                for start, end in param_positions:
                    sql = sql[:start] + "?" + sql[end:]
                if not isinstance(parameters, (list, tuple)):
                    return sql, (parameters,)
                return sql, tuple(parameters)

        # Standard processing for all other cases
        merged_params = parameters
        if kwargs:
            if is_dict(parameters):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for adbc driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs

        # 2. Create SQLStatement with dialect and process
        statement = SQLStatement(sql, merged_params, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, parsed_expr = statement.process()

        # Special handling for SQLite dialect with dict parameters
        if self.dialect == "sqlite" and is_dict(processed_params):
            # First, mask out comments and string literals with placeholders
            masked_sql = processed_sql

            # Replace comments and strings with placeholders
            comments = list(SQL_COMMENT_PATTERN.finditer(masked_sql))
            strings = list(SQL_STRING_PATTERN.finditer(masked_sql))

            # Sort all matches by their start position (descending)
            all_matches = [(m.start(), m.end(), "comment") for m in comments] + [
                (m.start(), m.end(), "string") for m in strings
            ]
            all_matches.sort(reverse=True)

            # Replace each match with spaces to preserve positions
            for start, end, _ in all_matches:
                masked_sql = masked_sql[:start] + " " * (end - start) + masked_sql[end:]

            # Now find parameters in the masked SQL
            param_order = []
            param_spans = []  # Store (start, end) of each parameter

            for match in SQLITE_PARAM_PATTERN.finditer(masked_sql):
                param_name = match.group(1)
                if param_name in processed_params:
                    param_order.append(param_name)
                    param_spans.append((match.start(), match.end()))

            if param_order:
                # Replace parameters with ? placeholders in reverse order to preserve positions
                result_sql = processed_sql
                for i, (start, end) in enumerate(reversed(param_spans)):  # noqa: B007
                    # Replace :param with ?
                    result_sql = result_sql[:start] + "?" + result_sql[start + 1 + len(param_order[-(i + 1)]) :]

                return result_sql, tuple(processed_params[name] for name in param_order)

        if processed_params is None:
            return processed_sql, ()
        if (
            isinstance(processed_params, (tuple, list))
            or (processed_params is not None and not isinstance(processed_params, dict))
        ) and parsed_expr is not None:
            # Find all named placeholders
            named_param_nodes = [
                node
                for node in parsed_expr.find_all(sqlglot_exp.Parameter, sqlglot_exp.Placeholder)
                if (isinstance(node, sqlglot_exp.Parameter) and node.name and not node.name.isdigit())
                or (
                    isinstance(node, sqlglot_exp.Placeholder)
                    and node.this
                    and not isinstance(node.this, (sqlglot_exp.Identifier, sqlglot_exp.Literal))
                    and not str(node.this).isdigit()
                )
            ]

            # If we found named parameters, transform to question marks
            if named_param_nodes:

                def convert_to_qmark(node: sqlglot_exp.Expression) -> sqlglot_exp.Expression:
                    if (isinstance(node, sqlglot_exp.Parameter) and node.name and not node.name.isdigit()) or (
                        isinstance(node, sqlglot_exp.Placeholder)
                        and node.this
                        and not isinstance(node.this, (sqlglot_exp.Identifier, sqlglot_exp.Literal))
                        and not str(node.this).isdigit()
                    ):
                        return sqlglot_exp.Placeholder()
                    return node

                # Transform the SQL
                processed_sql = parsed_expr.transform(convert_to_qmark, copy=True).sql(dialect=self.dialect)

                # If it's a scalar parameter, ensure it's wrapped in a tuple
                if not isinstance(processed_params, (tuple, list)):
                    processed_params = (processed_params,)  # type: ignore[unreachable]

        # 6. Handle dictionary parameters
        if is_dict(processed_params):
            # Skip conversion if there's no parsed expression to work with
            if parsed_expr is None:
                msg = f"ADBC ({self.dialect}): Failed to parse SQL with dictionary parameters. Cannot determine parameter order."
                raise SQLParsingError(msg)

            # Collect named parameters in the order they appear in the SQL
            named_params = []
            for node in parsed_expr.find_all(sqlglot_exp.Parameter, sqlglot_exp.Placeholder):
                if isinstance(node, sqlglot_exp.Parameter) and node.name and node.name in processed_params:
                    named_params.append(node.name)  # type: ignore[arg-type]
                elif (
                    isinstance(node, sqlglot_exp.Placeholder)
                    and isinstance(node.this, str)
                    and node.this in processed_params
                ):
                    named_params.append(node.this)  # type: ignore[arg-type]

            # If we found named parameters, convert them to ? placeholders
            if named_params:
                # Transform SQL to use ? placeholders
                def convert_to_qmark(node: sqlglot_exp.Expression) -> sqlglot_exp.Expression:
                    if isinstance(node, sqlglot_exp.Parameter) and node.name and node.name in processed_params:
                        return sqlglot_exp.Placeholder()  # Anonymous ? placeholder
                    if (
                        isinstance(node, sqlglot_exp.Placeholder)
                        and isinstance(node.this, str)
                        and node.this in processed_params
                    ):
                        return sqlglot_exp.Placeholder()  # Anonymous ? placeholder
                    return node

                return parsed_expr.transform(convert_to_qmark, copy=True).sql(dialect=self.dialect), tuple(
                    processed_params[name]  # type: ignore[index]
                    for name in named_params
                )
            return processed_sql, tuple(processed_params.values())
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)
        return processed_sql, (processed_params,)

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *filters: "StatementFilter",
        connection: Optional["AdbcConnection"] = None,
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
            results = cursor.fetchall()  # pyright: ignore
            if not results:
                return []
            column_names = [column[0] for column in cursor.description or []]

            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
            cursor.execute(sql, parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *filters: "StatementFilter",
        connection: Optional["AdbcConnection"] = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            column_names = [column[0] for column in cursor.description or []]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AdbcConnection]" = None,
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
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Args:
            sql: The SQL statement to execute.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if not result:
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            return self.to_schema(dict(zip(column_names, result[0])), schema_type=schema_type)

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AdbcConnection]" = None,
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
            return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("ArrowTable", cursor.fetch_arrow_table())  # pyright: ignore[reportUnknownMemberType]
