"""Psqlpy Driver Implementation."""

import logging
import re
from re import Match
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from psqlpy import Connection, QueryResult
from psqlpy.exceptions import RustPSQLDriverPyBaseError
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import SQLParsingError
from sqlspec.filters import StatementFilter
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import is_dict

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psqlpy import QueryResult

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

# Improved regex to match question mark placeholders only when they are outside string literals and comments
# This pattern handles:
# 1. Single quoted strings with escaped quotes
# 2. Double quoted strings with escaped quotes
# 3. Single-line comments (-- to end of line)
# 4. Multi-line comments (/* to */)
# 5. Only question marks outside of these contexts are considered parameters
QUESTION_MARK_PATTERN = re.compile(
    r"""
    (?:'[^']*(?:''[^']*)*') |           # Skip single-quoted strings (with '' escapes)
    (?:"[^"]*(?:""[^"]*)*") |           # Skip double-quoted strings (with "" escapes)
    (?:--.*?(?:\n|$)) |                 # Skip single-line comments
    (?:/\*(?:[^*]|\*(?!/))*\*/) |       # Skip multi-line comments
    (\?)                                # Capture only question marks outside of these contexts
    """,
    re.VERBOSE | re.DOTALL,
)

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
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], dict[str, Any]]]]":
        """Process SQL and parameters for psqlpy.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            The SQL statement and parameters.

        Raises:
            SQLParsingError: If the SQL parsing fails.
        """
        # Handle scalar parameter by converting to a single-item tuple
        if parameters is not None and not isinstance(parameters, (list, tuple, dict)):
            parameters = (parameters,)

        # Create and process the statement
        statement = SQLStatement(sql=sql, parameters=parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        # Process the statement
        sql, validated_params, parsed_expr = statement.process()

        if validated_params is None:
            return sql, None  # psqlpy can handle None

        # Convert positional parameters from question mark style to PostgreSQL's $N style
        if isinstance(validated_params, (list, tuple)):
            # Use a counter to generate $1, $2, etc. for each ? in the SQL that's outside strings/comments
            param_index = 0

            def replace_question_mark(match: Match[str]) -> str:
                # Only process the match if it's not in a skipped context (string/comment)
                if match.group(1):  # This is a question mark outside string/comment
                    nonlocal param_index
                    param_index += 1
                    return f"${param_index}"
                # Return the entire matched text unchanged for strings/comments
                return match.group(0)

            return QUESTION_MARK_PATTERN.sub(replace_question_mark, sql), tuple(validated_params)

        # If no parsed expression is available, we can't safely transform dictionary parameters
        if is_dict(validated_params) and parsed_expr is None:
            msg = f"psqlpy: SQL parsing failed and dictionary parameters were provided. Cannot determine parameter order without successful parse. SQL: {sql}"
            raise SQLParsingError(msg)

        # Convert dictionary parameters to the format expected by psqlpy
        if is_dict(validated_params) and parsed_expr is not None:
            # Find all named parameters in the SQL expression
            named_params = []

            for node in parsed_expr.find_all(exp.Parameter, exp.Placeholder):
                if isinstance(node, exp.Parameter) and node.name and node.name in validated_params:
                    named_params.append(node.name)
                elif isinstance(node, exp.Placeholder) and isinstance(node.this, str) and node.this in validated_params:
                    named_params.append(node.this)

            if named_params:
                # Transform the SQL to use $1, $2, etc.
                def convert_named_to_dollar(node: exp.Expression) -> exp.Expression:
                    if isinstance(node, exp.Parameter) and node.name and node.name in validated_params:
                        idx = named_params.index(node.name) + 1
                        return exp.Parameter(this=str(idx))
                    if (
                        isinstance(node, exp.Placeholder)
                        and isinstance(node.this, str)
                        and node.this in validated_params
                    ):
                        idx = named_params.index(node.this) + 1
                        return exp.Parameter(this=str(idx))
                    return node

                return parsed_expr.transform(convert_named_to_dollar, copy=True).sql(dialect=self.dialect), tuple(
                    validated_params[name] for name in named_params
                )

            # If no named parameters were found in the SQL but dictionary was provided
            return sql, tuple(validated_params.values())

        # For any other case, return validated params
        return sql, (validated_params,) if not isinstance(validated_params, (list, tuple)) else tuple(validated_params)  # type: ignore[unreachable]

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []  # psqlpy expects a list/tuple

        results: QueryResult = await connection.fetch(sql, parameters=parameters)

        # Convert to dicts and use ResultConverter
        dict_results = results.result()
        return self.to_schema(dict_results, schema_type=schema_type)

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)

        # Convert to dict and use ResultConverter
        dict_results = result.result()
        if not dict_results:
            self.check_not_found(None)

        return self.to_schema(dict_results[0], schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)
        dict_results = result.result()

        if not dict_results:
            return None

        return self.to_schema(dict_results[0], schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []

        value = await connection.fetch_val(sql, parameters=parameters)
        value = self.check_not_found(value)

        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []
        try:
            value = await connection.fetch_val(sql, parameters=parameters)
        except RustPSQLDriverPyBaseError:
            return None

        if value is None:
            return None
        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
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
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        # For INSERT/UPDATE/DELETE, psqlpy returns an empty list but the operation succeeded
        # if no error was raised
        return 1

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: StatementFilter,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
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
        parameters = parameters or []

        result = await connection.execute(sql, parameters=parameters)
        dict_results = result.result()

        if not dict_results:
            self.check_not_found(None)

        return self.to_schema(dict_results[0], schema_type=schema_type)

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[PsqlpyConnection]" = None,
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
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        return "Script executed successfully"

    def _connection(self, connection: "Optional[PsqlpyConnection]" = None) -> "PsqlpyConnection":
        """Get the connection to use.

        Args:
            connection: Optional connection to use. If not provided, use the default connection.

        Returns:
            The connection to use.
        """
        return connection or self.connection
