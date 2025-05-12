import logging
import re
from re import Match
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from asyncpg import Connection
from sqlglot import exp
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement import SQLStatement

if TYPE_CHECKING:
    from collections.abc import Sequence

    from asyncpg import Record
    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[Connection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = "Union[Connection, PoolConnectionProxy]"

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE) \d+ (\d+)$")

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


class AsyncpgDriver(
    SQLTranslatorMixin["AsyncpgConnection"],
    AsyncDriverAdapterProtocol["AsyncpgConnection"],
    ResultConverter,
):
    """AsyncPG Postgres Driver Adapter."""

    connection: "AsyncpgConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for AsyncPG using SQLStatement.

        This method applies filters (if provided), processes the SQL through SQLStatement
        with dialect support, and converts parameters to the format required by AsyncPG.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            Tuple of processed SQL and parameters.
        """
        # Handle scalar parameter by converting to a single-item tuple
        if parameters is not None and not isinstance(parameters, (list, tuple, dict)):
            parameters = (parameters,)

        # Create a SQLStatement with PostgreSQL dialect
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        # Process the statement
        processed_sql, processed_params, parsed_expr = statement.process()

        if processed_params is None:
            return processed_sql, ()

        # Convert question marks to PostgreSQL style $N parameters
        if isinstance(processed_params, (list, tuple)) and "?" in processed_sql:
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

            processed_sql = QUESTION_MARK_PATTERN.sub(replace_question_mark, processed_sql)

        # Now handle the asyncpg-specific parameter conversion - asyncpg requires positional parameters
        if isinstance(processed_params, dict):
            if parsed_expr is not None:
                # Find named parameters
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

                # Convert named parameters to positional
                if named_params:
                    # Transform the SQL to use $1, $2, etc.
                    def replace_named_with_positional(node: exp.Expression) -> exp.Expression:
                        if isinstance(node, exp.Parameter) and node.name and node.name in processed_params:
                            idx = named_params.index(node.name) + 1
                            return exp.Parameter(this=str(idx))
                        if (
                            isinstance(node, exp.Placeholder)
                            and isinstance(node.this, str)
                            and node.this in processed_params
                        ):
                            idx = named_params.index(node.this) + 1
                            return exp.Parameter(this=str(idx))
                        return node

                    return parsed_expr.transform(replace_named_with_positional, copy=True).sql(
                        dialect=self.dialect
                    ), tuple(processed_params[name] for name in named_params)
            return processed_sql, tuple(processed_params.values())
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)
        return processed_sql, (processed_params,)  # type: ignore[unreachable]

    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()

        results = await connection.fetch(sql, *parameters)  # pyright: ignore
        if not results:
            return []
        return self.to_schema([dict(row.items()) for row in results], schema_type=schema_type)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        result = self.check_not_found(result)
        return self.to_schema(dict(result.items()), schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        return self.to_schema(dict(result.items()), schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore
        result = self.check_not_found(result)
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchval(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: Optional["AsyncpgConnection"] = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.execute(sql, *parameters)  # pyright: ignore
        # asyncpg returns e.g. 'INSERT 0 1', 'UPDATE 0 2', etc.
        match = ROWCOUNT_REGEX.match(result)
        if match:
            return int(match.group(1))
        return 0

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
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
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return the affected row.

        Args:
            *filters: Statement filters to apply.
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The affected row data as either a model instance or dictionary.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None

        return self.to_schema(dict(result.items()), schema_type=schema_type)

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[AsyncpgConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else ()
        return await connection.execute(sql, *parameters)  # pyright: ignore

    def _connection(self, connection: "Optional[AsyncpgConnection]" = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection
