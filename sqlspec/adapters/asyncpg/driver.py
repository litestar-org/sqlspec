import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from asyncpg import Connection
from sqlglot import exp
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import SQLStatement

if TYPE_CHECKING:
    from collections.abc import Sequence

    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

AsyncpgConnection: TypeAlias = "Union[Connection[Any], PoolConnectionProxy[Any]]"

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE) \d+ (\d+)$")


class AsyncpgDriver(
    SQLTranslatorMixin["AsyncpgConnection"],
    AsyncDriverAdapterProtocol["AsyncpgConnection"],
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
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # First, use SQLStatement for parameter validation
        statement = SQLStatement(sql=sql, parameters=parameters, kwargs=kwargs)
        sql, validated_params = statement.process()

        if validated_params is None:
            return sql, ()  # asyncpg expects an empty tuple for no params

        # Now use SQLGlot for PostgreSQL-specific parameter handling
        try:
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"asyncpg: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # Find different types of parameter nodes in the AST
        # Traditional named parameters (e.g., @name, $name)
        sql_named_param_nodes = [node for node in parsed_expression.find_all(exp.Parameter) if node.name]

        # For PostgreSQL dialect, ':name' gets parsed as Placeholder with this='name'
        colon_named_placeholder_nodes = [
            node
            for node in parsed_expression.find_all(exp.Placeholder)
            if isinstance(node.this, str) and not node.this.isdigit()
        ]

        # Anonymous placeholders (?)
        question_mark_nodes = [node for node in parsed_expression.find_all(exp.Placeholder) if node.this is None]

        # PostgreSQL-specific dollar-numeric parameters like $1, $2
        sql_numeric_dollar_nodes = [
            node
            for node in parsed_expression.find_all(exp.Parameter)
            if not node.name and node.this and isinstance(node.this, str) and node.this.isdigit()
        ]

        final_params_seq = []

        if isinstance(validated_params, dict):
            # Dictionary parameters - convert named parameters to PostgreSQL style ($n)
            if question_mark_nodes or sql_numeric_dollar_nodes:
                msg = "asyncpg: Dictionary parameters provided, but SQL uses positional placeholders. Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            # With PostgreSQL dialect, :name is parsed as Placeholder, not Parameter
            if not sql_named_param_nodes and not colon_named_placeholder_nodes:
                msg = "asyncpg: Dictionary parameters provided, but no named placeholders found in SQL by sqlglot."
                raise ParameterStyleMismatchError(msg)

            # Transform both types of named parameters to $n placeholders
            def _convert_named_to_dollar(node: exp.Expression) -> exp.Expression:
                param_name = None
                if isinstance(node, exp.Parameter) and node.name:
                    param_name = node.name
                elif isinstance(node, exp.Placeholder) and isinstance(node.this, str) and not node.this.isdigit():
                    param_name = node.this

                if param_name and param_name in validated_params:
                    final_params_seq.append(validated_params[param_name])
                    # Create a Parameter node that PostgreSQL will interpret as a positional parameter ($n)
                    # The position is determined by the order in final_params_seq
                    return exp.Parameter(this=str(len(final_params_seq)))
                return node

            transformed_expression = parsed_expression.transform(_convert_named_to_dollar, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)

        elif hasattr(validated_params, "__iter__") and not isinstance(validated_params, (str, bytes, dict)):
            # Sequence parameters (list or tuple)
            if sql_named_param_nodes or colon_named_placeholder_nodes:
                msg = (
                    "asyncpg: Sequence parameters provided, but SQL contains named placeholders. Use '?' placeholders."
                )
                raise ParameterStyleMismatchError(msg)

            # Count placeholders for validation
            total_placeholders = len(question_mark_nodes) + len(sql_numeric_dollar_nodes)
            if total_placeholders != len(validated_params):
                msg = (
                    f"asyncpg: Parameter count mismatch. SQL expects {total_placeholders} placeholders, "
                    f"but {len(validated_params)} parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            # For PostgreSQL, convert ? to $n
            def _convert_qmark_to_dollar(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Placeholder) and node.this is None:
                    position = len(final_params_seq) + 1  # PostgreSQL positions are 1-indexed
                    final_params_seq.append(validated_params[position - 1])
                    return exp.Parameter(this=str(position))
                return node

            transformed_expression = parsed_expression.transform(_convert_qmark_to_dollar, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)

            # If we didn't add any parameters via transformation (e.g., SQL already uses $n style),
            # just add all parameters in order
            if not final_params_seq and validated_params:
                final_params_seq.extend(validated_params)

        else:  # Scalar parameter
            if sql_named_param_nodes or colon_named_placeholder_nodes:
                msg = "asyncpg: Scalar parameter provided, but SQL contains named placeholders. Use a '?' placeholder."
                raise ParameterStyleMismatchError(msg)

            total_placeholders = len(question_mark_nodes) + len(sql_numeric_dollar_nodes)
            if total_placeholders != 1:
                msg = (
                    f"asyncpg: Scalar parameter provided, but SQL expects {total_placeholders} placeholders. "
                    f"Expected exactly 1. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            # Convert single ? to $1
            def _convert_single_qmark_to_dollar(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Placeholder) and node.this is None:
                    final_params_seq.append(validated_params)
                    return exp.Parameter(this="1")
                return node

            transformed_expression = parsed_expression.transform(_convert_single_qmark_to_dollar, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)

            # If we didn't add the parameter via transformation (e.g., SQL already uses $1),
            # add it directly
            if not final_params_seq:
                final_params_seq.append(validated_params)

        return final_sql, tuple(final_params_seq)

    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else ()

        results = await connection.fetch(sql, *parameters)  # pyright: ignore
        if not results:
            return []
        if schema_type is None:
            return [dict(row.items()) for row in results]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return [cast("ModelDTOT", schema_type(**dict(row.items()))) for row in results]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        result = self.check_not_found(result)

        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())  # type: ignore[attr-defined]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # type: ignore[attr-defined]

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            # Always return as dictionary
            return dict(result.items())
        return cast("ModelDTOT", schema_type(**dict(result.items())))

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AsyncpgConnection"] = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
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
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return the affected row.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The affected row data as either a model instance or dictionary.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters if parameters is not None else ()
        result = await connection.fetchrow(sql, *parameters)  # pyright: ignore
        if result is None:
            return None
        if schema_type is None:
            return dict(result.items())  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return cast("ModelDTOT", schema_type(**dict(result.items())))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType, reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
