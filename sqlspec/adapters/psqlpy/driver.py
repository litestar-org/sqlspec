# ruff: noqa: PLR0915
"""Psqlpy Driver Implementation."""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from psqlpy import Connection, QueryResult
from psqlpy.exceptions import RustPSQLDriverPyBaseError
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import SQLStatement

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psqlpy import QueryResult

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("PsqlpyConnection", "PsqlpyDriver")


PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    SQLTranslatorMixin["PsqlpyConnection"],
    AsyncDriverAdapterProtocol["PsqlpyConnection"],
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
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], dict[str, Any]]]]":
        """Process SQL and parameters for psqlpy.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            **kwargs: Additional keyword arguments.

        Returns:
            The SQL statement and parameters.

        Raises:
            ParameterStyleMismatchError: If the parameter style doesn't match the SQL statement.
            SQLParsingError: If the SQL parsing fails.
        """
        # First, use SQLStatement for parameter validation
        statement = SQLStatement(sql=sql, parameters=parameters, kwargs=kwargs)
        sql, validated_params = statement.process()

        if validated_params is None:
            return sql, None  # psqlpy can handle None

        # Now use SQLGlot for PostgreSQL-specific parameter handling
        try:
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"psqlpy: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # Traditional named parameters (e.g., @name, $name)
        sql_named_param_nodes = [
            node for node in parsed_expression.find_all(exp.Parameter) if node.name and not node.name.isdigit()
        ]

        # Named placeholders (e.g., :name which are parsed as Placeholder with this="name")
        named_placeholder_nodes = [
            node
            for node in parsed_expression.find_all(exp.Placeholder)
            if isinstance(node.this, str) and not node.this.isdigit()
        ]

        # Anonymous placeholders (?)
        qmark_placeholder_nodes = [node for node in parsed_expression.find_all(exp.Placeholder) if node.this is None]

        # PostgreSQL-specific dollar-numeric parameters like $1, $2
        sql_numeric_dollar_nodes = [
            node
            for node in parsed_expression.find_all(exp.Parameter)
            if not node.name and node.this and isinstance(node.this, str) and node.this.isdigit()
        ]

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None

        if isinstance(validated_params, dict):
            # Dictionary parameters - need to convert named parameters from :name to $n style
            if qmark_placeholder_nodes or sql_numeric_dollar_nodes:
                msg = "psqlpy: Dictionary parameters provided, but SQL uses positional placeholders. Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes and not named_placeholder_nodes:
                msg = "psqlpy: Dictionary parameters provided, but no named placeholders found in SQL by sqlglot."
                raise ParameterStyleMismatchError(msg)

            # Transform named parameters to $n placeholders for psqlpy
            param_seq = []

            def _convert_named_to_dollar(node: exp.Expression) -> exp.Expression:
                param_name = None
                if isinstance(node, exp.Parameter) and node.name:
                    param_name = node.name
                elif isinstance(node, exp.Placeholder) and isinstance(node.this, str) and not node.this.isdigit():
                    param_name = node.this

                if param_name and param_name in validated_params:
                    param_seq.append(validated_params[param_name])
                    # Create a Parameter node that PostgreSQL will interpret as a positional parameter ($n)
                    return exp.Parameter(this=str(len(param_seq)))
                return node

            transformed_expression = parsed_expression.transform(_convert_named_to_dollar, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)
            final_params = tuple(param_seq)

        elif hasattr(validated_params, "__iter__") and not isinstance(validated_params, (str, bytes, dict)):
            # Sequence parameters (list or tuple)
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "psqlpy: Sequence parameters provided, but SQL contains named placeholders. Use positional placeholders ('?' or '$1')."
                raise ParameterStyleMismatchError(msg)

            # Count placeholders for validation
            total_positional_params = len(qmark_placeholder_nodes) + len(sql_numeric_dollar_nodes)
            if total_positional_params != len(validated_params):
                msg = (
                    f"psqlpy: Parameter count mismatch. SQL expects {total_positional_params} "
                    f"positional placeholders, but {len(validated_params)} parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            param_seq = list(validated_params)

            # Convert question marks to $n parameters
            if qmark_placeholder_nodes:
                counter = [0]  # Make it mutable for the inner function

                def _convert_qmark_to_dollar(node: exp.Expression) -> exp.Expression:
                    if isinstance(node, exp.Placeholder) and node.this is None:
                        counter[0] += 1
                        return exp.Parameter(this=str(counter[0]))
                    return node

                transformed_expression = parsed_expression.transform(_convert_qmark_to_dollar, copy=True)
                final_sql = transformed_expression.sql(dialect=self.dialect)
            else:
                final_sql = parsed_expression.sql(dialect=self.dialect)

            final_params = tuple(param_seq)

        else:  # Scalar parameter (if validated_params is not None and not a sequence)
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "psqlpy: Scalar parameter provided, but SQL contains named placeholders. Use a single positional placeholder ('?' or '$1')."
                raise ParameterStyleMismatchError(msg)

            total_positional_params = len(qmark_placeholder_nodes) + len(sql_numeric_dollar_nodes)
            if total_positional_params != 1:
                msg = (
                    f"psqlpy: Scalar parameter provided, but SQL expects {total_positional_params} "
                    f"positional placeholders. Expected 1. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            # For scalar parameter with question mark, convert to $1
            if qmark_placeholder_nodes:
                transformed_expression = parsed_expression.transform(
                    lambda node: exp.Parameter(this="1")
                    if isinstance(node, exp.Placeholder) and node.this is None
                    else node,
                    copy=True,
                )
                final_sql = transformed_expression.sql(dialect=self.dialect)
            else:
                final_sql = parsed_expression.sql(dialect=self.dialect)

            final_params = (validated_params,)  # psqlpy expects a tuple for $1

        return final_sql, final_params

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []  # psqlpy expects a list/tuple

        results: QueryResult = await connection.fetch(sql, parameters=parameters)

        if schema_type is None:
            return cast("list[dict[str, Any]]", results.result())
        return results.as_class(as_class=schema_type)

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)

        if schema_type is None:
            result = cast("list[dict[str, Any]]", result.result())  # type: ignore[assignment]
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        return result.as_class(as_class=schema_type)[0]

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.fetch(sql, parameters=parameters)
        if schema_type is None:
            result = cast("list[dict[str, Any]]", result.result())  # type: ignore[assignment]
            if len(result) == 0:  # type: ignore[arg-type]
                return None
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        result = cast("list[ModelDTOT]", result.as_class(as_class=schema_type))  # type: ignore[assignment]
        if len(result) == 0:  # type: ignore[arg-type]
            return None
        return cast("ModelDTOT", result[0])  # type: ignore[index]

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        value = await connection.fetch_val(sql, parameters=parameters)

        if schema_type is None:
            return value
        return schema_type(value)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
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
        *,
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
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        result = await connection.execute(sql, parameters=parameters)
        if schema_type is None:
            result = result.result()  # type: ignore[assignment]
            if len(result) == 0:  # type: ignore[arg-type]
                return None
            return cast("dict[str, Any]", result[0])  # type: ignore[index]
        result = result.as_class(as_class=schema_type)  # type: ignore[assignment]
        if len(result) == 0:  # type: ignore[arg-type]
            return None
        return cast("ModelDTOT", result[0])  # type: ignore[index]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsqlpyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        parameters = parameters or []

        await connection.execute(sql, parameters=parameters)
        return sql

    def _connection(self, connection: "Optional[PsqlpyConnection]" = None) -> "PsqlpyConnection":
        """Get the connection to use.

        Args:
            connection: Optional connection to use. If not provided, use the default connection.

        Returns:
            The connection to use.
        """
        return connection or self.connection
