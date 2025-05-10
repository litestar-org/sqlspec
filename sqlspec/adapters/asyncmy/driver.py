# type: ignore
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from asyncmy import Connection
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncmyDriver",)

AsyncmyConnection = Connection

logger = logging.getLogger("sqlspec")


class AsyncmyDriver(
    SQLTranslatorMixin["AsyncmyConnection"],
    AsyncDriverAdapterProtocol["AsyncmyConnection"],
):
    """Asyncmy MySQL/MariaDB Driver Adapter."""

    connection: "AsyncmyConnection"
    dialect: str = "mysql"

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "AsyncmyConnection") -> AsyncGenerator["Cursor", None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...], Any]] = None

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for asyncmy driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters  # type: ignore[assignment]
        # else merged_params remains None

        # Check if the SQL contains MySQL format placeholders (%s)
        # If so, we'll use a simpler approach rather than SQLGlot parsing
        # as SQLGlot doesn't correctly handle %s parameters (treats them as modulo operations)
        format_placeholders_count = sql.count("%s")

        if format_placeholders_count > 0:
            # Simple MySQL format placeholder handling
            if merged_params is None:
                if format_placeholders_count > 0:
                    msg = f"asyncmy: SQL statement contains {format_placeholders_count} format placeholders ('%s'), but no parameters were provided. SQL: {sql}"
                    raise SQLParsingError(msg)
                return sql, None

            if isinstance(merged_params, (list, tuple)):
                if len(merged_params) != format_placeholders_count:
                    msg = f"asyncmy: Parameter count mismatch. SQL expects {format_placeholders_count} '%s' placeholders, but {len(merged_params)} parameters were provided. SQL: {sql}"
                    raise SQLParsingError(msg)
                # MySQL/asyncmy uses %s for format placeholders, so we can just pass the SQL as-is
                return sql, tuple(merged_params)

            if isinstance(merged_params, dict):
                msg = "asyncmy: Dictionary parameters provided with '%s' placeholders. MySQL format placeholders require tuple/list parameters."
                raise ParameterStyleMismatchError(msg)

            # Scalar parameter
            if format_placeholders_count != 1:
                msg = f"asyncmy: Scalar parameter provided, but SQL expects {format_placeholders_count} '%s' placeholders. Expected 1. SQL: {sql}"
                raise SQLParsingError(msg)
            return sql, (merged_params,)

        # Continue with SQLGlot parsing for non-%s cases (like ? or named parameters)
        try:
            # self.dialect is "mysql"
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            # If parsing fails but we have %s placeholders, it might be due to SQLGlot not handling %s correctly
            # In this case, use the simple approach for format placeholders
            if format_placeholders_count > 0:
                if merged_params is None:
                    return sql, None

                if isinstance(merged_params, (list, tuple)):
                    if len(merged_params) != format_placeholders_count:
                        msg = f"asyncmy: Parameter count mismatch. SQL expects {format_placeholders_count} '%s' placeholders, but {len(merged_params)} parameters were provided. SQL: {sql}"
                        raise SQLParsingError(msg) from e
                    return sql, tuple(merged_params)

                if isinstance(merged_params, dict):
                    msg = "asyncmy: Dictionary parameters provided with '%s' placeholders. MySQL format placeholders require tuple/list parameters."
                    raise ParameterStyleMismatchError(msg) from e

                # Scalar parameter
                if format_placeholders_count != 1:
                    msg = f"asyncmy: Scalar parameter provided, but SQL expects {format_placeholders_count} '%s' placeholders. Expected 1. SQL: {sql}"
                    raise SQLParsingError(msg) from e
                return sql, (merged_params,)
            # If no %s placeholders, then it's a genuine SQLGlot parsing error
            msg = f"asyncmy: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # From here, we're handling named parameters or ? placeholders only
        # exp.Parameter with .name for :name, @name, etc.
        # exp.Placeholder for '?'.
        sql_named_param_nodes = [node for node in parsed_expression.find_all(exp.Parameter) if node.name]
        sql_placeholder_nodes = list(parsed_expression.find_all(exp.Placeholder))

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or sql_placeholder_nodes:
                placeholder_types = set()
                if sql_named_param_nodes:
                    placeholder_types.add("named (e.g., :name)")
                if sql_placeholder_nodes:
                    placeholder_types.add("positional ('?')")
                msg = (
                    f"asyncmy: SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            return sql, None  # asyncmy can take None

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None
        # asyncmy execute takes `args: Any | tuple[Any, ...] | list[Any] | dict[str, Any] | None`
        # We will aim to return a tuple for sequence/scalar, or dict for named (if driver supports it natively and sql matches)
        # However, for consistency and to ensure broad compatibility if asyncmy prefers one over other internally for qmark, let's convert named to qmark/tuple.

        if isinstance(merged_params, dict):
            # Dictionary parameters. Convert to qmark style for asyncmy.
            if sql_placeholder_nodes:
                msg = "asyncmy: Dictionary parameters provided, but SQL uses positional placeholders ('?'). Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes:
                msg = "asyncmy: Dictionary parameters provided, but no named placeholders (e.g., :name) found by sqlglot to convert to '?'."
                raise ParameterStyleMismatchError(msg)

            ordered_param_values: list[Any] = []
            sql_param_names_in_ast = {node.name for node in sql_named_param_nodes if node.name}
            provided_keys = set(merged_params.keys())

            missing_keys = sql_param_names_in_ast - provided_keys
            if missing_keys:
                msg = f"asyncmy: Named parameters {missing_keys} found in SQL but not provided. SQL: {sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - sql_param_names_in_ast
            if extra_keys:
                logger.warning(
                    f"asyncmy: Parameters {extra_keys} provided but not found in SQL. They will be ignored during qmark conversion. SQL: {sql}"
                )

            def _convert_named_to_qmark(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Parameter) and node.name:
                    param_name = node.name
                    if param_name in merged_params:  # type: ignore[operator]
                        ordered_param_values.append(merged_params[param_name])  # type: ignore[index]
                        return exp.Placeholder()  # Represents '?' for MySQL dialect generation
                return node

            transformed_expression = parsed_expression.transform(_convert_named_to_qmark, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)  # MySQL dialect makes '?'
            final_params = tuple(ordered_param_values)

        elif isinstance(merged_params, (list, tuple)):
            # Sequence parameters. SQL should use '?'
            if sql_named_param_nodes:
                msg = "asyncmy: Sequence parameters provided, but SQL contains named placeholders. Use '?'."
                raise ParameterStyleMismatchError(msg)

            expected_param_count = len(sql_placeholder_nodes)
            if expected_param_count != len(merged_params):
                msg = (
                    f"asyncmy: Parameter count mismatch. SQL expects {expected_param_count} positional placeholders ('?'), "
                    f"but {len(merged_params)} were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = tuple(merged_params)

        elif merged_params is not None:  # Scalar parameter
            if sql_named_param_nodes:
                msg = "asyncmy: Scalar parameter provided, but SQL uses named placeholders. Use a single '?'."
                raise ParameterStyleMismatchError(msg)

            expected_param_count = len(sql_placeholder_nodes)
            if expected_param_count != 1:
                msg = (
                    f"asyncmy: Scalar parameter provided, but SQL expects {expected_param_count} positional placeholders. "
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
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            results = await cursor.fetchall()
            if not results:
                return []
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return [dict(zip(column_names, row)) for row in results]
            return [schema_type(**dict(zip(column_names, row))) for row in results]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is None:
                return dict(zip(column_names, result))
            return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            result = self.check_not_found(result)
            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            value = result[0]
            if schema_type is not None:
                return schema_type(value)  # type: ignore[call-arg]
            return value

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
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
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            result = await cursor.fetchone()
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))
            return dict(zip(column_names, result))

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AsyncmyConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return f"Script executed successfully. Rows affected: {cursor.rowcount}"

    def _connection(self, connection: "Optional[AsyncmyConnection]" = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
