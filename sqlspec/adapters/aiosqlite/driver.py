import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import aiosqlite
import sqlglot
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin
from sqlspec.statement import SQLStatement

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AiosqliteConnection", "AiosqliteDriver")
AiosqliteConnection = aiosqlite.Connection

logger = logging.getLogger("sqlspec")


class AiosqliteDriver(
    SQLTranslatorMixin["AiosqliteConnection"],
    AsyncDriverAdapterProtocol["AiosqliteConnection"],
):
    """SQLite Async Driver Adapter."""

    connection: "AiosqliteConnection"
    dialect: str = "sqlite"

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection

    @staticmethod
    async def _cursor(connection: "AiosqliteConnection", *args: Any, **kwargs: Any) -> "aiosqlite.Cursor":
        return await connection.cursor(*args, **kwargs)

    @asynccontextmanager
    async def _with_cursor(self, connection: "AiosqliteConnection") -> "AsyncGenerator[aiosqlite.Cursor, None]":
        cursor = await self._cursor(connection)
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
        # 1. Use SQLStatement for initial processing and validation
        stmt = SQLStatement(sql, parameters, kwargs=kwargs)
        original_sql, merged_params = stmt.process()  # `process` returns original_sql and merged_params

        if merged_params is None:
            return original_sql, None

        try:
            parsed_expression = sqlglot.parse_one(original_sql, read=self.dialect)
        except Exception as e:
            msg = f"aiosqlite: Failed to re-parse SQL with sqlglot after SQLStatement validation: {e}. SQL: {original_sql}"
            raise SQLParsingError(msg) from e

        final_sql: str = original_sql
        final_params_tuple: Optional[tuple[Any, ...]] = None

        if isinstance(merged_params, dict):
            actual_named_placeholders_in_ast: dict[str, Union[exp.Parameter, exp.Placeholder]] = {}
            has_true_qmark_placeholders = False

            for node in parsed_expression.find_all(exp.Placeholder, exp.Parameter):
                if isinstance(node, exp.Parameter) and node.name and not node.name.isdigit():
                    actual_named_placeholders_in_ast[node.name] = node
                elif isinstance(node, exp.Placeholder) and node.this and not node.this.isdigit():  # :name
                    actual_named_placeholders_in_ast[node.this] = node
                elif isinstance(node, exp.Placeholder) and node.this is None:  # ?
                    has_true_qmark_placeholders = True

            if has_true_qmark_placeholders:
                msg = "aiosqlite: Dictionary parameters provided, but SQL uses '?' placeholders. Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            if not actual_named_placeholders_in_ast and merged_params:
                # If merged_params is not empty and no named placeholders found.
                msg = "aiosqlite: Dictionary parameters provided, but no recognizable named placeholders found in SQL."
                raise ParameterStyleMismatchError(msg)
            if not actual_named_placeholders_in_ast and not merged_params:
                # Empty dict params and no named placeholders, this is fine, but should have been caught by merged_params is None earlier if truly no params.
                # If merged_params is an empty dict, it implies user intended named params but provided none.
                # However, if SQL also has no named params, it could be a query without params.
                # This path should be less common due to the `merged_params is None` check.
                pass  # Allowing empty dict with SQL that has no named params found by sqlglot.

            sql_param_names_in_ast = set(actual_named_placeholders_in_ast.keys())
            provided_keys = set(merged_params.keys())

            missing_keys = sql_param_names_in_ast - provided_keys
            if missing_keys:
                msg = f"aiosqlite: Named parameters {missing_keys} found in SQL but not provided. SQL: {original_sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - sql_param_names_in_ast
            if extra_keys:
                logger.warning(
                    f"aiosqlite: Parameters {extra_keys} provided but not found in SQL. They will be ignored. SQL: {original_sql}"
                )

            ordered_param_values: list[Any] = []

            def _convert_named_to_qmark(node_transform: exp.Expression) -> exp.Expression:
                param_name = None
                if (
                    isinstance(node_transform, exp.Parameter)
                    and node_transform.name
                    and not node_transform.name.isdigit()
                ):
                    param_name = node_transform.name
                elif (
                    isinstance(node_transform, exp.Placeholder)
                    and node_transform.this
                    and not node_transform.this.isdigit()
                ):
                    param_name = node_transform.this

                if param_name and param_name in merged_params:
                    ordered_param_values.append(merged_params[param_name])
                    return exp.Placeholder()  # Represents '?'
                return node_transform

            transformed_expression = parsed_expression.transform(_convert_named_to_qmark, copy=True)
            final_sql = transformed_expression.sql(dialect=self.dialect)
            final_params_tuple = tuple(ordered_param_values)

        elif isinstance(merged_params, (list, tuple)):
            # Logic for sequence parameters (should be largely correct now thanks to SQLStatement updates)
            # SQLStatement already validated that if sequence params are given, SQL doesn't have named placeholders.
            # It also validated counts if it could parse qmarks. The main task here is to ensure dialect-specific SQL.
            sql_placeholder_nodes = list(
                parsed_expression.find_all(exp.Placeholder)
            )  # Re-fetch for this block if needed
            sql_numeric_param_nodes = [  # Simplified numeric detection for sqlite context, primarily expecting ?
                node
                for node in parsed_expression.find_all(exp.Parameter)
                if (node.name and node.name.isdigit())
                or (not node.name and node.this and isinstance(node.this, str) and node.this.isdigit())
            ]
            qmark_count = sum(1 for p_node in sql_placeholder_nodes if p_node.this is None)
            numeric_count = len(sql_numeric_param_nodes)
            # For aiosqlite (sqlite), we primarily expect '?' (qmark). $N is not native.
            # SQLStatement should have validated this. If SQL had $N, it should be an error from SQLStatement if dialect is sqlite.
            # Here, we just ensure final SQL is in '?' form if it wasn't already.

            # This check is somewhat redundant if SQLStatement.process() worked correctly.
            # It should have raised ParameterStyleMismatchError if named params were in SQL with sequence input.
            has_named_placeholders_in_ast = False
            for node in parsed_expression.find_all(exp.Placeholder, exp.Parameter):
                if (isinstance(node, exp.Parameter) and node.name and not node.name.isdigit()) or (
                    isinstance(node, exp.Placeholder) and node.this and not node.this.isdigit()
                ):
                    has_named_placeholders_in_ast = True
                    break
            if has_named_placeholders_in_ast:
                msg = f"aiosqlite: Sequence parameters provided, but SQL unexpectedly contains named placeholders after SQLStatement validation. SQL: {original_sql}"
                raise ParameterStyleMismatchError(msg)

            expected_qmarks = qmark_count + numeric_count  # Should primarily be qmark_count for sqlite
            if expected_qmarks != len(merged_params):
                # This might indicate SQLStatement's regex fallback was used and sqlglot count differs.
                # Or, the SQL is malformed in a way that bypasses SQLStatement's initial validation for sequence params.
                msg = (
                    f"aiosqlite: Parameter count mismatch after SQLStatement validation. "
                    f"SQL (re-parsed) expects {expected_qmarks} positional placeholders, but {len(merged_params)} were provided. SQL: {original_sql}"
                )
                # Check if SQLStatement validation should have caught this. This is more of an internal consistency check.
                logger.warning(msg)  # Log as warning, as SQLStatement might have allowed it based on regex.
                # If we proceed, it might lead to runtime errors from aiosqlite. For now, we proceed with params as is.

            final_sql = parsed_expression.sql(dialect=self.dialect)  # Ensures '?' for sqlite
            final_params_tuple = tuple(merged_params)

        elif merged_params is not None:  # Scalar parameter
            # Similar to sequence, SQLStatement should have validated this.
            # Expecting one 'qmark' placeholder.
            sql_placeholder_nodes = list(parsed_expression.find_all(exp.Placeholder))  # Re-fetch for this block
            sql_numeric_param_nodes = [  # Simplified numeric detection for sqlite context
                node
                for node in parsed_expression.find_all(exp.Parameter)
                if (node.name and node.name.isdigit())
                or (not node.name and node.this and isinstance(node.this, str) and node.this.isdigit())
            ]
            qmark_count = sum(1 for p_node in sql_placeholder_nodes if p_node.this is None)
            numeric_count = len(sql_numeric_param_nodes)

            has_named_placeholders_in_ast = False
            for node in parsed_expression.find_all(exp.Placeholder, exp.Parameter):
                if (isinstance(node, exp.Parameter) and node.name and not node.name.isdigit()) or (
                    isinstance(node, exp.Placeholder) and node.this and not node.this.isdigit()
                ):
                    has_named_placeholders_in_ast = True
                    break
            if has_named_placeholders_in_ast:
                msg = f"aiosqlite: Scalar parameter provided, but SQL unexpectedly contains named placeholders after SQLStatement validation. SQL: {original_sql}"
                raise ParameterStyleMismatchError(msg)

            expected_qmarks = qmark_count + numeric_count
            if expected_qmarks != 1:
                msg = f"aiosqlite: Scalar parameter provided, but SQL (re-parsed) expects {expected_qmarks} positional placeholders (expected 1). SQL: {original_sql}"
                raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params_tuple = (merged_params,)

        return final_sql, final_params_tuple

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        results = await cursor.fetchall()
        if not results:
            return []

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return [dict(zip(column_names, row)) for row in results]
        return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
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
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Get column names
        column_names = [column[0] for column in cursor.description]

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
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        result = self.check_not_found(result)

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        if result is None:
            return None

        # Return first value from the row
        result_value = result[0]
        if schema_type is None:
            return result_value
        return schema_type(result_value)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        await connection.commit()
        return cursor.rowcount

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
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
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the query
        cursor = await connection.execute(sql, parameters or ())
        result = await cursor.fetchone()
        await connection.commit()
        await cursor.close()

        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        if schema_type is None:
            return dict(zip(column_names, result))
        return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AiosqliteConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        # Execute the script
        await connection.executescript(sql)
        await connection.commit()
        return "Script executed successfully."

    def _connection(self, connection: "Optional[AiosqliteConnection]" = None) -> "AiosqliteConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
