import logging
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from psycopg import AsyncConnection, Connection
from psycopg.rows import dict_row
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Sequence

    from sqlspec.typing import ModelDTOT, StatementParameterType, T

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection
PsycopgAsyncConnection = AsyncConnection


class PsycopgDriverBase:
    dialect: str

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
                msg = "Cannot mix positional parameters with keyword arguments for psycopg driver."
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
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"psycopg: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
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

        # Numeric dollar params (e.g., $1, $2)
        sql_numeric_dollar_nodes = [
            node
            for node in parsed_expression.find_all(exp.Parameter)
            if not node.name and node.this and isinstance(node.this, str) and node.this.isdigit()
        ]

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or named_placeholder_nodes or qmark_placeholder_nodes or sql_numeric_dollar_nodes:
                placeholder_types = set()
                if sql_named_param_nodes or named_placeholder_nodes:
                    placeholder_types.add("named (e.g., :name)")
                if qmark_placeholder_nodes:
                    placeholder_types.add("qmark ('?')")
                if sql_numeric_dollar_nodes:
                    placeholder_types.add("numeric ($n)")
                msg = (
                    f"psycopg: SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            return sql, None  # psycopg can take None for params

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None

        if isinstance(merged_params, dict):
            # Dictionary parameters. Aim for %(name)s style for psycopg.
            if qmark_placeholder_nodes or sql_numeric_dollar_nodes:
                msg = "psycopg: Dictionary parameters provided, but SQL uses positional placeholders ('?' or $n). Use named placeholders for dictionary params (e.g. :name, which will be converted to %(name)s)."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes and not named_placeholder_nodes and "%(" not in sql:
                msg = "psycopg: Dictionary parameters provided, but no standard named placeholders (e.g., :name) found to convert to %(name)s, and SQL does not appear to be pyformat already."
                raise ParameterStyleMismatchError(msg)

            if sql_named_param_nodes or named_placeholder_nodes:
                # Collect parameter names from both types of nodes
                sql_param_names_in_ast = set()

                # Get names from Parameter nodes
                sql_param_names_in_ast.update(node.name for node in sql_named_param_nodes if node.name)

                # Get names from Placeholder nodes
                sql_param_names_in_ast.update(
                    node.this for node in named_placeholder_nodes if isinstance(node.this, str)
                )

                provided_keys = set(merged_params.keys())

                missing_keys = sql_param_names_in_ast - provided_keys
                if missing_keys:
                    msg = f"psycopg: Named parameters {missing_keys} (from :name style) found in SQL but not provided. SQL: {sql}"
                    raise SQLParsingError(msg)

            try:
                final_sql = parsed_expression.sql(dialect=self.dialect, pyformat=True)
            except Exception as e:
                logger.exception("psycopg: Failed to generate pyformat SQL with sqlglot: %s. SQL: %s", e, sql)
                if "%(" in sql:
                    final_sql = sql
                else:
                    msg = f"psycopg: Error generating pyformat SQL: {e}"
                    raise SQLParsingError(msg) from e

            final_params = merged_params

        elif isinstance(merged_params, (list, tuple)):
            # Sequence parameters. Aim for %s style.
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "psycopg: Sequence parameters provided, but SQL contains named placeholders. Use '?' or '%s' for sequence parameters."
                raise ParameterStyleMismatchError(msg)
            if sql_numeric_dollar_nodes:
                msg = "psycopg: Sequence parameters provided, but SQL uses $n style. Use '?' or '%s' for psycopg."
                raise ParameterStyleMismatchError(msg)

            expected_param_count = len(qmark_placeholder_nodes)
            if expected_param_count == 0 and "%s" in sql and not qmark_placeholder_nodes:
                logger.debug(
                    "psycopg: No '?' found, but '%s' present with sequence params. Assuming intended for '%s' style. Count validation relies on psycopg."
                )
            elif expected_param_count != len(merged_params) and not ("%s" in sql and expected_param_count == 0):
                if not ("%s" in sql and len(merged_params) > 0 and expected_param_count == 0):
                    msg = (
                        f"psycopg: Parameter count mismatch. SQL (based on '?' count) expects "
                        f"{expected_param_count} positional placeholders, but {len(merged_params)} were provided. SQL: {sql}"
                    )
                    raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = tuple(merged_params)

        elif merged_params is not None:  # Scalar parameter
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "psycopg: Scalar parameter provided, but SQL uses named placeholders. Use a single '?' or '%s'."
                raise ParameterStyleMismatchError(msg)
            if sql_numeric_dollar_nodes:
                msg = "psycopg: Scalar parameter provided, but SQL uses $n style. Use '?' or '%s' for psycopg."
                raise ParameterStyleMismatchError(msg)

            expected_param_count = len(qmark_placeholder_nodes)
            if expected_param_count == 0 and "%s" in sql and not qmark_placeholder_nodes:
                logger.debug(
                    "psycopg: No '?' for scalar, but '%s' present. Assuming '%s' style. Count relies on psycopg."
                )
            elif expected_param_count != 1 and not ("%s" in sql and expected_param_count == 0):
                if not ("%s" in sql and expected_param_count == 0):  # Avoid error if it might be a single %s
                    msg = (
                        f"psycopg: Scalar parameter provided, but SQL expects {expected_param_count} "
                        f"positional placeholders ('?' or '%s'). Expected 1. SQL: {sql}"
                    )
                    raise SQLParsingError(msg)

            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = (merged_params,)

        else:  # Should be caught by 'merged_params is None' earlier
            final_sql = sql
            final_params = None

        return final_sql, final_params


class PsycopgSyncDriver(
    PsycopgDriverBase,
    SQLTranslatorMixin["PsycopgSyncConnection"],
    SyncDriverAdapterProtocol["PsycopgSyncConnection"],
):
    """Psycopg Sync Driver Adapter."""

    connection: "PsycopgSyncConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "PsycopgSyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "PsycopgSyncConnection") -> "Generator[Any, None, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            cursor.close()

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[PsycopgSyncConnection]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            results = cursor.fetchall()
            if not results:
                return []

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**row)) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [cast("dict[str,Any]", row) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            row = self.check_not_found(row)
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            row = self.check_not_found(row)
            val = next(iter(row.values())) if row else None
            val = self.check_not_found(val)
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            val = next(iter(row.values())) if row else None
            if val is None:
                return None
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Execute an INSERT, UPDATE, or DELETE query and return the number of affected rows.

        Returns:
            The number of rows affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            return getattr(cursor, "rowcount", -1)  # pyright: ignore[reportUnknownMemberType]

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
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
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            result = cursor.fetchone()

            if result is None:
                return None

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgSyncConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            return str(cursor.statusmessage) if cursor.statusmessage is not None else "DONE"


class PsycopgAsyncDriver(
    PsycopgDriverBase,
    SQLTranslatorMixin["PsycopgAsyncConnection"],
    AsyncDriverAdapterProtocol["PsycopgAsyncConnection"],
):
    """Psycopg Async Driver Adapter."""

    connection: "PsycopgAsyncConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "PsycopgAsyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "PsycopgAsyncConnection") -> "AsyncGenerator[Any, None]":
        cursor = connection.cursor(row_factory=dict_row)
        try:
            yield cursor
        finally:
            await cursor.close()

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        results: list[Union[ModelDTOT, dict[str, Any]]] = []

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            results = await cursor.fetchall()
            if not results:
                return []
            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row))) for row in results]  # pyright: ignore[reportUnknownArgumentType]
            return [cast("dict[str,Any]", row) for row in results]  # pyright: ignore[reportUnknownArgumentType]

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            row = self.check_not_found(row)
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            if row is None:
                return None
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**cast("dict[str,Any]", row)))
            return cast("dict[str,Any]", row)

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            row = self.check_not_found(row)
            val = next(iter(row.values())) if row else None
            val = self.check_not_found(val)
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
            row = await cursor.fetchone()
            if row is None:
                return None
            val = next(iter(row.values())) if row else None
            if val is None:
                return None
            if schema_type is not None:
                return schema_type(val)  # type: ignore[call-arg]
            return val

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Execute an INSERT, UPDATE, or DELETE query and return the number of affected rows.

        Returns:
            The number of rows affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)
            return getattr(cursor, "rowcount", -1)  # pyright: ignore[reportUnknownMemberType]

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
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
            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**result))  # pyright: ignore[reportUnknownArgumentType]
            return cast("dict[str, Any]", result)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[PsycopgAsyncConnection]" = None,
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
            return str(cursor.statusmessage) if cursor.statusmessage is not None else "DONE"
