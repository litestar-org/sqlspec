import logging
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

import sqlglot
from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import AsyncArrowBulkOperationsMixin, SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.typing import ArrowTable, StatementParameterType, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Sequence

    from sqlspec.typing import ModelDTOT

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = logging.getLogger("sqlspec")


class OracleDriverBase:
    """Base class for Oracle drivers with common functionality."""

    dialect: str = "oracle"

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], dict[str, Any]]]]":
        """Process SQL and parameters for OracleDB.

        OracleDB supports both named (:name) and positional (?, :1, etc.) parameters.
        This method merges parameters, validates them, and ensures SQL is in a
        consistent format for the OracleDB driver using sqlglot.
        """
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...], Any]] = None  # Allow Any for scalar

        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for OracleDB driver."
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
            # self.dialect is "oracle"
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"oracledb: Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        # Traditional named parameters (e.g., :name as Parameter nodes)
        sql_named_param_nodes = [
            node for node in parsed_expression.find_all(exp.Parameter) if node.name and not node.name.isdigit()
        ]

        # Named placeholders parsed as Placeholder nodes (e.g., :name might be here in some dialects)
        named_placeholder_nodes = [
            node
            for node in parsed_expression.find_all(exp.Placeholder)
            if isinstance(node.this, str) and not node.this.isdigit()
        ]

        # Anonymous placeholders (?)
        qmark_placeholder_nodes = [node for node in parsed_expression.find_all(exp.Placeholder) if node.this is None]

        # Oracle positional parameters (:1, :2, etc.)
        numeric_param_nodes = [
            node for node in parsed_expression.find_all(exp.Parameter) if node.name and node.name.isdigit()
        ]

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or named_placeholder_nodes or qmark_placeholder_nodes or numeric_param_nodes:
                placeholder_types = set()
                if sql_named_param_nodes or named_placeholder_nodes:
                    placeholder_types.add("named (e.g., :name)")
                if qmark_placeholder_nodes:
                    placeholder_types.add("qmark ('?')")
                if numeric_param_nodes:
                    placeholder_types.add("positional (:n)")
                msg = (
                    f"oracledb: SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            return sql, None  # OracleDB can handle None

        final_sql: str
        final_params: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None

        if isinstance(merged_params, dict):
            # Dictionary parameters. OracleDB can take dict for :name params.
            if qmark_placeholder_nodes or numeric_param_nodes:
                msg = "oracledb: Dictionary parameters provided, but SQL uses positional placeholders ('?' or :n). Use named placeholders (e.g., :name)."
                raise ParameterStyleMismatchError(msg)

            if not sql_named_param_nodes and not named_placeholder_nodes:
                msg = "oracledb: Dictionary parameters provided, but no named placeholders (e.g., :name) found by sqlglot."
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
                msg = f"oracledb: Named parameters {missing_keys} found in SQL but not provided. SQL: {sql}"
                raise SQLParsingError(msg)

            extra_keys = provided_keys - sql_param_names_in_ast
            if extra_keys:
                logger.warning(
                    f"oracledb: Parameters {extra_keys} provided but not found in SQL. They will be ignored. SQL: {sql}"
                )

            # Generate SQL with oracle dialect for named params
            final_sql = parsed_expression.sql(dialect=self.dialect)
            final_params = merged_params  # OracleDB works with dict directly

        elif isinstance(merged_params, (list, tuple)):
            # Sequence parameters. OracleDB can take tuple where SQL has ? or :n.
            # Ideally transform all to :n style for consistency with oracle dialect.
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "oracledb: Sequence parameters provided, but SQL contains named placeholders. Use positional placeholders ('?' or ':n')."
                raise ParameterStyleMismatchError(msg)

            total_positional = len(qmark_placeholder_nodes) + len(numeric_param_nodes)
            if total_positional != len(merged_params):
                msg = (
                    f"oracledb: Parameter count mismatch. SQL expects {total_positional} "
                    f"positional placeholders, but {len(merged_params)} parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            # Transform '?' to :n if needed for consistency
            counter: list[int] = [0]  # Use list to make counter mutable in nested function

            def _convert_qmark_to_param(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Placeholder) and node.this is None:
                    counter[0] += 1
                    return exp.Parameter(this=exp.Identifier(this=str(counter[0])))
                return node

            if qmark_placeholder_nodes:
                # Transform and regenerate SQL
                transformed_expression = parsed_expression.transform(_convert_qmark_to_param, copy=True)
                final_sql = transformed_expression.sql(dialect=self.dialect)
            else:
                final_sql = parsed_expression.sql(dialect=self.dialect)

            final_params = tuple(merged_params)  # OracleDB expects tuple for positional params

        elif merged_params is not None:  # Scalar parameter
            if sql_named_param_nodes or named_placeholder_nodes:
                msg = "oracledb: Scalar parameter provided, but SQL contains named placeholders. Use a single positional placeholder ('?' or ':1')."
                raise ParameterStyleMismatchError(msg)

            total_positional = len(qmark_placeholder_nodes) + len(numeric_param_nodes)
            if total_positional != 1:
                msg = (
                    f"oracledb: Scalar parameter provided, but SQL expects {total_positional} "
                    f"positional placeholders. Expected 1. SQL: {sql}"
                )
                raise SQLParsingError(msg)

            # Transform '?' to :1 if needed for consistency with oracle dialect
            if qmark_placeholder_nodes:
                transformed_expression = parsed_expression.transform(
                    lambda node: exp.Parameter(this=exp.Identifier(this="1"))
                    if isinstance(node, exp.Placeholder) and node.this is None
                    else node,
                    copy=True,
                )
                final_sql = transformed_expression.sql(dialect=self.dialect)
            else:
                final_sql = parsed_expression.sql(dialect=self.dialect)

            final_params = (merged_params,)  # OracleDB expects tuple for scalar

        else:  # Should be caught by 'merged_params is None' earlier
            final_sql = sql
            final_params = None

        return final_sql, final_params


class OracleSyncDriver(
    OracleDriverBase,
    SyncArrowBulkOperationsMixin["OracleSyncConnection"],
    SQLTranslatorMixin["OracleSyncConnection"],
    SyncDriverAdapterProtocol["OracleSyncConnection"],
):
    """Oracle Sync Driver Adapter."""

    connection: "OracleSyncConnection"

    def __init__(self, connection: "OracleSyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @contextmanager
    def _with_cursor(connection: "OracleSyncConnection") -> "Generator[Cursor, None, None]":
        cursor = connection.cursor()
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
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore

            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType]

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        results = connection.fetch_df_all(sql, parameters)
        return cast("ArrowTable", ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names()))  # pyright: ignore


class OracleAsyncDriver(
    OracleDriverBase,
    AsyncArrowBulkOperationsMixin["OracleAsyncConnection"],
    SQLTranslatorMixin["OracleAsyncConnection"],
    AsyncDriverAdapterProtocol["OracleAsyncConnection"],
):
    """Oracle Async Driver Adapter."""

    connection: "OracleAsyncConnection"

    def __init__(self, connection: "OracleAsyncConnection") -> None:
        self.connection = connection

    @staticmethod
    @asynccontextmanager
    async def _with_cursor(connection: "OracleAsyncConnection") -> "AsyncGenerator[AsyncCursor, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # --- Public API Methods --- #
    @overload
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = await cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore

            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore

    @overload
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]
            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType]

    @overload
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
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
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            # Always return dictionaries
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    async def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query asynchronously and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        results = await connection.fetch_df_all(sql, parameters)
        return ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names())  # pyright: ignore
