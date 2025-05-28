import logging
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot import exp

from sqlspec.base import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.filters import StatementFilter
from sqlspec.mixins import (
    AsyncArrowBulkOperationsMixin,
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowBulkOperationsMixin,
)
from sqlspec.sql.statement import SQLStatement
from sqlspec.typing import ArrowTable, Statement, StatementParameterType, T

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Mapping, Sequence

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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], dict[str, Any]]]]":
        """Process SQL and parameters for oracledb.

        Leverages SQLStatement to parse SQL, validate parameters, and obtain a
        sqlglot AST. If original parameters were a dictionary, :name style is used.
        Otherwise, the AST is transformed to use Oracle positional placeholders
        (e.g., :1, :2), and an ordered tuple of parameters is returned.

        Args:
            sql: The SQL statement or sqlglot Expression to process.
            parameters: The parameters to bind. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments for SQLStatement.

        Returns:
            A tuple of (sql_string, parameters_for_oracledb).
        """
        data_params_for_statement: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None
        combined_filters_list: list[StatementFilter] = list(filters)

        if parameters is not None:
            if isinstance(parameters, StatementFilter):
                combined_filters_list.insert(0, parameters)
            else:
                data_params_for_statement = parameters

        statement = SQLStatement(sql, data_params_for_statement, kwargs=kwargs, dialect=self.dialect)

        for filter_obj in combined_filters_list:
            statement = statement.apply_filter(filter_obj)

        parsed_expr, final_ordered_params, placeholder_nodes_in_order = statement.process()

        params_for_oracledb: Optional[Union[tuple[Any, ...], dict[str, Any]]] = None
        final_sql: str

        if not placeholder_nodes_in_order:
            final_sql = parsed_expr.sql(dialect=self.dialect)
            params_for_oracledb = None
        elif isinstance(statement._merged_parameters, dict):  # noqa: SLF001
            # Original params were a dict, use :name style (sqlglot default for Oracle with named nodes)
            final_sql = parsed_expr.sql(dialect=self.dialect)
            params_for_oracledb = statement._merged_parameters  # noqa: SLF001
        else:
            # Original params were a sequence/scalar, transform to :N style for Oracle
            placeholder_map: dict[int, exp.Expression] = {
                # Oracle uses :1, :2 for positional. sqlglot.exp.Placeholder(this=str(idx))
                # should render correctly for the 'oracle' dialect.
                id(p_node): exp.Placeholder(this=str(i + 1))
                for i, p_node in enumerate(placeholder_nodes_in_order)
            }

            def replace_with_oracle_positional(node: exp.Expression) -> exp.Expression:
                return placeholder_map.get(id(node), node)

            transformed_expr = parsed_expr.transform(replace_with_oracle_positional, copy=True)
            final_sql = transformed_expr.sql(dialect=self.dialect)
            params_for_oracledb = final_ordered_params

        # Safeguards
        if params_for_oracledb is None and placeholder_nodes_in_order:
            logger.warning("oracledb: SQL expects parameters, but they resolved to None. SQL: %s", final_sql)
            params_for_oracledb = ()
        elif isinstance(params_for_oracledb, dict) and not params_for_oracledb and placeholder_nodes_in_order:
            logger.warning(
                "oracledb: SQL may expect named parameters (:name), but an empty dictionary is being passed. SQL: %s",
                final_sql,
            )
        elif isinstance(params_for_oracledb, tuple) and not params_for_oracledb and placeholder_nodes_in_order:
            logger.warning(
                "oracledb: SQL may expect positional parameters (:N), but an empty sequence is being passed. SQL: %s",
                final_sql,
            )

        return final_sql, params_for_oracledb


class OracleSyncDriver(
    OracleDriverBase,
    SyncArrowBulkOperationsMixin["OracleSyncConnection"],
    SQLTranslatorMixin["OracleSyncConnection"],
    SyncDriverAdapterProtocol["OracleSyncConnection"],
    ResultConverter,
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            results = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names from description
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None

            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownArgumentType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
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
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[OracleSyncConnection]" = None,
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

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleSyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        results = connection.fetch_df_all(sql, parameters)
        return cast("ArrowTable", ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names()))  # pyright: ignore

    def _connection(self, connection: "Optional[OracleSyncConnection]" = None) -> "OracleSyncConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection


class OracleAsyncDriver(
    OracleDriverBase,
    AsyncArrowBulkOperationsMixin["OracleAsyncConnection"],
    SQLTranslatorMixin["OracleAsyncConnection"],
    AsyncDriverAdapterProtocol["OracleAsyncConnection"],
    ResultConverter,
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
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

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = await cursor.fetchall()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not results:
                return []
            # Get column names from description
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
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

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownArgumentType]
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
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

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
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
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            sql: The SQL statement to execute.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Args:
            sql: The SQL statement with RETURNING clause.
            parameters: The parameters for the statement (dict, tuple, list, or None).
            *filters: Statement filters to apply.
            connection: Optional connection override.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            The returned row data, as either a model instance or dictionary.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = await cursor.fetchone()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if result is None:
                return None

            # Get column names
            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return cast("ModelDTOT", schema_type(**dict(zip(column_names, result))))  # pyright: ignore[reportUnknownArgumentType]
            return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    async def execute_script(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        connection: "Optional[OracleAsyncConnection]" = None,
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

        async with self._with_cursor(connection) as cursor:
            await cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return str(cursor.rowcount)  # pyright: ignore[reportUnknownMemberType]

    async def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[OracleAsyncConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownVariableType]
        """Execute a SQL query asynchronously and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            filters: Statement filters to apply.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """

        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        results = await connection.fetch_df_all(sql, parameters)
        return ArrowTable.from_arrays(arrays=results.column_arrays(), names=results.column_names())  # pyright: ignore

    def _connection(self, connection: "Optional[OracleAsyncConnection]" = None) -> "OracleAsyncConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
