import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from duckdb import DuckDBPyConnection

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import ArrowTable, StatementParameterType

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("DuckDBConnection", "DuckDBDriver")

logger = logging.getLogger("sqlspec")

DuckDBConnection = DuckDBPyConnection


class DuckDBDriver(
    SyncArrowBulkOperationsMixin["DuckDBConnection"],
    SQLTranslatorMixin["DuckDBConnection"],
    SyncDriverAdapterProtocol["DuckDBConnection"],
    ResultConverter,
):
    """DuckDB Sync Driver Adapter."""

    connection: "DuckDBConnection"
    use_cursor: bool = True
    dialect: str = "duckdb"

    def __init__(self, connection: "DuckDBConnection", use_cursor: bool = True) -> None:
        self.connection = connection
        self.use_cursor = use_cursor

    def _cursor(self, connection: "DuckDBConnection") -> "DuckDBConnection":
        if self.use_cursor:
            return connection.cursor()
        return connection

    @contextmanager
    def _with_cursor(self, connection: "DuckDBConnection") -> "Generator[DuckDBConnection, None, None]":
        if self.use_cursor:
            cursor = self._cursor(connection)
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            yield connection

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for DuckDB using SQLStatement.

        DuckDB supports both named (:name, $name) and positional (?) parameters.
        This method processes the SQL with dialect-aware parsing and handles
        parameters appropriately for DuckDB.

        Args:
            sql: SQL statement.
            parameters: Query parameters.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            Tuple of processed SQL and parameters.
        """
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        # Apply any filters
        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, _ = statement.process()
        if processed_params is None:
            return processed_sql, None
        if isinstance(processed_params, dict):
            return processed_sql, processed_params
        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)
        return processed_sql, (processed_params,)  # type: ignore[unreachable]

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
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
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            results = cursor.fetchall()
            if not results:
                return []
            column_names = [column[0] for column in cursor.description or []]

            # Convert to dicts first
            dict_results = [dict(zip(column_names, row)) for row in results]
            return self.to_schema(dict_results, schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
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
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
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
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            column_names = [column[0] for column in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result))
            return self.to_schema(dict_result, schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            result = self.check_not_found(result)
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, [] if parameters is None else parameters)
            result = cursor.fetchone()
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> int:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return getattr(cursor, "rowcount", -1)

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[DuckDBConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            result = cursor.fetchall()
            result = self.check_not_found(result)
            column_names = [col[0] for col in cursor.description or []]

            # Convert to dict and use ResultConverter
            dict_result = dict(zip(column_names, result[0]))
            return self.to_schema(dict_result, schema_type=schema_type)

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> str:
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("str", getattr(cursor, "statusmessage", "DONE"))

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        connection = self._connection(connection)

        # Extract filters from kwargs if present
        filters = kwargs.pop("filters", [])

        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)
        with self._with_cursor(connection) as cursor:
            params = [] if parameters is None else parameters
            cursor.execute(sql, params)
            return cast("ArrowTable", cursor.fetch_arrow_table())

    def _connection(self, connection: "Optional[DuckDBConnection]" = None) -> "DuckDBConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
