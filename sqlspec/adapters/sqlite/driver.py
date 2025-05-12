import logging
import sqlite3
from contextlib import contextmanager
from sqlite3 import Cursor
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement import SQLStatement
from sqlspec.typing import is_dict

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from sqlspec.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SQLTranslatorMixin["SqliteConnection"],
    SyncDriverAdapterProtocol["SqliteConnection"],
    ResultConverter,
):
    """SQLite Sync Driver Adapter."""

    connection: "SqliteConnection"
    dialect: str = "sqlite"

    def __init__(self, connection: "SqliteConnection") -> None:
        self.connection = connection

    @staticmethod
    def _cursor(connection: "SqliteConnection", *args: Any, **kwargs: Any) -> Cursor:
        return connection.cursor(*args, **kwargs)  # type: ignore[no-any-return]

    @contextmanager
    def _with_cursor(self, connection: "SqliteConnection") -> "Generator[Cursor, None, None]":
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            cursor.close()

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL and parameters for SQLite using SQLStatement.

        SQLite supports both named (:name) and positional (?) parameters.
        This method processes the SQL with dialect-aware parsing and handles
        parameters appropriately for SQLite.

        Args:
            sql: The SQL to process.
            parameters: The parameters to process.
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments.

        Returns:
            A tuple of (processed SQL, processed parameters).
        """
        # Create a SQLStatement with SQLite dialect
        statement = SQLStatement(sql, parameters, kwargs=kwargs, dialect=self.dialect)

        for filter_obj in filters:
            statement = statement.apply_filter(filter_obj)

        processed_sql, processed_params, _ = statement.process()

        if processed_params is None:
            return processed_sql, None

        if is_dict(processed_params):
            return processed_sql, processed_params

        if isinstance(processed_params, (list, tuple)):
            return processed_sql, tuple(processed_params)

        return processed_sql, (processed_params,)

    # --- Public API Methods --- #
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            results = cursor.fetchall()
            if not results:
                return []

            # Get column names
            column_names = [column[0] for column in cursor.description]

            return self.to_schema([dict(zip(column_names, row)) for row in results], schema_type=schema_type)

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(sql, parameters or [])
        result = cursor.fetchone()
        result = self.check_not_found(result)

        # Get column names
        column_names = [column[0] for column in cursor.description]

        return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            if result is None:
                return None

            # Get column names
            column_names = [column[0] for column in cursor.description]

            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            result = self.check_not_found(result)

            # Return first value from the row
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
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            if result is None:
                return None

            # Return first value from the row
            result_value = result[0]
            if schema_type is None:
                return result_value
            return schema_type(result_value)  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            return cursor.rowcount

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
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
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *filters: "StatementFilter",
        connection: "Optional[SqliteConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, *filters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters or [])
            result = cursor.fetchone()
            result = self.check_not_found(result)
            column_names = [column[0] for column in cursor.description]
            return self.to_schema(dict(zip(column_names, result)), schema_type=schema_type)

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        connection: "Optional[SqliteConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.executescript(sql)
            return "DONE"

    def _connection(self, connection: "Optional[SqliteConnection]" = None) -> "SqliteConnection":
        """Get the connection to use for the operation.

        Args:
            connection: Optional connection to use.

        Returns:
            The connection to use.
        """
        return connection or self.connection
