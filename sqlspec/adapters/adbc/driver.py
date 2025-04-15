from collections.abc import Generator
from contextlib import contextmanager
from sqlite3 import Connection, Cursor
from typing import Any, Optional, Union, cast

from sqlspec.base import SyncDriverAdapterProtocol, T
from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = ("SQLiteDriver",)


class SQLiteDriver(SyncDriverAdapterProtocol[Connection]):
    """SQLite Sync Driver Adapter."""

    connection: Connection
    results_as_dict: bool = True

    def __init__(self, connection: Connection, results_as_dict: bool = True) -> None:
        self.connection = connection
        self.results_as_dict = results_as_dict

    @staticmethod
    def _execute(connection: Connection, sql: str, parameters: StatementParameterType) -> "list[Any]":
        """Execute a query and return the results.

        Args:
            connection: The SQLite connection.
            sql: The SQL query to execute.
            parameters: The query parameters.

        Returns:
            A list of query results.
        """
        parameters = parameters if parameters is not None else {}
        return connection.execute(sql, parameters).fetchall()

    @staticmethod
    def _cursor(connection: Connection, *args: Any, **kwargs: Any) -> Cursor:
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: Connection) -> Generator[Cursor, None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            cursor.close()

    def select(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Generator[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]], None, None]":
        """Fetch data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)

            if schema_type is None:
                first = True
                for row in cursor.fetchall():
                    if first:  # get column names on the fly
                        column_names = [c[0] for c in cursor.description or []]
                        first = False
                    if self.results_as_dict:  # pragma: no cover
                        # strict=False: requires 3.10
                        yield dict(zip(column_names, row))
                    else:
                        yield row
            else:  # pragma: no cover
                first = True
                for row in cursor.fetchall():
                    if first:
                        column_names = [c[0] for c in cursor.description or []]
                        first = False
                    yield schema_type(**dict(zip(column_names, row)))

    def select_one(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        column_names: list[str] = []
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None and self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result))
            if schema_type is not None:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, result)))
            return result

    def select_value(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            result = cast("Optional[tuple[Any, ...]]", cursor.fetchone())  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # pyright: ignore[reportCallIssue]

    def insert_update_delete(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            if returning is False:
                return cursor.rowcount if hasattr(cursor, "rowcount") else -1
            result = cursor.fetchall()
            if len(result) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, result[0])))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result[0]))
            return result[0]

    def execute_script(
        self,
        sql: str,
        parameters: StatementParameterType,
        /,
        connection: Optional[Connection] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        connection = connection if connection is not None else self.connection
        parameters = parameters if parameters is not None else {}
        column_names: list[str] = []
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)
            if returning is False:
                return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]
            result = cursor.fetchall()
            if len(result) == 0:
                return None
            if schema_type:
                column_names = [c[0] for c in cursor.description or []]
                return schema_type(**dict(zip(column_names, result[0])))
            if self.results_as_dict:
                column_names = [c[0] for c in cursor.description or []]
                return dict(zip(column_names, result[0]))
            return result[0]
