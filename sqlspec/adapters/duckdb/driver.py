from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.base import SyncDriverAdapterProtocol, T
from sqlspec.typing import ModelDTOT, StatementParameterType


class DuckDBSyncDriver(SyncDriverAdapterProtocol[DuckDBPyConnection]):
    """DuckDB Sync Driver Adapter."""

    connection: DuckDBPyConnection
    use_cursor: bool = True

    def __init__(self, connection: DuckDBPyConnection, use_cursor: bool = True, results_as_dict: bool = False) -> None:
        self.connection = connection
        self.use_cursor = use_cursor
        self.results_as_dict = results_as_dict

    @staticmethod
    def _execute(conn: DuckDBPyConnection, sql: str, parameters: StatementParameterType) -> "list[Any]":
        """Execute a query and return the results.

        Args:
            conn: The DuckDB connection.
            sql: The SQL query to execute.
            parameters: The query parameters.

        Returns:
            A list of query results.
        """
        return conn.execute(sql, parameters).fetchall()

    def _cursor(self, conn: DuckDBPyConnection) -> DuckDBPyConnection:
        if self.use_cursor:
            return conn.cursor()
        return conn

    @contextmanager
    def with_cursor(self, conn: DuckDBPyConnection) -> Generator[DuckDBPyConnection, None, None]:
        cursor = self._cursor(conn)
        try:
            yield cursor
        finally:
            if self.use_cursor:
                cursor.close()

    def select(
        self,
        conn: DuckDBPyConnection,
        sql: str,
        parameters: StatementParameterType,
        /,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Generator[Union[ModelDTOT, dict[str, Any]], None, None]":
        """Select data from the database.

        Yields:
            Row data as either model instances or dictionaries.
        """
        column_names: list[str] = []
        with self.with_cursor(conn) as cursor:
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
        conn: DuckDBPyConnection,
        sql: str,
        parameters: StatementParameterType,
        /,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Select one row from the database.

        Returns:
            The first row of the query results.
        """
        column_names: list[str] = []
        with self.with_cursor(conn) as cursor:
            cursor.execute(sql, parameters)
            result = cast("Optional[tuple[Any, ...]]", cursor.fetchone())  # pyright: ignore[reportUnknownMemberType]
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
        conn: DuckDBPyConnection,
        sql: str,
        parameters: StatementParameterType,
        /,
        schema_type: "Optional[type[T]]" = None,
    ) -> "Optional[Union[T, Any]]":
        """Select a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        with self.with_cursor(conn) as cursor:
            cursor.execute(sql, parameters)
            result = cast("Optional[tuple[Any, ...]]", cursor.fetchone())  # pyright: ignore[reportUnknownMemberType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]
            return schema_type(result[0])  # pyright: ignore[reportCallIssue]

    def insert_update_delete(
        self,
        conn: DuckDBPyConnection,
        sql: str,
        parameters: StatementParameterType,
        /,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[int, Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Insert, update, or delete data from the database.

        Returns:
            Row count if not returning data, otherwise the first row of results.
        """
        column_names: list[str] = []
        with self.with_cursor(conn) as cursor:
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
        conn: DuckDBPyConnection,
        sql: str,
        parameters: StatementParameterType,
        /,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        returning: bool = False,
    ) -> "Optional[Union[Any,ModelDTOT, dict[str, Any], tuple[Any, ...]]]":
        """Execute a script.

        Returns:
            The number of rows affected by the script.
        """
        column_names: list[str] = []
        with self.with_cursor(conn) as cursor:
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
