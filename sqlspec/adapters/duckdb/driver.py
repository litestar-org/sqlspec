from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.loader.protocols import StatementType, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from duckdb import DuckDBPyConnection

__all__ = ("DuckDBAdapter",)


class DuckDBAdapter(SyncDriverAdapterProtocol):
    """A DuckDB SQLSpec Adapter suitable for `named` parameter style and DuckDB connections."""

    is_async: bool = False

    def __init__(self, driver=None) -> None:
        self._driver = driver

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Preprocess SQL query.

        Args:
            op_type: The type of SQL operation being performed
            sql: The SQL query string

        Returns:
            The processed SQL query string
        """
        return sql

    def _cursor(self, connection: DuckDBPyConnection) -> Any:
        """Get a cursor from a connection.

        Args:
            connection: The DuckDB connection

        Returns:
            A DuckDB cursor object
        """
        return connection.cursor()

    def select(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters
            record_class: Optional class to construct records from

        Yields:
            Query results as either raw tuples or record_class instances
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            if record_class is None:
                yield from cur
            else:
                column_names = [c[0] for c in cur.description]
                for row in cur:
                    yield record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})  # pyright: ignore[reportCallIssue]
        finally:
            cur.close()

    def select_one(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        """Handle a single-row-returning SELECT.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters
            record_class: Optional class to construct the record from

        Returns:
            A single result row or None
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            row = cur.fetchone()
            if row is None or record_class is None:
                return row
            column_names = [c[0] for c in cur.description]
            return record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})  # pyright: ignore[reportCallIssue]
        finally:
            cur.close()

    def select_scalar(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        """Handle a scalar-returning SELECT.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters

        Returns:
            A single scalar value or None
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()

    @contextmanager
    def with_cursor(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
    ) -> Generator[Any, None, None]:
        """Execute a query and yield a cursor.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters

        Yields:
            A database cursor
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            yield cur
        finally:
            cur.close()

    def insert_update_delete(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle an INSERT, UPDATE, or DELETE.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters

        Returns:
            Number of rows affected
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            return cur.rowcount
        finally:
            cur.close()

    def insert_update_delete_returning(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Any:
        """Handle an INSERT, UPDATE, or DELETE with RETURNING clause.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters
            record_class: Optional class to construct the record from

        Returns:
            The returned row
        """
        return self.select_one(connection, sql, parameters, record_class)

    def insert_update_delete_many(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle multiple INSERT, UPDATE, or DELETE operations.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters

        Returns:
            Number of rows affected
        """
        cur = self._cursor(connection)
        try:
            cur.executemany(sql, parameters)
            return cur.rowcount
        finally:
            cur.close()

    def insert_update_delete_many_returning(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Iterable[Any]:
        """Handle multiple INSERT, UPDATE, or DELETE operations with RETURNING clause.

        Args:
            connection: The database connection
            sql: The SQL query string
            parameters: Query parameters
            record_class: Optional class to construct records from

        Returns:
            Iterator of returned rows
        """
        return self.select(connection, sql, parameters, record_class)

    def execute_script(
        self,
        connection: DuckDBPyConnection,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> Any:
        """Execute a SQL script.

        Args:
            connection: The database connection
            sql: The SQL script
            parameters: Optional query parameters
            record_class: Optional class to construct records from

        Returns:
            Result of the final statement in the script
        """
        cur = self._cursor(connection)
        try:
            if parameters:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()
