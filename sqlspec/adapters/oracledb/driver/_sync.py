from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.loader.protocols import StatementType, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from oracledb import Connection, Cursor

__all__ = ("OracleSyncAdapter",)


class OracleSyncAdapter(SyncDriverAdapterProtocol):
    """A synchronous Oracle SQLSpec Adapter suitable for `named` parameter style and DB-API compliant connections."""

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

    def _cursor(self, connection: Connection) -> Cursor:
        """Get a cursor from a connection.

        Args:
            connection: The Oracle connection

        Returns:
            An Oracle cursor object
        """
        return connection.cursor()

    def select(
        self,
        connection: Connection,
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
                column_names = [desc[0] for desc in cur.description]
                for row in cur:
                    yield record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})
        finally:
            cur.close()

    def select_one(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None:
        """Handle a single-row-returning SELECT."""
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            row = cur.fetchone()
            if row is None or record_class is None:
                return row
            column_names = [desc[0] for desc in cur.description]
            return record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})
        finally:
            cur.close()

    def select_scalar(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> Any | None:
        """Handle a scalar-returning SELECT."""
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
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> Generator[Cursor, None, None]:
        """Execute a query and yield the cursor."""
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            yield cur
        finally:
            cur.close()

    def insert_update_delete(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle an INSERT, UPDATE, or DELETE."""
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            return cur.rowcount
        finally:
            cur.close()

    def insert_update_delete_returning(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None = None,
    ) -> Any:
        """Handle an INSERT, UPDATE, or DELETE with RETURNING clause."""
        return self.select_one(connection, sql, parameters, record_class)

    def execute_script(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> Any:
        """Execute a SQL script."""
        cur = self._cursor(connection)
        try:
            if parameters:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()
