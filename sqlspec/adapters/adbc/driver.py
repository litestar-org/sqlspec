from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.types.protocols import StatementType, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from adbc_driver_manager.dbapi import Connection, Cursor

__all__ = ("AdbcAdapter",)


class AdbcAdapter(SyncDriverAdapterProtocol):
    """A synchronous ADBC SQLSpec Adapter."""

    is_async: bool = False

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Process SQL query."""
        return sql

    def _cursor(self, connection: Connection) -> Cursor:
        """Get a cursor from a connection."""
        return connection.cursor()

    def _process_row(self, row: Any, column_names: list[str], record_class: Callable | None = None) -> Any:
        """Process a row into the desired format."""
        if record_class is None:
            return row
        return record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})

    def select(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT."""
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            if record_class is None:
                yield from cur
            else:
                column_names = [desc[0] for desc in cur.description]
                for row in cur:
                    yield self._process_row(row, column_names, record_class)
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
            if row is None:
                return None
            if record_class is None:
                return row
            column_names = [desc[0] for desc in cur.description or []]
            return self._process_row(row, column_names, record_class)
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

    def insert_update_delete_many(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict,
    ) -> int:
        """Handle multiple INSERT, UPDATE, or DELETE operations."""
        cur = self._cursor(connection)
        try:
            cur.executemany(sql, parameters)
            return cur.rowcount
        finally:
            cur.close()
