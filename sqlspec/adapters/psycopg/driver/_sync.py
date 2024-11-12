from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.psycopg.driver._base import BasePsycopgAdapter
from sqlspec.types.protocols import SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from psycopg import Connection, Cursor

__all__ = ("PsycopgSyncAdapter",)


class PsycopgSyncAdapter(BasePsycopgAdapter, SyncDriverAdapterProtocol):
    """A synchronous Psycopg SQLSpec Adapter suitable for PostgreSQL-style parameter binding."""

    is_async: bool = False

    def _cursor(self, connection: Connection) -> Cursor:
        """Get a cursor from a connection."""
        return connection.cursor()

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
                column_names = [desc.name for desc in cur.description]
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
            column_names = [desc.name for desc in cur.description]
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
