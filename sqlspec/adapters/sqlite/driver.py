from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Union

from sqlspec.exceptions import SQLSpecError
from sqlspec.types.protocols import StatementType, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from sqlite3 import Connection

__all__ = ("SqliteAdapter",)


class SqliteAdapter(SyncDriverAdapterProtocol):
    """A SQLite SQLSpec Adapter suitable for `named` parameter style and DB-API compliant connections."""

    is_async: bool = False

    def __init__(self, driver=None) -> None:
        self._driver = driver

    def process_sql(self, op_type: StatementType, sql: str) -> str:
        """Preprocess SQL query."""
        return sql

    def _cursor(self, connection: Any) -> Any:
        """Get a cursor from a connection."""
        return connection.cursor()

    def select(
        self, connection: Connection, sql: str, parameters: list | dict, record_class: Callable | None
    ) -> Iterable[Any]:
        """Handle a relation-returning SELECT (no suffix)."""
        column_names: list[str] = []
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            if record_class is None:
                yield from cur  # pyright: ignore[reportReturnType]
            else:
                first = True
                for row in cur:
                    if first:  # only get description on the fly, for apsw
                        column_names = [c[0] for c in cur.description]
                        first = False
                    yield record_class(**{str(k): v for k, v in zip(column_names, row, strict=False)})  # pyright: ignore[reportCallIssue,reportReturnType]
        finally:
            cur.close()

    def select_one(
        self, connection: Connection, sql: str, parameters: list | dict, record_class: Callable | None
    ) -> Any | None:
        """Handle a tuple-returning (one row) SELECT (``^`` suffix).

        Return None if empty.
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            result = cur.fetchone()
            if result is not None and record_class is not None:
                column_names = [c[0] for c in cur.description]
                # this fails if result is not a list or tuple
                result = record_class(**{str(k): v for k, v in zip(column_names, result, strict=False)})  # pyright: ignore[reportCallIssue]
        finally:
            cur.close()
        return result

    def select_scalar(self, connection: Connection, sql: str, parameters: list | dict) -> Any | None:
        """Handle a scalar-returning (one value) SELECT (``$`` suffix).

        Return None if empty.
        """
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            result = cur.fetchone()
            if result:
                if isinstance(result, Union[list, tuple]):
                    return result[0]
                if isinstance(result, dict):
                    return next(iter(result.values()))
                # pragma: no cover
                msg = f"unexpected value type: {type(result)}"
                raise SQLSpecError(msg)
            return None
        finally:
            cur.close()

    @contextmanager
    def with_cursor(self, connection: Connection, sql: str, parameters: list | dict) -> Generator[Any, None, None]:
        """Return the raw cursor after a SELECT exec.

        Args:
            connection: The database connection
            sql: The SQL query to execute
            parameters: The query parameters

        Yields:
            The database cursor after executing the query
        """
        cur = self._cursor(connection)
        cur.execute(sql, parameters)
        try:
            yield cur
        finally:
            cur.close()

    def insert_update_delete(self, connection: Connection, sql: str, parameters: list | dict) -> int:
        """Handle affected row counts (INSERT UPDATE DELETE) (``!`` suffix)."""
        cur = self._cursor(connection)
        cur.execute(sql, parameters)
        rc = cur.rowcount if hasattr(cur, "rowcount") else -1
        cur.close()
        return rc

    def insert_update_delete_returning(self, connection: Connection, sql: str, parameters: list | dict) -> Any:
        """Execute Statement with a RETURNING (``<!`` suffix) clause with SQLite."""
        cur = self._cursor(connection)
        try:
            cur.execute(sql, parameters)
            results = cur.lastrowid
        finally:
            cur.close()
        return results

    def insert_update_delete_many(self, connection: Connection, sql: str, parameters: list | dict) -> int:
        """Handle affected row counts (INSERT UPDATE DELETE) (``*!`` suffix)."""
        cur = self._cursor(connection)
        try:
            cur.executemany(sql, parameters)
            rc = cur.rowcount if hasattr(cur, "rowcount") else -1
        finally:
            cur.close()
        return rc

    def insert_update_delete_many_returning(self, connection: Connection, sql: str, parameters: list | dict) -> list:
        """Special case for RETURNING (``<!`` suffix) with SQLite."""
        # very similar to select_one but the returned value
        cur = self._cursor(connection)
        cur.executemany(sql, parameters)
        res = cur.fetchall()
        cur.close()
        return res

    def execute_script(
        self,
        connection: Connection,
        sql: str,
        parameters: list | dict | None = None,
        record_class: Callable | None = None,
    ) -> str:
        """Handle an SQL script (``#`` suffix)."""
        connection.executescript(sql)
        return "DONE"
