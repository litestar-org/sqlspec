from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import SQLSpecError
from sqlspec.types.protocols import StatementType, SyncDriverAdapterProtocol

if TYPE_CHECKING:
    from collections.abc import Callable


class GenericAdapter(SyncDriverAdapterProtocol):
    """A Generic SQLSpec Adapter suitable for `named` parameter style and DB-API compliant connections..

    This class also serves as the base class for other adapters.
    """

    is_asyncio = False

    def __init__(self, driver=None) -> None:
        self._driver = driver

    def process_sql(self, statement_name: str, op_type: StatementType, sql: str) -> str:
        """Preprocess SQL query."""
        return sql

    def _cursor(self, conn: Any) -> Any:
        """Get a cursor from a connection."""
        return conn.cursor()

    def select(self, conn: Any, statement_name: str, sql: str, parameters: list | dict, record_class: Callable | None):
        """Handle a relation-returning SELECT (no suffix)."""
        column_names: list[str] = []
        cur = self._cursor(conn)
        try:
            cur.execute(sql, parameters)
            if record_class is None:
                yield from cur
            else:
                first = True
                for row in cur:
                    if first:  # only get description on the fly, for apsw
                        column_names = [c[0] for c in cur.description]
                        first = False
                    yield record_class(**dict(zip(column_names, row, strict=False)))
        finally:
            cur.close()

    def select_one(
        self, conn: Any, statement_name: str, sql: str, parameters: list | dict, record_class: Callable | None
    ):
        """Handle a tuple-returning (one row) SELECT (``^`` suffix).

        Return None if empty.
        """
        cur = self._cursor(conn)
        try:
            cur.execute(sql, parameters)
            result = cur.fetchone()
            if result is not None and record_class is not None:
                column_names = [c[0] for c in cur.description]
                # this fails if result is not a list or tuple
                result = record_class(**dict(zip(column_names, result, strict=False)))
        finally:
            cur.close()
        return result

    def select_value(self, conn: Any, statement_name: str, sql: str, parameters: list | dict):
        """Handle a scalar-returning (one value) SELECT (``$`` suffix).

        Return None if empty.
        """
        cur = self._cursor(conn)
        try:
            cur.execute(sql, parameters)
            result = cur.fetchone()
            if result:
                if isinstance(result, list | tuple):
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
    def select_cursor(self, conn: Any, statement_name: str, sql: str, parameters: list | dict):
        """Return the raw cursor after a SELECT exec."""
        cur = self._cursor(conn)
        cur.execute(sql, parameters)
        try:
            yield cur
        finally:
            cur.close()

    def insert_update_delete(self, conn: Any, statement_name: str, sql: str, parameters: list | dict) -> int:
        """Handle affected row counts (INSERT UPDATE DELETE) (``!`` suffix)."""
        cur = self._cursor(conn)
        cur.execute(sql, parameters)
        rc = cur.rowcount if hasattr(cur, "rowcount") else -1
        cur.close()
        return rc

    def insert_update_delete_many(self, conn: Any, statement_name: str, sql: str, parameters: list | dict) -> int:
        """Handle affected row counts (INSERT UPDATE DELETE) (``*!`` suffix)."""
        cur = self._cursor(conn)
        cur.executemany(sql, parameters)
        rc = cur.rowcount if hasattr(cur, "rowcount") else -1
        cur.close()
        return rc

    # FIXME this made sense when SQLite had no RETURNING prefix (v3.35, 2021-03-12)
    def insert_returning(self, conn: Any, statement_name: str, sql: str, parameters: list | dict):
        """Special case for RETURNING (``<!`` suffix) with SQLite."""
        # very similar to select_one but the returned value
        cur = self._cursor(conn)
        cur.execute(sql, parameters)
        res = cur.fetchone()
        cur.close()
        return res[0] if res and len(res) == 1 else res

    def execute_script(self, conn: Any, sql: str) -> str:
        """Handle an SQL script (``#`` suffix)."""
        cur = self._cursor(conn)
        cur.execute(sql)
        msg = cur.statusmessage if hasattr(cur, "statusmessage") else "DONE"
        cur.close()
        return msg
