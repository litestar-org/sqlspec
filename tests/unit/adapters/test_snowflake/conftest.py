"""Deterministic DB-API boundary fakes for Snowflake public driver tests."""

from collections.abc import Iterator
from typing import Any

import pytest


class SnowflakeCursorFake:
    """Model connector cursor ownership and command/result metadata."""

    def __init__(self, connection: "SnowflakeConnectionFake") -> None:
        self.connection = connection
        self.description: list[tuple[Any, ...]] | None = None
        self.rowcount = -1
        self.sfqid: str | None = None
        self.rows: list[Any] = []
        self.tables: list[Any] = []
        self.closed = False
        self.close_count = 0
        self.arrow_error: BaseException | None = None
        self.pending_query_id: str | None = None

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> "SnowflakeCursorFake":
        self.connection.calls.append((sql, parameters, kwargs))
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise self.connection.failure
        normalized = sql.strip().rstrip(";").upper()
        if normalized == "SELECT CURRENT_TRANSACTION()":
            self.description = [("CURRENT_TRANSACTION()", 0, None, None, 38, 0, True)]
            self.rows = [(123 if self.connection.transaction_active else None,)]
            self.rowcount = 1
            return self
        if normalized in {"BEGIN", "BEGIN WORK", "BEGIN TRANSACTION"}:
            self.connection.transaction_active = True
        elif normalized.startswith(("CREATE ", "ALTER ", "DROP ")):
            self.connection.commit_pending()
        elif normalized.startswith("INSERT "):
            self.connection.insert_calls += 1
            if self.connection.fail_on_insert == self.connection.insert_calls:
                raise self.connection.failure
            self.connection.write_rows([parameters])
        result = self.connection.results.get(sql, self.connection.default_result)
        if result is not None and (
            sql.lstrip().upper().startswith(("SELECT", "(SELECT", "WITH", "SHOW", "DESCRIBE", "CALL"))
            or sql in self.connection.results
        ):
            self.description = result.get("description")
            self.rows = list(result.get("rows", []))
            self.tables = list(result.get("tables", []))
            self.arrow_error = result.get("arrow_error")
            self.rowcount = len(self.rows)
        else:
            self.description = [("number of rows inserted", 0, None, None, 18, 0, False)]
            self.rows = [(1,)]
            self.tables = []
            self.rowcount = 1
        return self

    def executemany(self, sql: str, parameters: Any) -> "SnowflakeCursorFake":
        self.connection.calls.append((sql, parameters, {}))
        self.connection.batch_calls += 1
        if self.connection.fail_on_batch == self.connection.batch_calls:
            raise self.connection.failure
        if "VALUES" not in sql.upper():
            from snowflake.connector.errors import InterfaceError

            raise InterfaceError(msg="Failed to rewrite multi-row insert")
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise self.connection.failure
        self.description = [("number of rows inserted", 0, None, None, 18, 0, False)]
        self.connection.write_rows(list(parameters))
        self.rowcount = len(parameters)
        return self

    def execute_async(self, sql: str, parameters: Any = None, **kwargs: Any) -> None:
        self.execute(sql, parameters, **kwargs)
        self.sfqid = "sfqid-123"

    def get_results_from_sfqid(self, query_id: str) -> None:
        self.description = None
        self.pending_query_id = query_id
        self.sfqid = query_id

    def fetchall(self) -> list[Any]:
        if self.pending_query_id is not None:
            self.pending_query_id = None
            self.execute("SELECT async_result")
        rows, self.rows = self.rows, []
        return rows

    def fetch_arrow_batches(self) -> Iterator[Any]:
        if self.arrow_error is not None:
            raise self.arrow_error
        yield from self.tables

    def close(self) -> None:
        self.close_count += 1
        self.closed = True


class SnowflakeConnectionFake:
    """Record connector calls while yielding independent cursor result sets."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.cursors: list[SnowflakeCursorFake] = []
        self.results: dict[str, dict[str, Any]] = {}
        self.default_result: dict[str, Any] | None = None
        self.fail_on: str | None = None
        self.failure: BaseException = RuntimeError("connector failure")
        self.commits = 0
        self.rollbacks = 0
        self.scripts: list[str] = []
        self.transaction_active = False
        self.autocommit_enabled = True
        self.pending_rows: list[Any] = []
        self.committed_rows: list[Any] = []
        self.batch_calls = 0
        self.insert_calls = 0
        self.fail_on_insert: int | None = None
        self.fail_on_batch: int | None = None
        self.paramstyle = "qmark"

    def cursor(self) -> SnowflakeCursorFake:
        cursor = SnowflakeCursorFake(self)
        self.cursors.append(cursor)
        return cursor

    def write_rows(self, rows: list[Any]) -> None:
        if not self.autocommit_enabled:
            self.transaction_active = True
        (self.pending_rows if self.transaction_active else self.committed_rows).extend(rows)

    def commit_pending(self) -> None:
        self.committed_rows.extend(self.pending_rows)
        self.pending_rows.clear()
        self.transaction_active = False

    def commit(self) -> None:
        self.commits += 1
        self.commit_pending()

    def rollback(self) -> None:
        self.rollbacks += 1
        self.pending_rows.clear()
        self.transaction_active = False

    def execute_string(self, sql: str, *, remove_comments: bool, return_cursors: bool) -> list[SnowflakeCursorFake]:
        self.scripts.append(sql)
        cursors = []
        for statement in sql.split(";"):
            if statement.strip():
                cursor = self.cursor()
                cursor.execute(statement.strip())
                cursors.append(cursor)
        return cursors


@pytest.fixture
def snowflake_connection() -> SnowflakeConnectionFake:
    return SnowflakeConnectionFake()
