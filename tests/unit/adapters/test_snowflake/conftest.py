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
        self.arrow_error: BaseException | None = None
        self.pending_query_id: str | None = None

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> "SnowflakeCursorFake":
        self.connection.calls.append((sql, parameters, kwargs))
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise self.connection.failure
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
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise self.connection.failure
        self.description = [("number of rows inserted", 0, None, None, 18, 0, False)]
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

    def cursor(self) -> SnowflakeCursorFake:
        cursor = SnowflakeCursorFake(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def execute_string(self, sql: str, *, remove_comments: bool, return_cursors: bool) -> list[SnowflakeCursorFake]:
        self.scripts.append(sql)
        return [self.cursor(), self.cursor()]


@pytest.fixture
def snowflake_connection() -> SnowflakeConnectionFake:
    return SnowflakeConnectionFake()
