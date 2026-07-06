"""Shared pymssql adapter test doubles."""

from typing import Any


class FakePymssqlError(Exception):
    """Base fake pymssql exception."""


class FakePymssqlOperationalError(FakePymssqlError):
    """Fake operational error."""


class FakePymssqlIntegrityError(FakePymssqlError):
    """Fake integrity error."""


class FakeCursor:
    """Minimal DB-API cursor for pymssql unit tests."""

    def __init__(
        self, rows: "list[Any] | None" = None, description: "list[tuple[str, ...]] | None" = None, rowcount: int = -1
    ) -> None:
        self.rows = rows or []
        self.description = description
        self.rowcount = rowcount
        self.closed = False
        self.calls: list[tuple[str, Any]] = []
        self.fetchmany_sizes: list[int] = []
        self.many_calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.calls.append((sql, parameters))

    def executemany(self, sql: str, parameters: Any = None) -> None:
        self.many_calls.append((sql, parameters))

    def fetchall(self) -> "list[Any]":
        return self.rows

    def fetchmany(self, size: int) -> "list[Any]":
        self.fetchmany_sizes.append(size)
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Minimal pymssql connection for config, pool, and driver tests."""

    def __init__(self, cursor: "FakeCursor | None" = None) -> None:
        self.cursor_obj = cursor or FakeCursor()
        self.closed = False
        self.commits = 0
        self.rollbacks = 0
        self.autocommit_values: list[bool] = []

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        self.cursor_args = args
        self.cursor_kwargs = kwargs
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def autocommit(self, value: bool) -> None:
        self.autocommit_values.append(value)


class FakePymssqlModule:
    """Patch target that behaves like the pymssql module surface used by SQLSpec."""

    Error = FakePymssqlError
    OperationalError = FakePymssqlOperationalError
    IntegrityError = FakePymssqlIntegrityError
    DatabaseError = FakePymssqlError
    DataError = FakePymssqlError
    InterfaceError = FakePymssqlError
    ProgrammingError = FakePymssqlError

    def __init__(self, connection: "FakeConnection | None" = None) -> None:
        self.connection = connection or FakeConnection()
        self.connect_calls: list[dict[str, Any]] = []

    def connect(self, **kwargs: Any) -> FakeConnection:
        self.connect_calls.append(kwargs)
        return self.connection
