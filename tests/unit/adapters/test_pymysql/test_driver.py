"""Unit tests for the PyMySQL driver."""

from typing import Any, cast

from pymysql.constants import SERVER_STATUS

from sqlspec import StatementStack
from sqlspec.adapters.pymysql import PyMysqlDriver


class _FakeCursor:
    description = None
    lastrowid = 1
    rowcount = 1

    def __init__(self, connection: "_FakeConnection") -> None:
        self.connection = connection

    def close(self) -> None:
        self.connection.closed_cursors += 1

    def execute(self, statement: str, parameters: Any = None) -> None:
        self.connection.executed.append((statement, parameters))

    def fetchall(self) -> "list[Any]":
        return []


class _FakeConnection:
    def __init__(self, *, server_status: int | None = 0, autocommit: bool = False) -> None:
        self._autocommit = autocommit
        self._server_status = server_status
        self.closed_cursors = 0
        self.commits = 0
        self.executed: list[tuple[str, Any]] = []
        self.rollbacks = 0

    @property
    def server_status(self) -> int:
        if self._server_status is None:
            raise RuntimeError("server status unavailable")
        return self._server_status

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def get_autocommit(self) -> bool:
        return self._autocommit

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _driver(connection: _FakeConnection) -> PyMysqlDriver:
    return PyMysqlDriver(cast("Any", connection))


def test_execute_stack_commits_when_autocommit_disabled_without_server_transaction() -> None:
    connection = _FakeConnection(server_status=0, autocommit=False)
    stack = StatementStack().push_execute("INSERT INTO demo (name) VALUES (?)", ("owned",))

    _driver(connection).execute_stack(stack)

    assert connection.executed[0][0] == "BEGIN"
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_execute_stack_preserves_caller_transaction_when_server_status_reports_transaction() -> None:
    connection = _FakeConnection(server_status=SERVER_STATUS.SERVER_STATUS_IN_TRANS, autocommit=False)
    stack = StatementStack().push_execute("INSERT INTO demo (name) VALUES (?)", ("caller",))

    _driver(connection).execute_stack(stack)

    assert all(statement != "BEGIN" for statement, _ in connection.executed)
    assert connection.commits == 0
    assert connection.rollbacks == 0


def test_connection_in_transaction_returns_false_when_server_status_unreadable() -> None:
    connection = _FakeConnection(server_status=None, autocommit=False)

    assert _driver(connection)._connection_in_transaction() is False
