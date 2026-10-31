"""Unit tests for the PyMySQL driver."""

from typing import Any, cast

import pytest
from pymysql.constants import SERVER_STATUS

from sqlspec import SQL, StatementStack
from sqlspec.adapters.pymysql import PyMysqlDriver
from sqlspec.adapters.pymysql._typing import PyMysqlMySQLError
from sqlspec.exceptions import DeadlockError


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
        self.begins = 0

    @property
    def server_status(self) -> int:
        if self._server_status is None:
            raise RuntimeError("server status unavailable")
        return self._server_status

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def get_autocommit(self) -> bool:
        return self._autocommit

    def begin(self) -> None:
        self.begins += 1

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

    assert connection.begins == 1
    assert all(statement != "BEGIN" for statement, _ in connection.executed)


def test_pymysql_sync_stream_source_in_active_except_block() -> None:
    """PyMysqlStreamSource safely handles execution inside an active except block."""
    from unittest.mock import MagicMock

    from sqlspec.adapters.pymysql.core import PyMysqlStreamSource

    cursor = MagicMock()
    cursor.fetchmany.return_value = [{"id": 1}]
    cursor.description = [("id", 3, None, None, None, None, None)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = PyMysqlDriver(connection)

    try:
        raise ValueError("simulated active exception")
    except ValueError:
        source = PyMysqlStreamSource(driver, "SELECT 1", (), 10, set())
        source.start()
        chunk = source.fetch_chunk()
        assert len(chunk) == 1
        source.close()
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_execute_stack_preserves_caller_transaction_when_server_status_reports_transaction() -> None:
    connection = _FakeConnection(server_status=SERVER_STATUS.SERVER_STATUS_IN_TRANS, autocommit=False)
    stack = StatementStack().push_execute("INSERT INTO demo (name) VALUES (?)", ("caller",))

    _driver(connection).execute_stack(stack)

    assert connection.begins == 0
    assert all(statement != "BEGIN" for statement, _ in connection.executed)
    assert connection.commits == 0
    assert connection.rollbacks == 0


def test_connection_in_transaction_returns_false_when_server_status_unreadable() -> None:
    connection = _FakeConnection(server_status=None, autocommit=False)

    assert _driver(connection)._connection_in_transaction() is False


def test_commit_maps_pymysql_deadlock_error() -> None:
    connection = _FakeConnection()

    def _fail_commit() -> None:
        raise PyMysqlMySQLError(1213, "Deadlock found when trying to get lock")

    setattr(connection, "commit", _fail_commit)
    driver = _driver(connection)
    with pytest.raises(DeadlockError):
        driver.commit()


def test_pymysql_stream_source_caches_deserializer() -> None:
    """PyMysqlStreamSource caches deserializer in __init__."""
    from unittest.mock import MagicMock

    from sqlspec.adapters.pymysql.core import PyMysqlStreamSource

    def custom_deserializer(v: Any) -> Any:
        return v

    driver = MagicMock(
        _json_deserializer=custom_deserializer, driver_features={"json_deserializer": custom_deserializer}
    )

    source = PyMysqlStreamSource(driver, "SELECT 1", (), 10, set())
    assert source._deserializer is custom_deserializer


def test_pymysql_dispatch_execute_many_escapes_entire_sql() -> None:
    """dispatch_execute_many escapes % across the entire SQL statement."""
    cursor = _FakeCursor()
    conn = _FakeConnection()
    driver = PyMysqlDriver(cast("Any", conn))
    statement = SQL(
        "INSERT INTO demo (val) VALUES (%s) ON DUPLICATE KEY UPDATE val = val % 2",
        [(1,)],
        statement_config=driver.statement_config,
        is_many=True,
    )
    driver.dispatch_execute_many(cursor, statement)
    assert len(cursor.executed) == 1


def test_pymysql_dispatch_execute_uses_statement_config() -> None:
    """dispatch_execute respects statement.statement_config."""
    from sqlspec.core import StatementConfig

    cursor = _FakeCursor()
    conn = _FakeConnection()
    driver = PyMysqlDriver(cast("Any", conn))
    custom_config = StatementConfig(enable_caching=False)
    statement = SQL("SELECT 1", statement_config=custom_config)
    result = driver.dispatch_execute(cursor, statement)
    assert result is not None


def test_pymysql_dispatch_execute_script_delegates_to_super() -> None:
    """dispatch_execute_script emits script statement events via super()."""
    cursor = _FakeCursor()
    conn = _FakeConnection()
    driver = PyMysqlDriver(cast("Any", conn))
    statement = SQL("SELECT 1; SELECT 2").as_script()
    result = driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2
