"""Unit tests for mssql_python driver wiring."""

from typing import Any, cast

from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.core import StatementStack


class DummyCursor:
    """Minimal cursor for driver dispatch tests."""

    description = None
    rowcount = 1

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.executed.append((sql, parameters))

    def close(self) -> None:
        self.closed = True


class DummyConnection:
    """Minimal connection for driver construction."""

    def __init__(self) -> None:
        self.cursor_obj = DummyCursor()
        self.commits = 0
        self.rollbacks = 0
        self.autocommit = False

    def cursor(self) -> DummyCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_sync_driver_lazily_initializes_data_dictionary() -> None:
    """The sync driver should expose the MSSQL data dictionary."""
    driver = MssqlPythonDriver(cast("Any", DummyConnection()))

    assert isinstance(driver.data_dictionary, MssqlPythonSyncDataDictionary)
    assert driver.data_dictionary is driver.data_dictionary


def test_sync_driver_execute_stack_uses_dbapi_transaction_fallback() -> None:
    """Stack execution should use and commit the DBAPI transaction."""
    connection = DummyConnection()
    driver = MssqlPythonDriver(cast("Any", connection))
    stack = StatementStack().push_execute("UPDATE queue SET state = ? WHERE id = ?", "ready", 1)

    results = driver.execute_stack(stack)

    assert len(results) == 1
    assert connection.cursor_obj.executed == [("UPDATE queue SET state = ? WHERE id = ?", ["ready", 1])]
    assert connection.commits == 1
    assert connection.rollbacks == 0
