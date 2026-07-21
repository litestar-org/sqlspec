"""mssql-python transaction ownership and state tests."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.exceptions import SQLSpecError, TransactionError

pytestmark = pytest.mark.xdist_group("mssql_python")

UNSAFE_SAVEPOINT_NAMES = ["1; DROP TABLE users", "sp-1", "sp 1", "", '"sp"']


class FakeCursor:
    """Minimal cursor recording executed statements."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.closed = False

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.calls.append((sql, parameters))

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Minimal mssql-python connection tracking transaction state changes."""

    def __init__(self, cursor: "FakeCursor | None" = None, *, autocommit: bool = False) -> None:
        self.cursor_obj = cursor or FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.autocommit_values: list[bool] = []
        self.fail_autocommit_value: bool | None = None
        self.fail_commit = False
        self.fail_rollback = False
        self._autocommit = autocommit

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if value is self.fail_autocommit_value:
            raise FakeMssqlError
        self._autocommit = value
        self.autocommit_values.append(value)

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        if self.fail_commit:
            raise FakeMssqlError
        self.commits += 1

    def rollback(self) -> None:
        if self.fail_rollback:
            raise FakeMssqlError
        self.rollbacks += 1


class FakeMssqlError(Exception):
    """Minimal mssql-python database error."""


def test_mssql_python_sync_begin_uses_dbapi_transaction_and_tracks_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Begin should track the DBAPI transaction without issuing transaction SQL."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]

    driver.begin()
    assert cursor.calls == []
    assert connection.autocommit_values == []
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    driver.commit()
    assert connection.commits == 1
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_mssql_python_transaction_restores_autocommit(
    method_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Transactions entered from autocommit mode should restore that mode on completion."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection(autocommit=True)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    driver.begin()

    assert connection.autocommit is False
    assert connection.autocommit_values == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    getattr(driver, method_name)()

    assert connection.autocommit is True
    assert connection.autocommit_values == [False, True]
    assert connection.commits == (1 if method_name == "commit" else 0)
    assert connection.rollbacks == (1 if method_name == "rollback" else 0)
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_mssql_python_repeated_begin_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated begin should preserve the original autocommit restoration decision."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection(autocommit=True)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    driver.begin()
    driver.begin()
    driver.commit()

    assert connection.autocommit_values == [False, True]
    assert connection.cursor_obj.calls == []


def test_mssql_python_begin_failure_keeps_transaction_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failure while disabling autocommit should leave transaction state inactive."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection(autocommit=True)
    connection.fail_autocommit_value = False
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    with pytest.raises(SQLSpecError, match="Failed to begin transaction"):
        driver.begin()

    assert connection.autocommit is True
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_mssql_python_completion_failure_preserves_active_state(
    method_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed DBAPI completion should leave the transaction active and un-restored."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection(autocommit=True)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))
    driver.begin()
    setattr(connection, f"fail_{method_name}", True)

    with pytest.raises(SQLSpecError, match=f"Failed to {method_name} transaction"):
        getattr(driver, method_name)()

    assert connection.autocommit is False
    assert connection.autocommit_values == [False]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("method_name", ["commit", "rollback"])
def test_mssql_python_restore_failure_reports_inactive_transaction(
    method_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A restoration failure should surface after the DBAPI transaction has completed."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection(autocommit=True)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))
    driver.begin()
    connection.fail_autocommit_value = True

    with pytest.raises(SQLSpecError, match="Failed to restore autocommit"):
        getattr(driver, method_name)()

    assert connection.autocommit is False
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


def test_mssql_python_sync_rollback_tracks_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rollback should delegate to the DBAPI connection and clear state."""
    monkeypatch.setattr("sqlspec.adapters.mssql_python.driver._MSSQL_ERROR", FakeMssqlError)
    connection = FakeConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    driver.begin()
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]
    driver.rollback()
    assert connection.rollbacks == 1
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("bad_name", UNSAFE_SAVEPOINT_NAMES)
def test_mssql_python_savepoint_overrides_reject_unsafe_names(bad_name: str) -> None:
    """The T-SQL savepoint overrides must reject unsafe identifiers before interpolation."""
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", FakeConnection()))

    with pytest.raises(TransactionError):
        driver.create_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.release_savepoint(bad_name)
    with pytest.raises(TransactionError):
        driver.rollback_to_savepoint(bad_name)


def test_mssql_python_savepoint_overrides_accept_valid_name() -> None:
    """A safe savepoint name should pass validation and reach the underlying execute path."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    driver.create_savepoint("sp1")
    driver.rollback_to_savepoint("sp1")

    executed_sql = [call[0] for call in cursor.calls]
    assert "SAVE TRANSACTION sp1" in executed_sql
    assert "ROLLBACK TRANSACTION sp1" in executed_sql
