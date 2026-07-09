"""mssql-python transaction honesty: begin() issues BEGIN TRANSACTION and the state predicate follows begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.exceptions import TransactionError

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
    """Minimal mssql-python connection tracking commit/rollback counts."""

    def __init__(self, cursor: "FakeCursor | None" = None) -> None:
        self.cursor_obj = cursor or FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_mssql_python_sync_begin_issues_tsql_and_tracks_state() -> None:
    """Sync begin/commit/rollback should issue BEGIN TRANSACTION and drive the state predicate."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]

    driver.begin()
    assert cursor.calls == [("BEGIN TRANSACTION", None)]
    assert driver._connection_in_transaction() is True  # pyright: ignore[reportPrivateUsage]

    driver.commit()
    assert connection.commits == 1
    assert driver._connection_in_transaction() is False  # pyright: ignore[reportPrivateUsage]

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
