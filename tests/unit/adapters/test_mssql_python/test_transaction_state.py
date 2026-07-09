"""mssql-python transaction honesty: begin() issues BEGIN TRANSACTION and the state predicate follows begin/commit/rollback."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

pytestmark = pytest.mark.xdist_group("mssql_python")


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
