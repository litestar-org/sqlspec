"""Unit tests for the mssql-python Litestar store."""

from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore


class FakeCursor:
    """Cursor stub recording executed SQL and returning queued rows."""

    def __init__(self, connection: "FakeConnection") -> None:
        self._connection = connection
        self.rowcount = connection.rowcount

    def execute(self, sql: str, parameters: Any = None) -> None:
        self._connection.executed.append((sql, parameters))

    def fetchone(self) -> Any:
        if self._connection.rows:
            return self._connection.rows.pop(0)
        return None

    def close(self) -> None:
        return None


class FakeConnection:
    """Sync connection stub producing recording cursors."""

    def __init__(self, rows: "list[Any] | None" = None, rowcount: int = 0) -> None:
        self.rows = list(rows or [])
        self.rowcount = rowcount
        self.executed: list[tuple[str, Any]] = []
        self.commits = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1


class FakeConnectionContext:
    """Sync context manager returning a fake connection."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class FakeSession:
    """Sync driver stub recording DDL scripts."""

    def __init__(self) -> None:
        self.scripts: list[str] = []
        self.commits = 0

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)

    def commit(self) -> None:
        self.commits += 1


class FakeSessionContext:
    """Sync context manager returning a fake session."""

    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def __enter__(self) -> FakeSession:
        return self.session

    def __exit__(self, *_: object) -> None:
        return None


class FakeConfig:
    """Config stub implementing provide_connection and provide_session."""

    def __init__(
        self,
        connection: "FakeConnection | None" = None,
        session: "FakeSession | None" = None,
        table_name: str = "mssql_sessions",
    ) -> None:
        self.extension_config = {"litestar": {"session_table": table_name}}
        self._connection = connection
        self._session = session

    def provide_connection(self) -> FakeConnectionContext:
        assert self._connection is not None
        return FakeConnectionContext(self._connection)

    def provide_session(self) -> FakeSessionContext:
        assert self._session is not None
        return FakeSessionContext(self._session)


def test_mssql_python_store_imports() -> None:
    """The mssql_python Litestar namespace should export the store."""
    assert MssqlPythonStore.__name__ == "MssqlPythonStore"


def test_mssql_python_store_create_table_uses_tsql_guards() -> None:
    """The store DDL should use SQL Server idempotent guards and binary payload storage."""
    store = MssqlPythonStore(cast("Any", FakeConfig()))

    ddl = store._table_ddl()

    assert "IF NOT EXISTS" in ddl
    assert "sys.tables" in ddl
    assert "VARBINARY(MAX)" in ddl
    assert "DATETIME2(6)" in ddl
    assert "WHERE expires_at IS NOT NULL" in ddl


async def test_mssql_python_store_create_table_bridges_sync_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """create_table should run the DDL script through the sync session."""
    session = FakeSession()
    monkeypatch.setattr(MssqlPythonStore, "reconcile_schema", AsyncMock())
    store = MssqlPythonStore(cast("Any", FakeConfig(session=session)))

    await store.create_table()

    assert len(session.scripts) == 1
    assert "CREATE TABLE mssql_sessions" in session.scripts[0]
    assert session.commits == 1


async def test_mssql_python_store_set_uses_merge_with_positional_parameters() -> None:
    """set should upsert with MERGE and pass the key/data/expiry as positional parameters."""
    connection = FakeConnection()
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    await store.set("session-1", "payload", expires_in=60)

    sql, parameters = connection.executed[0]
    assert "MERGE INTO mssql_sessions" in sql
    assert parameters[0] == "session-1"
    assert parameters[1] == b"payload"
    assert isinstance(parameters[2], datetime)
    assert connection.commits == 1


async def test_mssql_python_store_get_renews_unexpired_session() -> None:
    """get should return bytes and renew expiry only when the existing row had an expiry."""
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    connection = FakeConnection(rows=[{"data": b"payload", "expires_at": future}])
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    data = await store.get("session-1", renew_for=60)

    assert data == b"payload"
    assert any("UPDATE mssql_sessions" in sql for sql, _ in connection.executed)
    assert connection.commits == 1


async def test_mssql_python_store_get_returns_none_for_missing_row() -> None:
    """get should return None when the filtered query yields no row."""
    connection = FakeConnection(rows=[])
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    data = await store.get("session-1")

    assert data is None
    assert connection.commits == 0


async def test_mssql_python_store_delete_expired_returns_rows_affected() -> None:
    """delete_expired should return the cursor rowcount."""
    connection = FakeConnection(rowcount=3)
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    count = await store.delete_expired()

    assert count == 3
    assert connection.commits == 1
