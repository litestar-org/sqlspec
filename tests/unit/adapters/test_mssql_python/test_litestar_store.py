"""Unit tests for the mssql-python Litestar store."""

from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore


class FakeResult:
    """Small SQL result stub exposing rows_affected."""

    def __init__(self, rows_affected: int) -> None:
        self.rows_affected = rows_affected


class FakeSession:
    """Async driver stub that records store SQL calls."""

    def __init__(self, rows: list[Any] | None = None, rows_affected: int = 0) -> None:
        self.rows = list(rows or [])
        self.rows_affected = rows_affected
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.scripts: list[str] = []
        self.commits = 0

    async def execute(self, sql: str, *parameters: Any, **kwargs: Any) -> FakeResult:
        self.executed.append((sql, parameters))
        return FakeResult(self.rows_affected)

    async def execute_script(self, sql: str, *parameters: Any, **kwargs: Any) -> FakeResult:
        self.scripts.append(sql)
        return FakeResult(self.rows_affected)

    async def select_one_or_none(self, sql: str, *parameters: Any, **kwargs: Any) -> Any:
        self.executed.append((sql, parameters))
        if self.rows:
            return self.rows.pop(0)
        return None

    async def commit(self) -> None:
        self.commits += 1


class FakeSessionContext:
    """Async context manager returning a fake session."""

    def __init__(self, session: FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> FakeSession:
        return self.session

    async def __aexit__(self, *_: object) -> None:
        return None


class FakeConfig:
    """Config stub implementing provide_session."""

    def __init__(self, session: FakeSession, table_name: str = "mssql_sessions") -> None:
        self.extension_config = {"litestar": {"session_table": table_name}}
        self.session = session

    def provide_session(self) -> FakeSessionContext:
        return FakeSessionContext(self.session)


def test_mssql_python_store_imports() -> None:
    """The mssql_python Litestar namespace should export the store."""
    assert MssqlPythonStore.__name__ == "MssqlPythonStore"


def test_mssql_python_store_create_table_uses_tsql_guards() -> None:
    """The store DDL should use SQL Server idempotent guards and binary payload storage."""
    store = MssqlPythonStore(cast("Any", FakeConfig(FakeSession())))

    ddl = store._get_create_table_sql()

    assert "IF NOT EXISTS" in ddl
    assert "sys.tables" in ddl
    assert "VARBINARY(MAX)" in ddl
    assert "DATETIME2(6)" in ddl
    assert "WHERE expires_at IS NOT NULL" in ddl


async def test_mssql_python_store_set_uses_merge_with_positional_parameters() -> None:
    """set should upsert with MERGE and pass the key/data/expiry as positional parameters."""
    session = FakeSession()
    store = MssqlPythonStore(cast("Any", FakeConfig(session)))

    await store.set("session-1", "payload", expires_in=60)

    sql, parameters = session.executed[0]
    assert "MERGE INTO mssql_sessions" in sql
    assert parameters[0] == ("session-1", b"payload", parameters[0][2])
    assert isinstance(parameters[0][2], datetime)
    assert session.commits == 1


async def test_mssql_python_store_get_renews_unexpired_session() -> None:
    """get should return bytes and renew expiry only when the existing row had an expiry."""
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    session = FakeSession(rows=[{"data": b"payload", "expires_at": future}])
    store = MssqlPythonStore(cast("Any", FakeConfig(session)))

    data = await store.get("session-1", renew_for=60)

    assert data == b"payload"
    assert any("UPDATE mssql_sessions" in sql for sql, _ in session.executed)
    assert session.commits == 1


async def test_mssql_python_store_get_deletes_expired_session() -> None:
    """get should lazily delete expired rows before returning None."""
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    session = FakeSession(rows=[{"data": b"payload", "expires_at": past}])
    store = MssqlPythonStore(cast("Any", FakeConfig(session)))

    data = await store.get("session-1")

    assert data is None
    assert any("DELETE FROM mssql_sessions" in sql for sql, _ in session.executed)
    assert session.commits == 1


async def test_mssql_python_store_delete_expired_returns_rows_affected() -> None:
    """delete_expired should return the SQLSpec result rowcount."""
    session = FakeSession(rows_affected=3)
    store = MssqlPythonStore(cast("Any", FakeConfig(session)))

    count = await store.delete_expired()

    assert count == 3
    assert session.commits == 1
