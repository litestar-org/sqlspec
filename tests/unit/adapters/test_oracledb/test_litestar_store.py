# pyright: reportPrivateUsage=false
"""Unit tests for Oracle Litestar session store behavior."""

from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlspec.adapters.oracledb.litestar import OracleSyncStore


class _FakeCursor:
    """Cursor stub that records SQL calls and can return preset rows."""

    def __init__(self, rows: "list[tuple[Any, ...]] | None" = None) -> None:
        self.rows = list(rows or [])
        self.executed: list[tuple[str, dict[str, Any] | None]] = []
        self.rowcount = 0

    def execute(self, sql: str, parameters: "dict[str, Any] | None" = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> "tuple[Any, ...] | None":
        if not self.rows:
            return None
        return self.rows.pop(0)


class _FakeConnection:
    """Connection stub exposing the minimal oracledb sync API used by the store."""

    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.commits = 0

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


class _FakeConnectionContext:
    """Context manager returning a fake Oracle connection."""

    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> _FakeConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class _FakeOracleConfig:
    """Config stub implementing provide_connection."""

    def __init__(self, connection: _FakeConnection) -> None:
        self.extension_config = {"litestar": {"session_table": "oracle_sessions"}}
        self.connection = connection

    def provide_connection(self) -> _FakeConnectionContext:
        return _FakeConnectionContext(self.connection)


def test_oracle_sync_store_set_calculates_expiry_from_database_clock() -> None:
    """set should bind TTL seconds and let Oracle derive expires_at from SYSTIMESTAMP."""
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)
    store = OracleSyncStore(cast("Any", _FakeOracleConfig(connection)))

    store._set("session-1", "payload", expires_in=timedelta(seconds=3))

    sql, parameters = cursor.executed[0]
    assert "SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')" in sql
    assert parameters == {"session_id": "session-1", "data": b"payload", "expires_in_seconds": 3}
    assert connection.commits == 1


def test_oracle_sync_store_expires_in_uses_database_clock() -> None:
    """expires_in should compare expires_at to Oracle's SYSTIMESTAMP value."""
    db_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expires_at = db_now + timedelta(seconds=5)
    cursor = _FakeCursor(rows=[(expires_at, db_now)])
    connection = _FakeConnection(cursor)
    store = OracleSyncStore(cast("Any", _FakeOracleConfig(connection)))

    assert store._expires_in("session-1") == 5

    sql, parameters = cursor.executed[0]
    assert "SELECT expires_at, SYSTIMESTAMP" in sql
    assert parameters == {"session_id": "session-1"}
