"""Unit tests for Oracle Litestar session store behavior."""

from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb.core import DB_TYPE_BLOB
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache
from sqlspec.adapters.oracledb.litestar import OracleAsyncStore, OracleSyncStore


class _FakeCursor:
    """Cursor stub that records SQL calls and can return preset rows."""

    def __init__(self, rows: "list[tuple[Any, ...]] | None" = None) -> None:
        self.rows = list(rows or [])
        self.executed: list[tuple[str, dict[str, Any] | tuple[object, ...] | None]] = []
        self.rowcount = 0
        self.closed = False

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.closed = True

    def execute(self, sql: str, parameters: "dict[str, Any] | tuple[object, ...] | None" = None) -> None:
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
        self.created_lobs: list[tuple[Any, bytes]] = []

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def createlob(self, lob_type: Any, data: bytes) -> tuple[Any, bytes]:
        lob = (lob_type, data)
        self.created_lobs.append(lob)
        return lob

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


def _storage_config(settings: "dict[str, Any]") -> Any:
    config = cast("Any", _FakeOracleConfig(_FakeConnection(_FakeCursor())))
    config.extension_config = {"litestar": {"session_table": "oracle_sessions", **settings}}
    cache = OracleVersionCache()
    config._oracle_version_cache = cache
    return config


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
    db_now = datetime(2026, 1, 1, tzinfo=UTC)
    expires_at = db_now + timedelta(seconds=5)
    cursor = _FakeCursor(rows=[(expires_at, db_now)])
    connection = _FakeConnection(cursor)
    store = OracleSyncStore(cast("Any", _FakeOracleConfig(connection)))

    assert store._expires_in("session-1") == 5

    sql, parameters = cursor.executed[0]
    assert "SELECT expires_at, SYSTIMESTAMP" in sql
    assert parameters == ("session-1",)


def test_oracle_litestar_store_interval_partition_on_expires_at() -> None:
    """Litestar DDL applies gated compression, partitioning, and table options."""
    config = _storage_config({
        "compression": {"enabled": True, "algorithm": "basic"},
        "partitioning": {"strategy": "range", "interval": "month"},
        "table_options": "TABLESPACE session_data",
    })
    store = OracleSyncStore(config)

    sql = store._table_ddl()

    assert "ROW STORE COMPRESS BASIC" in sql
    assert "TABLESPACE session_data" in sql
    assert "PARTITION BY RANGE (expires_at)" in sql
    assert "NUMTOYMINTERVAL(1, ''MONTH'')" in sql


def test_oracle_sync_store_set_large_blob_single_statement() -> None:
    """set with >32KB payload should execute a single MERGE with a temporary BLOB locator."""
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)
    store = OracleSyncStore(cast("Any", _FakeOracleConfig(connection)))

    large_payload = b"x" * 65536
    store._set("session-large", large_payload, expires_in=timedelta(seconds=60))

    assert len(cursor.executed) == 1
    sql, parameters = cursor.executed[0]
    assert "MERGE INTO oracle_sessions t" in sql
    assert parameters is not None
    parameters = cast("dict[str, object]", parameters)
    assert parameters["session_id"] == "session-large"
    assert connection.created_lobs == [(DB_TYPE_BLOB, large_payload)]
    assert parameters["data"] == (DB_TYPE_BLOB, large_payload)
    assert parameters["expires_in_seconds"] == 60
    assert connection.commits == 1


def test_oracle_litestar_store_table_ddl_sanitizes_and_truncates_index_name() -> None:
    """_table_ddl replaces dots with underscores and truncates index identifier to <=30 chars."""
    config = _storage_config({})
    store = OracleSyncStore(config)
    store._table_name = "my_very_long_schema.oracle_sessions_table"

    ddl = store._table_ddl()

    assert "CREATE INDEX idx_my_very_long_schema_oracle" in ddl
    assert "idx_my_very_long_schema.oracle" not in ddl


def test_oracle_store_set_rolls_back_on_execute_failure() -> None:
    """Store mutations roll back connection on execution failure."""
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("execute failed")
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = connection
    config = MagicMock()
    config.provide_connection.return_value = context
    config.extension_config = {}

    store = OracleSyncStore(config)
    with pytest.raises(RuntimeError, match="execute failed"):
        store._set("fail-key", b"data")

    connection.rollback.assert_called_once()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_session_renewal_binds_each_sql_placeholder(is_async: bool) -> None:
    config = MagicMock()
    config.extension_config = {}
    cursor = MagicMock()
    cursor.fetchone.return_value = (b"payload", datetime(2026, 5, 10, tzinfo=UTC))
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    if is_async:
        cursor.execute = AsyncMock()
        cursor.fetchone = AsyncMock(return_value=cursor.fetchone.return_value)
        connection.commit = AsyncMock()
        config.provide_connection.return_value.__aenter__.return_value = connection
        store = OracleAsyncStore(config)
    else:
        config.provide_connection.return_value.__enter__.return_value = connection
        store = OracleSyncStore(config)

    result = await store.get("session-1", renew_for=60)

    assert result == b"payload"
    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args.args[1] == (60, 60, "session-1")
    connection.commit.assert_called_once()
