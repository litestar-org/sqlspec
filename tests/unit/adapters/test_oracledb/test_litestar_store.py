"""Unit tests for Oracle Litestar session store behavior."""

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.core import DB_TYPE_BLOB
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache
from sqlspec.adapters.oracledb.litestar import OracleAsyncStore, OracleSyncStore
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext


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


def test_oracle_stores_build_companion_index_for_bare_table() -> None:
    sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "litestar_session"}},
    )
    async_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "litestar_session"}},
    )
    sync_store = OracleSyncStore(sync_cfg)
    async_store = OracleAsyncStore(async_cfg)

    for store in (sync_store, async_store):
        targets = store.get_schema_targets()
        assert len(targets) == 1
        assert targets[0].table_name == "litestar_session"
        assert targets[0].schema is None
        assert [col.name for col in targets[0].create_table.columns] == [
            "session_id",
            "data",
            "expires_at",
            "created_at",
            "updated_at",
        ]
        indexes = store.get_companion_indexes()
        assert len(indexes) == 1
        assert indexes[0].name == "idx_litestar_session_expires_at"[:30]
        assert indexes[0].table == "litestar_session"
        assert indexes[0].schema is None
        assert "SQLCODE NOT IN (-955, -1408)" in indexes[0].ddl


@pytest.mark.anyio
async def test_oracle_reconcile_creates_missing_companion_index_sync_and_async() -> None:
    sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "litestar_session", "migration_mode": "reconcile"}},
    )
    async_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "litestar_session", "migration_mode": "reconcile"}},
    )
    existing_tables = [{"table_name": "LITESTAR_SESSION"}]
    existing_columns = [
        {"column_name": "SESSION_ID", "data_type": "VARCHAR2", "nullable": False},
        {"column_name": "DATA", "data_type": "BLOB", "nullable": False},
        {"column_name": "EXPIRES_AT", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": True},
        {"column_name": "CREATED_AT", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": False},
        {"column_name": "UPDATED_AT", "data_type": "TIMESTAMP(6) WITH TIME ZONE", "nullable": False},
    ]
    upper_index_row = [{"index_name": "IDX_LITESTAR_SESSION_EXPIRES_A", "table_name": "LITESTAR_SESSION"}]

    sync_missing = MagicMock()
    sync_missing.dialect = "oracle"
    sync_missing.data_dictionary.get_tables.return_value = existing_tables
    sync_missing.data_dictionary.get_columns.return_value = existing_columns
    sync_missing.data_dictionary.get_indexes.return_value = []

    assert litestar_reconcile(MigrationContext(config=sync_cfg, driver=sync_missing)) is None
    assert sync_missing.execute_script.call_count == 1
    sync_ddl = str(sync_missing.execute_script.call_args.args[0])
    assert "CREATE INDEX idx_litestar_session_expires_a" in sync_ddl
    assert "SQLCODE NOT IN (-955, -1408)" in sync_ddl
    sync_missing.commit.assert_not_called()

    sync_present = MagicMock()
    sync_present.dialect = "oracle"
    sync_present.data_dictionary.get_tables.return_value = existing_tables
    sync_present.data_dictionary.get_columns.return_value = existing_columns
    sync_present.data_dictionary.get_indexes.return_value = upper_index_row

    assert litestar_reconcile(MigrationContext(config=sync_cfg, driver=sync_present)) is None
    sync_present.execute_script.assert_not_called()
    sync_present.commit.assert_not_called()

    async_missing = SimpleNamespace(
        dialect="oracle",
        data_dictionary=SimpleNamespace(
            get_tables=AsyncMock(return_value=existing_tables),
            get_columns=AsyncMock(return_value=existing_columns),
            get_indexes=AsyncMock(return_value=[]),
        ),
        execute_script=AsyncMock(),
        commit=AsyncMock(),
    )
    coro = litestar_reconcile(MigrationContext(config=async_cfg, driver=cast("Any", async_missing)))
    assert inspect.isawaitable(coro)
    await coro
    async_missing.execute_script.assert_awaited_once()
    assert "SQLCODE NOT IN (-955, -1408)" in str(async_missing.execute_script.await_args.args[0])
    async_missing.commit.assert_not_awaited()

    async_present = SimpleNamespace(
        dialect="oracle",
        data_dictionary=SimpleNamespace(
            get_tables=AsyncMock(return_value=existing_tables),
            get_columns=AsyncMock(return_value=existing_columns),
            get_indexes=AsyncMock(return_value=upper_index_row),
        ),
        execute_script=AsyncMock(),
        commit=AsyncMock(),
    )
    coro_present = litestar_reconcile(MigrationContext(config=async_cfg, driver=cast("Any", async_present)))
    assert inspect.isawaitable(coro_present)
    await coro_present
    async_present.execute_script.assert_not_awaited()
    async_present.commit.assert_not_awaited()


def test_oracle_stores_accept_qualified_session_table_names() -> None:
    """Oracle sync and async stores accept validated schema.table names and scope targets and indexes."""
    sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "APP.litestar_session"}},
    )
    async_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"},
        extension_config={"litestar": {"session_table": "APP.litestar_session"}},
    )

    for store in (OracleSyncStore(sync_cfg), OracleAsyncStore(async_cfg)):
        target = store.get_schema_targets()[0]
        index = store.get_companion_indexes()[0]
        assert target.schema == "APP"
        assert target.table_name == "litestar_session"
        assert index.schema == "APP"
        assert index.table == "litestar_session"
        assert index.name == "idx_APP_litestar_session_expir"
        assert "ON APP.litestar_session(expires_at)" in index.ddl


@pytest.mark.parametrize("invalid_name", ["", "   ", "A.B.C", "APP..T", ".T", "APP.", "bad;drop", "x" * 64])
def test_oracle_stores_reject_invalid_qualified_session_table_names(invalid_name: str) -> None:
    """Oracle sync and async stores raise ValueError for malformed or unsafe session_table names."""
    sync_cfg = OracleSyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"}, extension_config={"litestar": {"session_table": invalid_name}}
    )
    async_cfg = OracleAsyncConfig(
        connection_config={"dsn": "localhost/FREEPDB1"}, extension_config={"litestar": {"session_table": invalid_name}}
    )
    with pytest.raises(ValueError):
        OracleSyncStore(sync_cfg)
    with pytest.raises(ValueError):
        OracleAsyncStore(async_cfg)
