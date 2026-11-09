"""Unit tests for Db2 Litestar and event queue schema reconciliation."""

from typing import Any

import pytest

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.litestar import Db2AsyncStore, Db2SyncStore
from sqlspec.adapters.db2.litestar.store import _session_companion_index, session_index_ddl
from sqlspec.extensions.events.migrations.reconcile import reconcile as reconcile_events
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as reconcile_litestar
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter
from tests.unit.adapters.test_db2._fakes import DriverMode, FakeDb2Connection, FakeDb2Cursor, db2_description

pytestmark = pytest.mark.anyio

_TABLE_COLUMNS = ("schema_name", "table_name", "table_type")
_COLUMN_COLUMNS = (
    "schema_name",
    "table_name",
    "column_name",
    "data_type",
    "is_nullable",
    "column_default",
    "ordinal_position",
    "max_length",
    "numeric_scale",
    "is_primary",
    "is_unique",
    "identity_generation",
    "is_generated",
)
_INDEX_COLUMNS = (
    "schema_name",
    "table_name",
    "index_name",
    "column_name",
    "column_position",
    "is_unique",
    "is_primary",
)


def _col_row(
    table: str, name: str, dtype: str, *, nullable: bool, ordinal: int, primary: bool = False, schema: str = "DB2INST1"
) -> tuple[Any, ...]:
    return (
        schema,
        table,
        name,
        dtype,
        1 if nullable else 0,
        None,
        ordinal,
        255,
        0,
        1 if primary else 0,
        1 if primary else 0,
        "N",
        0,
    )


def _session_column_rows(table: str = "LITESTAR_SESSION") -> list[tuple[Any, ...]]:
    return [
        _col_row(table, "SESSION_ID", "VARCHAR", nullable=False, ordinal=1, primary=True),
        _col_row(table, "DATA", "BLOB", nullable=False, ordinal=2),
        _col_row(table, "EXPIRES_AT", "TIMESTAMP", nullable=True, ordinal=3),
        _col_row(table, "CREATED_AT", "TIMESTAMP", nullable=False, ordinal=4),
        _col_row(table, "UPDATED_AT", "TIMESTAMP", nullable=False, ordinal=5),
    ]


def _event_column_rows(table: str = "SQLSPEC_EVENT_QUEUE") -> list[tuple[Any, ...]]:
    return [
        _col_row(table, "EVENT_ID", "VARCHAR", nullable=False, ordinal=1, primary=True),
        _col_row(table, "CHANNEL", "VARCHAR", nullable=False, ordinal=2),
        _col_row(table, "PAYLOAD_JSON", "CLOB", nullable=False, ordinal=3),
        _col_row(table, "METADATA_JSON", "CLOB", nullable=True, ordinal=4),
        _col_row(table, "STATUS", "VARCHAR", nullable=False, ordinal=5),
        _col_row(table, "AVAILABLE_AT", "TIMESTAMP", nullable=False, ordinal=6),
        _col_row(table, "LEASE_EXPIRES_AT", "TIMESTAMP", nullable=True, ordinal=7),
        _col_row(table, "ATTEMPTS", "INTEGER", nullable=False, ordinal=8),
        _col_row(table, "CREATED_AT", "TIMESTAMP", nullable=False, ordinal=9),
        _col_row(table, "ACKNOWLEDGED_AT", "TIMESTAMP", nullable=True, ordinal=10),
    ]


def _executed_ddl(connection: FakeDb2Connection) -> list[str]:
    return [
        sql.strip()
        for cursor in connection.cursors
        for sql, _ in cursor.executed
        if not sql.lstrip().upper().startswith(("SELECT", "WITH", "VALUES"))
    ]


@pytest.mark.parametrize("config_class", [Db2SyncConfig, Db2AsyncConfig])
def test_validate_builtin_reconcile_adapter_accepts_db2(config_class: type[Any]) -> None:
    """validate_builtin_reconcile_adapter accepts both Db2SyncConfig and Db2AsyncConfig."""
    config = config_class(connection_config={"database": "TESTDB"})
    assert validate_builtin_reconcile_adapter(config) == "db2"


def test_session_companion_index_uses_catalog_folded_names() -> None:
    """_session_companion_index returns upper-folded catalog table, schema, and index names."""
    unqualified = _session_companion_index("litestar_session")
    assert unqualified.name == "IX_LITESTAR_SESSION_EXP"
    assert unqualified.table == "LITESTAR_SESSION"
    assert unqualified.schema is None
    assert unqualified.ddl == session_index_ddl("litestar_session")

    qualified = _session_companion_index("app_schema.litestar_session")
    assert qualified.name == "IX_LITESTAR_SESSION_EXP"
    assert qualified.table == "LITESTAR_SESSION"
    assert qualified.schema == "APP_SCHEMA"
    assert qualified.ddl == session_index_ddl("app_schema.litestar_session")


async def test_db2_litestar_reconcile_repairs_missing_session_index(db2_mode: DriverMode) -> None:
    """Litestar reconcile repairs a missing Db2 session expiry index once without committing."""
    table_rows = [("DB2INST1", "LITESTAR_SESSION", "BASE TABLE")]
    cursors = [
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=_session_column_rows(), description=db2_description(*_COLUMN_COLUMNS)),
        FakeDb2Cursor(rows=[], description=db2_description(*_INDEX_COLUMNS)),
        FakeDb2Cursor(),
    ]
    connection = FakeDb2Connection(cursors)
    driver = db2_mode.driver(connection)
    config = db2_mode.config(
        connection_config={"database": "TESTDB"}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )
    store = Db2AsyncStore(config) if db2_mode.is_async else Db2SyncStore(config)
    assert [idx.name for idx in store.get_companion_indexes()] == ["IX_LITESTAR_SESSION_EXP"]

    ctx = MigrationContext(config=config, dialect="db2", driver=driver)
    await db2_mode.call(reconcile_litestar, ctx)

    assert _executed_ddl(connection) == [session_index_ddl("litestar_session")]
    assert connection.commits == 0


async def test_db2_litestar_reconcile_skips_present_uppercase_index(db2_mode: DriverMode) -> None:
    """Litestar reconcile emits no DDL when the catalog already reports the upper-case Db2 index."""
    table_rows = [("DB2INST1", "LITESTAR_SESSION", "BASE TABLE")]
    index_rows = [("DB2INST1", "LITESTAR_SESSION", "IX_LITESTAR_SESSION_EXP", "EXPIRES_AT", 1, 0, 0)]
    cursors = [
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=_session_column_rows(), description=db2_description(*_COLUMN_COLUMNS)),
        FakeDb2Cursor(rows=index_rows, description=db2_description(*_INDEX_COLUMNS)),
    ]
    connection = FakeDb2Connection(cursors)
    driver = db2_mode.driver(connection)
    config = db2_mode.config(
        connection_config={"database": "TESTDB"}, extension_config={"litestar": {"migration_mode": "reconcile"}}
    )

    ctx = MigrationContext(config=config, dialect="db2", driver=driver)
    await db2_mode.call(reconcile_litestar, ctx)

    assert _executed_ddl(connection) == []
    assert connection.commits == 0


async def test_db2_events_reconcile_repairs_missing_index_and_skips_when_present(db2_mode: DriverMode) -> None:
    """Events reconcile repairs the Db2 queue index once when absent and skips DDL when present."""
    table_rows = [("DB2INST1", "SQLSPEC_EVENT_QUEUE", "BASE TABLE")]
    missing_cursors = [
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=_event_column_rows(), description=db2_description(*_COLUMN_COLUMNS)),
        FakeDb2Cursor(rows=[], description=db2_description(*_INDEX_COLUMNS)),
        FakeDb2Cursor(),
    ]
    missing_conn = FakeDb2Connection(missing_cursors)
    config = db2_mode.config(
        connection_config={"database": "TESTDB"}, extension_config={"events": {"migration_mode": "reconcile"}}
    )
    await db2_mode.call(
        reconcile_events, MigrationContext(config=config, dialect="db2", driver=db2_mode.driver(missing_conn))
    )
    assert _executed_ddl(missing_conn) == [
        "CREATE INDEX idx_sqlspec_event_queue_channel_status ON sqlspec_event_queue(channel, status, available_at)"
    ]
    assert missing_conn.commits == 0

    present_index = [("DB2INST1", "SQLSPEC_EVENT_QUEUE", "IDX_SQLSPEC_EVENT_QUEUE_CHANNEL_STATUS", "CHANNEL", 1, 0, 0)]
    present_cursors = [
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=_event_column_rows(), description=db2_description(*_COLUMN_COLUMNS)),
        FakeDb2Cursor(rows=present_index, description=db2_description(*_INDEX_COLUMNS)),
    ]
    present_conn = FakeDb2Connection(present_cursors)
    await db2_mode.call(
        reconcile_events, MigrationContext(config=config, dialect="db2", driver=db2_mode.driver(present_conn))
    )
    assert _executed_ddl(present_conn) == []
