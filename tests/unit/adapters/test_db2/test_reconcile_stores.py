"""Unit tests for Db2 Litestar, event queue, and ADK schema reconciliation."""

from typing import Any

import pytest

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.litestar import Db2AsyncStore, Db2SyncStore
from sqlspec.adapters.db2.litestar.store import _session_companion_index, session_index_ddl
from sqlspec.extensions.adk.migrations.reconcile import reconcile as reconcile_adk
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


def _adk_table_columns(table: str) -> list[tuple[Any, ...]]:
    if table == "ADK_SESSION":
        return [
            _col_row(table, "ID", "VARCHAR", nullable=False, ordinal=1, primary=True),
            _col_row(table, "APP_NAME", "VARCHAR", nullable=False, ordinal=2),
            _col_row(table, "USER_ID", "VARCHAR", nullable=False, ordinal=3),
            _col_row(table, "STATE", "CLOB", nullable=False, ordinal=4),
            _col_row(table, "CREATE_TIME", "TIMESTAMP", nullable=False, ordinal=5),
            _col_row(table, "UPDATE_TIME", "TIMESTAMP", nullable=False, ordinal=6),
        ]
    if table == "ADK_EVENT":
        return [
            _col_row(table, "ID", "VARCHAR", nullable=False, ordinal=1, primary=True),
            _col_row(table, "APP_NAME", "VARCHAR", nullable=False, ordinal=2),
            _col_row(table, "USER_ID", "VARCHAR", nullable=False, ordinal=3),
            _col_row(table, "SESSION_ID", "VARCHAR", nullable=False, ordinal=4),
            _col_row(table, "INVOCATION_ID", "VARCHAR", nullable=False, ordinal=5),
            _col_row(table, "TIMESTAMP", "TIMESTAMP", nullable=False, ordinal=6),
            _col_row(table, "EVENT_DATA", "CLOB", nullable=False, ordinal=7),
        ]
    if table == "ADK_APP_STATE":
        return [
            _col_row(table, "APP_NAME", "VARCHAR", nullable=False, ordinal=1, primary=True),
            _col_row(table, "STATE", "CLOB", nullable=False, ordinal=2),
            _col_row(table, "UPDATE_TIME", "TIMESTAMP", nullable=False, ordinal=3),
        ]
    if table == "ADK_USER_STATE":
        return [
            _col_row(table, "APP_NAME", "VARCHAR", nullable=False, ordinal=1, primary=True),
            _col_row(table, "USER_ID", "VARCHAR", nullable=False, ordinal=2, primary=True),
            _col_row(table, "STATE", "CLOB", nullable=False, ordinal=3),
            _col_row(table, "UPDATE_TIME", "TIMESTAMP", nullable=False, ordinal=4),
        ]
    if table == "ADK_INTERNAL_METADATA":
        return [
            _col_row(table, "KEY", "VARCHAR", nullable=False, ordinal=1, primary=True),
            _col_row(table, "VALUE", "VARCHAR", nullable=False, ordinal=2),
        ]
    return [
        _col_row(table, "ID", "VARCHAR", nullable=False, ordinal=1, primary=True),
        _col_row(table, "SESSION_ID", "VARCHAR", nullable=False, ordinal=2),
        _col_row(table, "APP_NAME", "VARCHAR", nullable=False, ordinal=3),
        _col_row(table, "USER_ID", "VARCHAR", nullable=False, ordinal=4),
        _col_row(table, "SCOPE", "VARCHAR", nullable=False, ordinal=5),
        _col_row(table, "EVENT_ID", "VARCHAR", nullable=False, ordinal=6),
        _col_row(table, "AUTHOR", "VARCHAR", nullable=True, ordinal=7),
        _col_row(table, "TIMESTAMP", "TIMESTAMP", nullable=False, ordinal=8),
        _col_row(table, "CONTENT_JSON", "CLOB", nullable=False, ordinal=9),
        _col_row(table, "CONTENT_TEXT", "CLOB", nullable=False, ordinal=10),
        _col_row(table, "METADATA_JSON", "CLOB", nullable=True, ordinal=11),
        _col_row(table, "INSERTED_AT", "TIMESTAMP", nullable=False, ordinal=12),
    ]


async def test_db2_adk_reconcile_entrypoint_repairs_session_and_memory_indexes(db2_mode: DriverMode) -> None:
    """ADK reconcile repairs a single missing Db2 memory index across session and memory stores without committing."""
    adk_tables = ["ADK_SESSION", "ADK_EVENT", "ADK_APP_STATE", "ADK_USER_STATE", "ADK_INTERNAL_METADATA", "ADK_MEMORY"]
    table_rows = [("DB2INST1", name, "BASE TABLE") for name in adk_tables]
    session_indexes = [
        ("DB2INST1", "ADK_SESSION", "IDX_ADK_SESSION_APP_USER", "APP_NAME", 1, 0, 0),
        ("DB2INST1", "ADK_SESSION", "IDX_ADK_SESSION_UPDATE_TIME", "UPDATE_TIME", 1, 0, 0),
    ]
    event_indexes = [
        ("DB2INST1", "ADK_EVENT", "IDX_ADK_EVENT_SCOPE", "APP_NAME", 1, 0, 0),
        ("DB2INST1", "ADK_EVENT", "IDX_ADK_EVENT_SESSION", "SESSION_ID", 1, 0, 0),
        ("DB2INST1", "ADK_EVENT", "IDX_ADK_EVENT_INVOCATION", "INVOCATION_ID", 1, 0, 0),
        ("DB2INST1", "ADK_EVENT", "IDX_ADK_EVENT_TIMESTAMP", "TIMESTAMP", 1, 0, 0),
        ("DB2INST1", "ADK_EVENT", "IDX_ADK_EVENT_APP_TIMESTAMP", "APP_NAME", 1, 0, 0),
    ]
    memory_indexes = [
        ("DB2INST1", "ADK_MEMORY", "IDX_ADK_MEMORY_SCOPE", "APP_NAME", 1, 0, 0),
        ("DB2INST1", "ADK_MEMORY", "IDX_ADK_MEMORY_SESSION", "SESSION_ID", 1, 0, 0),
    ]
    cursors = [
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        FakeDb2Cursor(rows=table_rows, description=db2_description(*_TABLE_COLUMNS)),
        *(
            FakeDb2Cursor(rows=_adk_table_columns(name), description=db2_description(*_COLUMN_COLUMNS))
            for name in adk_tables
        ),
        FakeDb2Cursor(rows=session_indexes, description=db2_description(*_INDEX_COLUMNS)),
        FakeDb2Cursor(rows=event_indexes, description=db2_description(*_INDEX_COLUMNS)),
        FakeDb2Cursor(rows=memory_indexes, description=db2_description(*_INDEX_COLUMNS)),
        FakeDb2Cursor(),
    ]
    connection = FakeDb2Connection(cursors)
    config = db2_mode.config(
        connection_config={"database": "TESTDB"},
        extension_config={"adk": {"migration_mode": "reconcile", "enable_memory": True}},
    )

    await db2_mode.call(
        reconcile_adk, MigrationContext(config=config, dialect="db2", driver=db2_mode.driver(connection))
    )

    assert _executed_ddl(connection) == ["CREATE INDEX idx_adk_memory_inserted ON adk_memory (inserted_at)"]
    assert connection.commits == 0


class _DropCatalog:
    """Catalog fake whose DROP TABLE fails for absent tables, like Db2 SQLSTATE 42704."""

    def __init__(self, tables: "set[str]") -> None:
        self.tables = tables
        self.dropped: list[str] = []

    def probe(self, parameters: "tuple[str | None, str]") -> "dict[str, int] | None":
        return {"found": 1} if parameters[1] in self.tables else None

    def drop(self, statement: str) -> None:
        name = statement.removeprefix("DROP TABLE ").upper()
        if name not in self.tables:
            msg = f"SQL0204N {name} is an undefined name. SQLSTATE=42704"
            raise RuntimeError(msg)
        self.tables.discard(name)
        self.dropped.append(name)


class _SyncDropDriver:
    def __init__(self, catalog: _DropCatalog) -> None:
        self._catalog = catalog

    def select_one_or_none(self, _sql: str, parameters: "tuple[str | None, str]") -> "dict[str, int] | None":
        return self._catalog.probe(parameters)

    def execute_script(self, statement: str) -> None:
        self._catalog.drop(statement)


class _AsyncDropDriver:
    def __init__(self, catalog: _DropCatalog) -> None:
        self._catalog = catalog

    async def select_one_or_none(self, _sql: str, parameters: "tuple[str | None, str]") -> "dict[str, int] | None":
        return self._catalog.probe(parameters)

    async def execute_script(self, statement: str) -> None:
        self._catalog.drop(statement)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_db2_extension_downgrades_skip_tables_already_dropped(is_async: bool) -> None:
    """Retrying a partially applied downgrade drops only the tables the catalog still lists."""
    from sqlspec.adapters.db2.adk import (
        Db2AsyncADKMemoryStore,
        Db2AsyncADKStore,
        Db2SyncADKMemoryStore,
        Db2SyncADKStore,
    )
    from sqlspec.adapters.db2.events import Db2AsyncEventQueueStore, Db2SyncEventQueueStore
    from sqlspec.extensions.adk.migrations import reconcile as adk_module
    from sqlspec.extensions.events.migrations import reconcile as events_module
    from sqlspec.extensions.litestar.migrations import reconcile as litestar_module

    extension_config: Any = {"adk": {"enable_memory": True}}
    if is_async:
        config: Any = Db2AsyncConfig(connection_config={"database": "SAMPLE"}, extension_config=extension_config)
        adk_stores: list[Any] = [Db2AsyncADKStore(config), Db2AsyncADKMemoryStore(config)]
        session_store: Any = Db2AsyncStore(config)
        queue_store: Any = Db2AsyncEventQueueStore(config)
    else:
        config = Db2SyncConfig(connection_config={"database": "SAMPLE"}, extension_config=extension_config)
        adk_stores = [Db2SyncADKStore(config), Db2SyncADKMemoryStore(config)]
        session_store = Db2SyncStore(config)
        queue_store = Db2SyncEventQueueStore(config)

    async def run(module: Any, *args: Any) -> None:
        if is_async:
            await module._downgrade_async(*args)
        else:
            module._downgrade_sync(*args)

    catalog = _DropCatalog({"ADK_SESSION", "ADK_EVENT", "ADK_MEMORY", "LITESTAR_SESSION"})
    driver: Any = _AsyncDropDriver(catalog) if is_async else _SyncDropDriver(catalog)
    await run(adk_module, driver, adk_stores)
    await run(adk_module, driver, adk_stores)
    await run(litestar_module, driver, session_store)
    await run(litestar_module, driver, session_store)
    await run(events_module, driver, queue_store, True)
    assert catalog.dropped == ["ADK_MEMORY", "ADK_EVENT", "ADK_SESSION", "LITESTAR_SESSION"]
    assert catalog.tables == set()
