"""Unit tests for the mssql-python Litestar store."""

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext


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
    assert "CREATE TABLE [dbo].[mssql_sessions]" in session.scripts[0]
    assert session.commits == 1


async def test_mssql_python_store_set_uses_merge_with_positional_parameters() -> None:
    """set should upsert with MERGE and pass the key/data/expiry as positional parameters."""
    connection = FakeConnection()
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    await store.set("session-1", "payload", expires_in=60)

    sql, parameters = connection.executed[0]
    assert "MERGE INTO [dbo].[mssql_sessions]" in sql
    assert parameters[0] == "session-1"
    assert parameters[1] == b"payload"
    assert isinstance(parameters[2], datetime)
    assert connection.commits == 1


async def test_mssql_python_store_get_renews_unexpired_session() -> None:
    """get should return bytes and renew expiry only when the existing row had an expiry."""
    future = datetime.now(UTC) + timedelta(minutes=5)
    connection = FakeConnection(rows=[{"data": b"payload", "expires_at": future}])
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    data = await store.get("session-1", renew_for=60)

    assert data == b"payload"
    assert any("UPDATE [dbo].[mssql_sessions]" in sql for sql, _ in connection.executed)
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


def test_mssql_python_store_supports_schema_qualified_and_quoted_table_names() -> None:
    """Litestar store DDL should split schema.table and escape single quotes in metadata probes."""
    store = MssqlPythonStore(cast("Any", FakeConfig()))
    store._table_name = "auth.user's_sessions"

    ddl = store._table_ddl()
    drop_sql = store._drop_table_sql()

    assert "SCHEMA_ID(N'auth')" in ddl
    assert "WHERE name = N'user''s_sessions'" in ddl
    assert "CREATE TABLE [auth].[user's_sessions]" in ddl

    assert "CREATE INDEX [IX_user's_sessions_expires_at] ON [auth].[user's_sessions](expires_at)" in " ".join(
        ddl.split()
    )
    assert drop_sql == [
        "IF OBJECT_ID(N'[auth].[user''s_sessions]', N'U') IS NOT NULL DROP TABLE [auth].[user's_sessions];"
    ]


async def test_mssql_python_store_delete_expired_returns_zero_when_negative() -> None:
    """delete_expired should return 0 when rowcount is -1."""
    connection = FakeConnection(rowcount=-1)
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    count = await store.delete_expired()

    assert count == 0
    assert connection.commits == 1


async def test_mssql_python_store_set_uses_holdlock() -> None:
    """_set executes MERGE WITH (HOLDLOCK) to prevent concurrent insert races."""
    connection = FakeConnection()
    store = MssqlPythonStore(cast("Any", FakeConfig(connection=connection)))

    await store.set("session-1", b"payload", expires_in=60)

    merge_sql, _ = connection.executed[0]
    assert "MERGE INTO [dbo].[mssql_sessions] WITH (HOLDLOCK) AS target" in merge_sql


def test_mssql_python_store_scopes_bare_table_to_dbo() -> None:
    """Unqualified session tables scope SchemaTarget and CompanionIndex to dbo."""
    store = MssqlPythonStore(cast("Any", FakeConfig(table_name="litestar_session")))

    targets = store.get_schema_targets()
    indexes = store.get_companion_indexes()

    assert len(targets) == 1
    assert targets[0].schema == "dbo"
    assert targets[0].table_name == "litestar_session"
    assert [col.name for col in targets[0].create_table.columns] == [
        "session_id",
        "data",
        "expires_at",
        "created_at",
        "updated_at",
    ]
    assert targets[0].constraint_columns == frozenset({"session_id"})
    assert len(indexes) == 1
    assert indexes[0].name == "IX_litestar_session_expires_at"
    assert indexes[0].table == "litestar_session"
    assert indexes[0].schema == "dbo"
    assert "WHERE expires_at IS NOT NULL" in indexes[0].ddl


def test_mssql_python_store_reconciles_missing_companion_index_on_existing_table() -> None:
    """Reconcile queries dbo metadata and restores missing filtered expiry index without committing."""
    config = MssqlPythonConfig(
        extension_config={"litestar": {"session_table": "litestar_session", "migration_mode": "reconcile"}}
    )
    existing_columns = [
        {"column_name": "session_id", "data_type": "nvarchar", "nullable": False},
        {"column_name": "data", "data_type": "varbinary", "nullable": False},
        {"column_name": "expires_at", "data_type": "datetime2", "nullable": True},
        {"column_name": "created_at", "data_type": "datetime2", "nullable": False},
        {"column_name": "updated_at", "data_type": "datetime2", "nullable": False},
    ]

    missing_driver = MagicMock()
    missing_driver.dialect = "tsql"
    missing_driver.data_dictionary.get_tables.return_value = [{"table_name": "litestar_session", "schema_name": "dbo"}]
    missing_driver.data_dictionary.get_columns.return_value = existing_columns
    missing_driver.data_dictionary.get_indexes.return_value = []

    assert litestar_reconcile(MigrationContext(config=config, driver=missing_driver)) is None

    missing_driver.data_dictionary.get_tables.assert_called_once_with(missing_driver, schema="dbo")
    missing_driver.data_dictionary.get_columns.assert_called_once_with(missing_driver, "litestar_session", schema="dbo")
    missing_driver.data_dictionary.get_indexes.assert_called_once_with(
        missing_driver, table="litestar_session", schema="dbo"
    )
    assert missing_driver.execute_script.call_count == 1
    script_sql = str(missing_driver.execute_script.call_args.args[0])
    assert "CREATE INDEX [IX_litestar_session_expires_at]" in script_sql
    assert "ON [dbo].[litestar_session](expires_at)" in script_sql
    assert "WHERE expires_at IS NOT NULL" in script_sql
    missing_driver.commit.assert_not_called()

    present_driver = MagicMock()
    present_driver.dialect = "tsql"
    present_driver.data_dictionary.get_tables.return_value = [{"table_name": "litestar_session", "schema_name": "dbo"}]
    present_driver.data_dictionary.get_columns.return_value = existing_columns
    present_driver.data_dictionary.get_indexes.return_value = [
        {"index_name": "IX_LITESTAR_SESSION_EXPIRES_AT", "table_name": "LITESTAR_SESSION", "schema_name": "DBO"}
    ]

    assert litestar_reconcile(MigrationContext(config=config, driver=present_driver)) is None
    present_driver.execute_script.assert_not_called()
    present_driver.commit.assert_not_called()
