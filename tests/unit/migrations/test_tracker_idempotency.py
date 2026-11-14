"""Unit tests for migration tracker behavior."""

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgSyncDriver
from sqlspec.adapters.mssql_python.core import create_mapped_exception as mssql_python_map_exception
from sqlspec.adapters.mssql_python.core import default_statement_config as mssql_python_statement_config
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.adapters.pymssql.core import create_mapped_exception as pymssql_map_exception
from sqlspec.adapters.pymssql.core import default_statement_config as pymssql_statement_config
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL, StatementConfig
from sqlspec.core.result import SQLResult
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker


def test_sync_update_version_record_success() -> None:
    """Test sync update succeeds when old version exists."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    mock_result = Mock()
    mock_result.rows_affected = 1
    driver.execute.return_value = mock_result

    tracker.update_version_record(driver, "20251011120000", "0001")

    update_call = driver.execute.call_args_list[0]
    update_sql = str(update_call[0][0])
    assert "UPDATE" in update_sql
    assert "ddl_migrations" in update_sql


def test_sync_tracker_qualifies_table_sql_when_schema_is_configured() -> None:
    """The base tracker should render the tracking table in the configured schema."""
    tracker = SyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="history")

    assert tracker.version_table == "history.ddl_migrations"
    rendered_statements = [
        str(tracker._tracking_table_ddl()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._current_version_query()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._applied_migrations_query()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._next_execution_sequence_query()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._record_migration_statement("0001", "sequential", 1, "init", 10, "abc", "tester")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._remove_migration_statement("0001")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._update_version_statement("20250101000000", "0001", "sequential")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._delete_versions_statement(["0001"])),  # pyright: ignore[reportPrivateUsage]
        str(
            tracker._record_squashed_migration_statement("0002", "sequential", 2, "squash", 0, "def", "tester", "0001")
        ),  # pyright: ignore[reportPrivateUsage]
    ]

    assert all('"history"."ddl_migrations"' in statement for statement in rendered_statements)


def test_sync_tracker_keeps_bare_table_sql_without_schema() -> None:
    """Existing migration tracker SQL should stay unqualified by default."""
    tracker = SyncMigrationTracker(version_table_name="ddl_migrations")

    assert tracker.version_table == "ddl_migrations"
    assert "history.ddl_migrations" not in str(tracker._tracking_table_ddl())  # pyright: ignore[reportPrivateUsage]


def test_sync_tracker_introspects_bare_table_name_with_schema() -> None:
    """Qualified tracker SQL should not leak into data-dictionary table-name arguments."""
    tracker = SyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="history")
    driver = Mock()
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": column.name}
        for column in tracker._tracking_table_ddl().columns  # pyright: ignore[reportPrivateUsage]
    ]

    tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.data_dictionary.get_columns.assert_called_once_with(driver, "ddl_migrations", schema="history")
    driver.execute.assert_not_called()


@pytest.mark.anyio
async def test_async_tracker_introspects_bare_table_name_with_schema() -> None:
    """Async qualified tracker SQL should keep introspection table/schema separate."""
    tracker = AsyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="history")
    driver = MagicMock()
    driver.data_dictionary.get_columns = AsyncMock(
        return_value=[
            {"column_name": column.name}
            for column in tracker._tracking_table_ddl().columns  # pyright: ignore[reportPrivateUsage]
        ]
    )
    driver.execute = AsyncMock()

    await tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.data_dictionary.get_columns.assert_awaited_once_with(driver, "ddl_migrations", schema="history")
    driver.execute.assert_not_awaited()


def test_oracle_tracker_quotes_lowercase_qualified_tracking_table() -> None:
    """Oracle tracker should normalize lowercase names before quoting them."""
    tracker = OracleSyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="app_owner")

    assert tracker.version_table == '"APP_OWNER"."DDL_MIGRATIONS"'
    assert tracker.version_table_schema == "app_owner"
    assert tracker.version_table_name == "ddl_migrations"


def test_oracle_tracker_preserves_mixed_case_qualified_tracking_table() -> None:
    """Oracle tracker should preserve mixed-case schema/table names as quoted identifiers."""
    tracker = OracleSyncMigrationTracker(version_table_name="DdlMigrations", version_table_schema="AppOwner")

    assert tracker.version_table == '"AppOwner"."DdlMigrations"'
    assert tracker.version_table_schema == "AppOwner"
    assert tracker.version_table_name == "DdlMigrations"


@pytest.mark.parametrize("tracker_type", [OracleSyncMigrationTracker, OracleAsyncMigrationTracker])
def test_oracle_tracking_ddl_preserves_explicit_case(tracker_type: Any) -> None:
    tracker = tracker_type(version_table_name='"app"."Migration Tracker"')
    ddl = tracker._tracking_table_ddl().to_statement(StatementConfig(dialect="oracle"))

    assert tracker.version_table == '"app"."Migration Tracker"'
    assert tracker.version_table_name == "Migration Tracker"
    assert tracker.version_table_schema == "app"
    assert ddl.sql.startswith('CREATE TABLE "app"."Migration Tracker" (')
    query = tracker._current_version_query().to_statement(StatementConfig(dialect="oracle"))
    assert 'FROM "app"."Migration Tracker"' in query.sql


def test_oracle_sync_tracker_introspects_unmodified_table_name_with_schema() -> None:
    """Oracle data-dictionary calls should preserve mixed-case configured identifiers."""
    tracker = OracleSyncMigrationTracker(version_table_name="DdlMigrations", version_table_schema="AppOwner")
    driver = Mock()
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": column.name}
        for column in tracker._tracking_table_ddl().columns  # pyright: ignore[reportPrivateUsage]
    ]

    tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.data_dictionary.get_columns.assert_called_once_with(driver, "DdlMigrations", schema="AppOwner")
    driver.execute.assert_not_called()


@pytest.mark.anyio
async def test_oracle_async_tracker_introspects_unmodified_table_name_with_schema() -> None:
    """Oracle async data-dictionary calls should preserve mixed-case configured identifiers."""
    tracker = OracleAsyncMigrationTracker(version_table_name="DdlMigrations", version_table_schema="AppOwner")
    driver = MagicMock()
    driver.data_dictionary.get_columns = AsyncMock(
        return_value=[
            {"column_name": column.name}
            for column in tracker._tracking_table_ddl().columns  # pyright: ignore[reportPrivateUsage]
        ]
    )
    driver.execute = AsyncMock()

    await tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.data_dictionary.get_columns.assert_awaited_once_with(driver, "DdlMigrations", schema="AppOwner")
    driver.execute.assert_not_awaited()


def test_sync_driver_migration_schema_hook_logs_noop(caplog: pytest.LogCaptureFixture) -> None:
    """Base sync drivers should accept migration schema configuration as a no-op hook."""
    caplog.set_level(logging.DEBUG, logger="sqlspec.driver")

    SyncDriverAdapterBase.set_migration_session_schema(object(), "app_schema")  # type: ignore[arg-type]

    records = [record for record in caplog.records if record.getMessage() == "migration.schema.noop"]
    assert records
    assert getattr(records[0], "extra_fields") == {"schema": "app_schema", "driver": "object"}


@pytest.mark.anyio
async def test_async_driver_migration_schema_hook_logs_noop(caplog: pytest.LogCaptureFixture) -> None:
    """Base async drivers should accept migration schema configuration as a no-op hook."""
    caplog.set_level(logging.DEBUG, logger="sqlspec.driver")

    await AsyncDriverAdapterBase.set_migration_session_schema(object(), "app_schema")  # type: ignore[arg-type]

    records = [record for record in caplog.records if record.getMessage() == "migration.schema.noop"]
    assert records
    assert getattr(records[0], "extra_fields") == {"schema": "app_schema", "driver": "object"}


def test_sync_update_version_record_idempotent_when_already_updated() -> None:
    """Test sync update is idempotent when version already exists."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [
        {"version_num": "0001", "version_type": "sequential"},
        {"version_num": "0002", "version_type": "sequential"},
    ]
    check_result.get_data.return_value = check_result.data

    driver.execute.side_effect = [update_result, check_result]

    tracker.update_version_record(driver, "20251011120000", "0001")

    assert driver.execute.call_count == 2


def test_sync_update_version_record_reuses_known_applied_versions() -> None:
    """A fix batch should not re-query all applied rows for every version."""
    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value.rows_affected = 0

    tracker.update_version_record(driver, "20251011120000", "0001", applied_versions={"20251011120000", "0001"})

    driver.execute.assert_called_once()


@pytest.mark.anyio
async def test_async_update_version_record_reuses_known_applied_versions() -> None:
    """Async fix batches should keep query count constant too."""
    tracker = AsyncMigrationTracker()
    driver = MagicMock()
    driver.execute = AsyncMock()
    driver.execute.return_value.rows_affected = 0

    await tracker.update_version_record(driver, "20251011120000", "0001", applied_versions={"20251011120000", "0001"})

    driver.execute.assert_awaited_once()


def test_sync_update_version_record_raises_when_neither_version_exists() -> None:
    """Test sync update raises ValueError when neither old nor new version exists."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [{"version_num": "0002", "version_type": "sequential"}]
    check_result.get_data.return_value = check_result.data

    driver.execute.side_effect = [update_result, check_result]

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        tracker.update_version_record(driver, "20251011120000", "0001")


def test_sync_update_version_record_empty_database() -> None:
    """Test sync update raises when database is empty."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = []
    check_result.get_data.return_value = check_result.data

    driver.execute.side_effect = [update_result, check_result]

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        tracker.update_version_record(driver, "20251011120000", "0001")


def test_sync_update_version_record_commits_after_success() -> None:
    """Test sync update commits transaction after successful update."""
    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.connection = None
    driver.driver_features = {}

    mock_result = Mock()
    mock_result.rows_affected = 1
    driver.execute.return_value = mock_result

    tracker.update_version_record(driver, "20251011120000", "0001")

    driver.commit.assert_called_once()


def test_sync_update_version_record_no_commit_on_idempotent_path() -> None:
    """Test sync update does not commit when taking idempotent path."""
    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.connection = Mock()
    driver.connection.autocommit = False

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [{"version_num": "0001", "version_type": "sequential"}]
    check_result.get_data.return_value = check_result.data

    driver.execute.side_effect = [update_result, check_result]

    tracker.update_version_record(driver, "20251011120000", "0001")

    driver.commit.assert_not_called()


def test_sync_schema_migration_rolls_back_on_failure() -> None:
    """Schema migration should rollback after a failed column update."""
    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {}
    driver.data_dictionary.get_columns.return_value = [{"column_name": "version"}]
    driver.rollback = Mock()

    def raise_on_add(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("boom")

    driver.execute = Mock(side_effect=raise_on_add)

    tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.rollback.assert_called_once()


@pytest.mark.anyio
async def test_async_update_version_record_success() -> None:
    """Test async update succeeds when old version exists."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    mock_result = Mock()
    mock_result.rows_affected = 1

    async def mock_execute(sql: Any) -> Mock:
        return mock_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    await tracker.update_version_record(driver, "20251011120000", "0001")

    update_call = driver.execute.call_args_list[0]
    update_sql = str(update_call[0][0])
    assert "UPDATE" in update_sql
    assert "ddl_migrations" in update_sql


@pytest.mark.anyio
async def test_async_update_version_record_idempotent_when_already_updated() -> None:
    """Test async update is idempotent when version already exists."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [
        {"version_num": "0001", "version_type": "sequential"},
        {"version_num": "0002", "version_type": "sequential"},
    ]
    check_result.get_data.return_value = check_result.data

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    await tracker.update_version_record(driver, "20251011120000", "0001")

    assert driver.execute.call_count == 2


@pytest.mark.anyio
async def test_async_schema_migration_rolls_back_on_failure() -> None:
    """Async schema migration should rollback after a failed column update."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()
    driver.driver_features = {}
    driver.data_dictionary.get_columns = AsyncMock(return_value=[{"column_name": "version"}])
    driver.rollback = AsyncMock()

    async def raise_on_add(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("boom")

    driver.execute = AsyncMock(side_effect=raise_on_add)

    await tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.rollback.assert_awaited_once()


@pytest.mark.anyio
async def test_async_update_version_record_raises_when_neither_version_exists() -> None:
    """Test async update raises ValueError when neither old nor new version exists."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [{"version_num": "0002", "version_type": "sequential"}]
    check_result.get_data.return_value = check_result.data

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        await tracker.update_version_record(driver, "20251011120000", "0001")


@pytest.mark.anyio
async def test_async_update_version_record_empty_database() -> None:
    """Test async update raises when database is empty."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = []
    check_result.get_data.return_value = check_result.data

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        await tracker.update_version_record(driver, "20251011120000", "0001")


@pytest.mark.anyio
async def test_async_update_version_record_commits_after_success() -> None:
    """Test async update commits transaction after successful update."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()
    driver.connection = None
    driver.driver_features = {}

    mock_result = Mock()
    mock_result.rows_affected = 1

    async def mock_execute(sql: Any) -> Mock:
        return mock_result

    driver.execute = AsyncMock(side_effect=mock_execute)
    driver.commit = AsyncMock()

    await tracker.update_version_record(driver, "20251011120000", "0001")

    driver.commit.assert_called_once()


@pytest.mark.anyio
async def test_async_update_version_record_no_commit_on_idempotent_path() -> None:
    """Test async update does not commit when taking idempotent path."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()
    driver.connection = None
    driver.driver_features = {}

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [{"version_num": "0001", "version_type": "sequential"}]
    check_result.get_data.return_value = check_result.data

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)
    driver.commit = AsyncMock()

    await tracker.update_version_record(driver, "20251011120000", "0001")

    driver.commit.assert_not_called()


def test_sync_update_version_preserves_sequential_type() -> None:
    """Test sync update correctly sets version_type to sequential."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    mock_result = Mock()
    mock_result.rows_affected = 1
    driver.execute.return_value = mock_result

    tracker.update_version_record(driver, "20251011120000", "0001")

    update_call = driver.execute.call_args_list[0]
    update_sql = str(update_call[0][0])
    assert "version_type" in update_sql
    assert "SET" in update_sql


def test_sync_update_version_handles_extension_versions() -> None:
    """Test sync update handles extension version format."""
    tracker = SyncMigrationTracker()
    driver = Mock()

    mock_result = Mock()
    mock_result.rows_affected = 1
    driver.execute.return_value = mock_result

    tracker.update_version_record(driver, "ext_litestar_20251011120000", "ext_litestar_0001")

    update_call = driver.execute.call_args_list[0]
    update_sql = str(update_call[0][0])
    assert "UPDATE" in update_sql


@pytest.mark.anyio
@pytest.mark.parametrize("use_logger", [False, True])
@pytest.mark.parametrize("is_async", [False, True])
async def test_tracker_schema_output_initializes_console_on_demand(
    use_logger: bool,
    is_async: bool,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema updates retain their output routing and reuse the first console."""
    from unittest.mock import patch

    from rich.console import Console

    from sqlspec.migrations import tracker as tracker_module

    tracker = AsyncMigrationTracker() if is_async else SyncMigrationTracker()
    tracker.set_output_policy(use_logger=use_logger, echo=True, summary_only=False)
    driver = Mock()
    driver.data_dictionary.get_columns = AsyncMock() if is_async else Mock()
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": column.name}
        for column in tracker._tracking_table_ddl().columns  # pyright: ignore[reportPrivateUsage]
        if column.name != "checksum"
    ]
    driver.execute = AsyncMock() if is_async else Mock()
    driver.commit = AsyncMock() if is_async else Mock()
    driver.rollback = AsyncMock() if is_async else Mock()
    monkeypatch.setattr(tracker_module, "_console", None)

    with (
        patch("rich.console.Console", wraps=Console) as console_factory,
        caplog.at_level(logging.INFO, logger="sqlspec.migrations.tracker"),
    ):
        for _ in range(2):
            if isinstance(tracker, AsyncMigrationTracker):
                await tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]
            else:
                tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]
        assert console_factory.call_count == (0 if use_logger else 1)

    assert driver.execute.call_count == 2
    assert all("checksum" in str(call.args[0]) for call in driver.execute.call_args_list)
    assert sum(getattr(record, "extra_fields", {}).get("status") == "column_added" for record in caplog.records) == 2
    output = capsys.readouterr().out
    if use_logger:
        assert output == ""
    else:
        assert output.count("Migration tracking table schema updated successfully") == 2
        assert "adding columns: checksum" in output


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("dialect", "expects_single_insert"),
    [("postgres", True), ("mysql", True), ("sqlite", True), ("duckdb", True), ("oracle", False), ("snowflake", False)],
)
async def test_tracker_sequenced_insert_vs_two_statement_path(
    is_async: bool, dialect: str, expects_single_insert: bool
) -> None:
    """Stock trackers use one INSERT ... SELECT for supported dialects and two statements outside the set."""
    recorded_sql: list[str] = []

    def _run(stmt: Any) -> Any:
        rendered = stmt.build(dialect=dialect).sql if hasattr(stmt, "build") else str(stmt)
        recorded_sql.append(rendered)
        result = Mock()
        result.data = [{"next_seq": 3}]
        result.get_data.return_value = [{"next_seq": 3}]
        return result

    async def _run_async(stmt: Any) -> Any:
        return _run(stmt)

    driver = Mock()
    driver.dialect = dialect
    driver.driver_features = {"autocommit": False}
    driver.execute = _run_async if is_async else _run
    driver.commit = AsyncMock() if is_async else Mock()

    checksum = "a" * 64
    if is_async:
        tracker = AsyncMigrationTracker()
        await tracker.record_migration(driver, "0003", "add items", 15, checksum)
        await tracker.replace_with_squash(driver, "0003", ["0001", "0002"], "squash", checksum)
    else:
        sync_tracker = SyncMigrationTracker()
        sync_tracker.record_migration(driver, "0003", "add items", 15, checksum)
        sync_tracker.replace_with_squash(driver, "0003", ["0001", "0002"], "squash", checksum)

    if expects_single_insert:
        assert len(recorded_sql) == 3
        record_sql = recorded_sql[0]
        squash_insert_sql = recorded_sql[2]
        assert "INSERT INTO" in record_sql
        assert "seq_src" in record_sql
        assert "COALESCE(MAX(" in record_sql
        assert "INSERT INTO" in squash_insert_sql
        assert "seq_src" in squash_insert_sql
    else:
        assert len(recorded_sql) == 5
        assert "SELECT" in recorded_sql[0]
        assert "COALESCE(MAX(" in recorded_sql[0]
        assert "SELECT" in recorded_sql[3]
        assert "COALESCE(MAX(" in recorded_sql[3]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_tracker_real_sqlite_sequenced_insert_increments_execution_sequence(
    tmp_path: Path, is_async: bool
) -> None:
    """Single-statement INSERT ... SELECT increments execution_sequence cleanly on SQLite."""
    db_path = tmp_path / "seq.db"
    checksum = "b" * 64
    if is_async:
        cfg_async = AiosqliteConfig(connection_config={"database": str(db_path)})
        tracker_async = AsyncMigrationTracker()
        try:
            async with cfg_async.provide_session() as driver:
                await tracker_async.ensure_tracking_table(driver)
                await tracker_async.record_migration(driver, "0001", "first", 5, checksum)
                await tracker_async.record_migration(driver, "0002", "second", 7, checksum)
                rows = await tracker_async.get_applied_migrations(driver)
            assert [row["execution_sequence"] for row in rows] == [1, 2]
        finally:
            await cfg_async.close_pool()
        return

    cfg_sync = SqliteConfig(connection_config={"database": str(db_path)})
    tracker_sync = SyncMigrationTracker()
    try:
        with cfg_sync.provide_session() as driver:
            tracker_sync.ensure_tracking_table(driver)
            tracker_sync.record_migration(driver, "0001", "first", 5, checksum)
            tracker_sync.record_migration(driver, "0002", "second", 7, checksum)
            rows = tracker_sync.get_applied_migrations(driver)
        assert [row["execution_sequence"] for row in rows] == [1, 2]
    finally:
        cfg_sync.close_pool()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("dialect", "expected_enter_fragment", "expected_exit_fragment"),
    [
        ("postgres", "pg_advisory_lock", "pg_advisory_unlock"),
        ("mysql", "GET_LOCK", "RELEASE_LOCK"),
        ("tsql", "sp_getapplock", "sp_releaseapplock"),
        ("sqlite", "BEGIN IMMEDIATE", None),
        ("duckdb", None, None),
    ],
)
async def test_tracker_acquire_lock_dialect_statements_and_release_on_error(
    is_async: bool, dialect: str, expected_enter_fragment: str | None, expected_exit_fragment: str | None
) -> None:
    """acquire_lock issues dialect-appropriate lock acquire and release statements even when the body raises."""
    executed: list[tuple[str, Any]] = []
    lifecycle: list[str] = []
    tx_state = {"active": False}

    def _exec(_self: Any, stmt: Any, params: Any = None) -> SQLResult:
        sql_text = str(stmt)
        executed.append((sql_text, params))
        if sql_text == "BEGIN IMMEDIATE":
            tx_state["active"] = True
        if "sp_getapplock" in sql_text:
            return SQLResult(data=[{"res": 0}])
        return SQLResult()

    async def _exec_async(_self: Any, stmt: Any, params: Any = None) -> SQLResult:
        return _exec(_self, stmt, params)

    def _select_value(_self: Any, stmt: Any, params: Any = None) -> int:
        executed.append((str(stmt), params))
        return 1

    async def _select_value_async(_self: Any, stmt: Any, params: Any = None) -> int:
        return _select_value(_self, stmt, params)

    def _commit(_self: Any) -> None:
        tx_state["active"] = False
        lifecycle.append("commit")

    async def _commit_async(_self: Any) -> None:
        _commit(_self)

    def _rollback(_self: Any) -> None:
        tx_state["active"] = False
        lifecycle.append("rollback")

    async def _rollback_async(_self: Any) -> None:
        _rollback(_self)

    attrs: dict[str, Any] = {
        "dialect": dialect,
        "execute": _exec_async if is_async else _exec,
        "select_value": _select_value_async if is_async else _select_value,
        "commit": _commit_async if is_async else _commit,
        "rollback": _rollback_async if is_async else _rollback,
        "_connection_in_transaction": lambda _self: tx_state["active"],
    }
    driver_cls = type(f"_Fake{dialect.title()}Driver", (), attrs)
    driver: Any = driver_cls()

    if is_async:
        tracker_async = AsyncMigrationTracker(version_table_name="ddl_migrations")
        async with tracker_async.acquire_lock(driver):
            pass
        if dialect == "sqlite":
            assert lifecycle == ["commit"]
        executed.clear()
        lifecycle.clear()
        with pytest.raises(RuntimeError, match="step failed"):
            async with tracker_async.acquire_lock(driver):
                raise RuntimeError("step failed")
    else:
        tracker_sync = SyncMigrationTracker(version_table_name="ddl_migrations")
        with tracker_sync.acquire_lock(driver):
            pass
        if dialect == "sqlite":
            assert lifecycle == ["commit"]
        executed.clear()
        lifecycle.clear()
        with pytest.raises(RuntimeError, match="step failed"):
            with tracker_sync.acquire_lock(driver):
                raise RuntimeError("step failed")

    if expected_enter_fragment is None:
        assert executed == []
    else:
        assert expected_enter_fragment in executed[0][0]
    if expected_exit_fragment is not None:
        assert expected_exit_fragment in executed[-1][0]
    elif dialect == "sqlite":
        assert lifecycle == ["rollback"]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_tracker_acquire_lock_skips_cockroachdb_and_raises_on_lock_timeout(
    monkeypatch: pytest.MonkeyPatch, is_async: bool
) -> None:
    """CockroachDB drivers emit no advisory lock statements; MySQL and SQL Server timeouts raise MigrationError."""
    cockroach_calls: list[str] = []

    if is_async:

        async def _cr_exec_async(_self: Any, stmt: Any, *_args: Any, **_kwargs: Any) -> SQLResult:
            cockroach_calls.append(str(stmt))
            return SQLResult()

        monkeypatch.setattr(CockroachAsyncpgDriver, "execute", _cr_exec_async)
        cr_driver_async = CockroachAsyncpgDriver(MagicMock())
        async with AsyncMigrationTracker().acquire_lock(cr_driver_async):
            pass
    else:

        def _cr_exec_sync(_self: Any, stmt: Any, *_args: Any, **_kwargs: Any) -> SQLResult:
            cockroach_calls.append(str(stmt))
            return SQLResult()

        monkeypatch.setattr(CockroachPsycopgSyncDriver, "execute", _cr_exec_sync)
        cr_driver_sync = CockroachPsycopgSyncDriver(MagicMock())
        with SyncMigrationTracker().acquire_lock(cr_driver_sync):
            pass
    assert cockroach_calls == []

    async def _mysql_timeout_async(_self: Any, stmt: Any, params: Any = None) -> int:
        return 0

    mysql_cls = type(
        "_TimeoutMySQLDriver",
        (),
        {
            "dialect": "mysql",
            "select_value": _mysql_timeout_async if is_async else (lambda _self, _s, _p=None: 0),
            "execute": AsyncMock() if is_async else Mock(),
        },
    )
    mysql_driver: Any = mysql_cls()
    with pytest.raises(MigrationError, match="Failed to acquire MySQL migration lock"):
        if is_async:
            async with AsyncMigrationTracker().acquire_lock(mysql_driver, timeout_seconds=1):
                pass
        else:
            with SyncMigrationTracker().acquire_lock(mysql_driver, timeout_seconds=1):
                pass

    async def _mssql_timeout_async(_self: Any, stmt: Any, params: Any = None) -> SQLResult:
        return SQLResult(data=[{"res": -1}])

    mssql_cls = type(
        "_TimeoutMssqlDriver",
        (),
        {
            "dialect": "tsql",
            "execute": _mssql_timeout_async if is_async else (lambda _self, _s, _p=None: SQLResult(data=[{"res": -1}])),
        },
    )
    mssql_driver: Any = mssql_cls()
    with pytest.raises(MigrationError, match="Failed to acquire SQL Server migration lock"):
        if is_async:
            async with AsyncMigrationTracker().acquire_lock(mssql_driver, timeout_seconds=1):
                pass
        else:
            with SyncMigrationTracker().acquire_lock(mssql_driver, timeout_seconds=1):
                pass


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_async", "error_msg", "expected_exc"),
    [
        (False, "Migration lock acquisition failed", MigrationError),
        (True, "Migration lock acquisition failed", MigrationError),
        (False, "connection terminated unexpectedly", SQLSpecError),
        (True, "connection terminated unexpectedly", SQLSpecError),
    ],
)
async def test_mssql_acquire_lock_translates_throw_error_and_reraises_unrelated(
    is_async: bool, error_msg: str, expected_exc: type[Exception]
) -> None:
    """SQL Server THROW 51000 errors become MigrationError while unrelated SQLSpecError propagates."""

    def _raise_sync(_self: Any, _stmt: Any, _params: Any = None) -> SQLResult:
        raise SQLSpecError(error_msg)

    async def _raise_async(_self: Any, _stmt: Any, _params: Any = None) -> SQLResult:
        raise SQLSpecError(error_msg)

    mssql_cls = type("_ThrowMssqlDriver", (), {"dialect": "tsql", "execute": _raise_async if is_async else _raise_sync})
    driver: Any = mssql_cls()

    with pytest.raises(expected_exc):
        if is_async:
            async with AsyncMigrationTracker().acquire_lock(driver, timeout_seconds=1):
                pass
        else:
            with SyncMigrationTracker().acquire_lock(driver, timeout_seconds=1):
                pass


@pytest.mark.anyio
@pytest.mark.parametrize(("is_async", "unlock_raises"), [(False, False), (False, True), (True, False), (True, True)])
async def test_postgres_acquire_lock_rolls_back_before_unlock_and_preserves_primary_error(
    is_async: bool, unlock_raises: bool
) -> None:
    """PostgreSQL acquire_lock rolls back an open transaction before unlocking and preserves caller exceptions."""
    events: list[str] = []
    tx_state = {"active": False}

    def _exec_sync(_self: Any, stmt: Any, _params: Any = None) -> SQLResult:
        sql_text = str(stmt)
        if "pg_advisory_lock" in sql_text:
            tx_state["active"] = True
            events.append("lock")
        elif "pg_advisory_unlock" in sql_text:
            events.append("unlock")
            if unlock_raises:
                raise RuntimeError("unlock socket failure")
        return SQLResult()

    async def _exec_async(_self: Any, stmt: Any, _params: Any = None) -> SQLResult:
        return _exec_sync(_self, stmt, _params)

    def _rollback_sync(_self: Any) -> None:
        events.append("rollback")
        tx_state["active"] = False

    async def _rollback_async(_self: Any) -> None:
        _rollback_sync(_self)

    pg_cls = type(
        "_PgUnwindDriver",
        (),
        {
            "dialect": "postgres",
            "execute": _exec_async if is_async else _exec_sync,
            "rollback": _rollback_async if is_async else _rollback_sync,
            "_connection_in_transaction": lambda _self: tx_state["active"],
        },
    )
    driver: Any = pg_cls()

    with pytest.raises(ValueError, match="migration statement error"):
        if is_async:
            async with AsyncMigrationTracker().acquire_lock(driver):
                raise ValueError("migration statement error")
        else:
            with SyncMigrationTracker().acquire_lock(driver):
                raise ValueError("migration statement error")

    assert events == ["lock", "rollback", "unlock"]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_sqlite_migration_lock_persists_across_ensure_tracking_table_and_steps(
    tmp_path: Path, is_async: bool
) -> None:
    """SQLite BEGIN IMMEDIATE stays held across ensure_tracking_table, migrations, and partial-failure savepoints."""
    db_path = tmp_path / "locked_mig.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    probe_file = tmp_path / "probe.txt"

    (migrations_dir / "0001_good.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE items_one (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )
    (migrations_dir / "0002_probe.py").write_text(
        f"""import sqlite3
from pathlib import Path

def up(context=None):
    conn = sqlite3.connect({str(db_path)!r}, timeout=0.01)
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
            Path({str(probe_file)!r}).write_text("unlocked", encoding="utf-8")
        except sqlite3.OperationalError:
            Path({str(probe_file)!r}).write_text("locked", encoding="utf-8")
    finally:
        conn.close()
    return ["CREATE TABLE items_two (id INTEGER PRIMARY KEY);"]
""",
        encoding="utf-8",
    )
    (migrations_dir / "0003_bad.sql").write_text(
        "-- name: migrate-0003-up\nINSERT INTO nonexistent_table VALUES (1);\n", encoding="utf-8"
    )

    if is_async:
        cfg_async = AiosqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_async = AsyncMigrationCommands(cfg_async)
        orig_ensure_async = cmds_async.tracker.ensure_tracking_table

        async def _check_after_ensure_async(driver: Any) -> None:
            await orig_ensure_async(driver)
            assert driver._connection_in_transaction() is True
            probe_conn = sqlite3.connect(str(db_path), timeout=0.01)
            try:
                with pytest.raises(sqlite3.OperationalError):
                    probe_conn.execute("BEGIN IMMEDIATE")
            finally:
                probe_conn.close()

        cmds_async.tracker.ensure_tracking_table = _check_after_ensure_async  # type: ignore[method-assign]
        try:
            with pytest.raises(MigrationError):
                await cmds_async.upgrade(echo=False)
            assert probe_file.read_text(encoding="utf-8") == "locked"
            async with cfg_async.provide_session() as verify_driver:
                tables = {
                    row["name"]
                    for row in await verify_driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                applied_rows = await verify_driver.select(
                    "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence"
                )
        finally:
            await cfg_async.close_pool()
    else:
        cfg_sync = SqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_sync = SyncMigrationCommands(cfg_sync)
        orig_ensure_sync = cmds_sync.tracker.ensure_tracking_table

        def _check_after_ensure_sync(driver: Any) -> None:
            orig_ensure_sync(driver)
            assert driver._connection_in_transaction() is True
            probe_conn = sqlite3.connect(str(db_path), timeout=0.01)
            try:
                with pytest.raises(sqlite3.OperationalError):
                    probe_conn.execute("BEGIN IMMEDIATE")
            finally:
                probe_conn.close()

        cmds_sync.tracker.ensure_tracking_table = _check_after_ensure_sync  # type: ignore[method-assign]
        try:
            with pytest.raises(MigrationError):
                cmds_sync.upgrade(echo=False)
            assert probe_file.read_text(encoding="utf-8") == "locked"
            with cfg_sync.provide_session() as verify_driver:
                tables = {
                    row["name"] for row in verify_driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                applied_rows = verify_driver.select(
                    "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence"
                )
        finally:
            cfg_sync.close_pool()

    assert {"items_one", "items_two"}.issubset(tables)
    assert [row["version_num"] for row in applied_rows] == ["0001", "0002"]

    post_conn = sqlite3.connect(str(db_path), timeout=0.5)
    try:
        post_conn.execute("BEGIN IMMEDIATE")
        post_conn.rollback()
    finally:
        post_conn.close()


class _SyntheticMssqlLockError(Exception):
    number: int

    def __init__(self, message: str, number: int = 51000) -> None:
        super().__init__(message)
        self.number = number


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_async", "stmt_config", "map_exception"),
    [
        (False, StatementConfig(dialect="tsql"), mssql_python_map_exception),
        (False, mssql_python_statement_config, mssql_python_map_exception),
        (False, pymssql_statement_config, pymssql_map_exception),
        (True, StatementConfig(dialect="tsql"), mssql_python_map_exception),
        (True, mssql_python_statement_config, mssql_python_map_exception),
        (True, pymssql_statement_config, pymssql_map_exception),
    ],
    ids=["sync-tsql", "sync-mssql-python", "sync-pymssql", "async-tsql", "async-mssql-python", "async-pymssql"],
)
async def test_mssql_acquire_lock_compiles_with_tsql_configs_and_translates_mapped_51000_error(
    is_async: bool, stmt_config: StatementConfig, map_exception: Any
) -> None:
    """SQL Server lock acquisition compiles cleanly under T-SQL configs and maps error 51000 to MigrationError."""
    tracker = SyncMigrationTracker()
    compiled_sql, compiled_params = SQL(
        tracker._MSSQL_ACQUIRE_LOCK_SQL,  # pyright: ignore[reportPrivateUsage]
        {"lock_name": "sqlspec_mig_ddl_migrations", "timeout_ms": 5000},
        statement_config=stmt_config,
    ).compile()

    assert "sp_getapplock" in compiled_sql
    assert "@sqlspec_res" in compiled_sql
    if isinstance(compiled_params, dict):
        assert compiled_params == {"lock_name": "sqlspec_mig_ddl_migrations", "timeout_ms": 5000}
    else:
        assert list(compiled_params) == ["sqlspec_mig_ddl_migrations", 5000]

    mapped_exc = map_exception(_SyntheticMssqlLockError("Migration lock acquisition failed", number=51000))
    assert isinstance(mapped_exc, SQLSpecError)
    assert "Migration lock acquisition failed" in str(mapped_exc)

    def _raise_sync(_self: Any, _stmt: Any, _params: Any = None) -> SQLResult:
        raise mapped_exc

    async def _raise_async(_self: Any, _stmt: Any, _params: Any = None) -> SQLResult:
        raise mapped_exc

    mssql_cls = type(
        "_MappedThrowMssqlDriver", (), {"dialect": "tsql", "execute": _raise_async if is_async else _raise_sync}
    )
    driver: Any = mssql_cls()

    with pytest.raises(MigrationError, match="Failed to acquire SQL Server migration lock"):
        if is_async:
            async with AsyncMigrationTracker().acquire_lock(driver, timeout_seconds=2):
                pass
        else:
            with SyncMigrationTracker().acquire_lock(driver, timeout_seconds=2):
                pass


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_async", "simulate_schema_error"),
    [(False, False), (False, True), (True, False), (True, True)],
    ids=["sync-schema-upgrade", "sync-schema-rollback", "async-schema-upgrade", "async-schema-rollback"],
)
async def test_sqlite_locked_schema_migration_and_rollback_keep_outer_write_lock(
    tmp_path: Path, is_async: bool, simulate_schema_error: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQLite tracker schema savepoints add missing columns or roll back errors without losing BEGIN IMMEDIATE."""
    db_path = tmp_path / f"locked_schema_{is_async}_{simulate_schema_error}.db"
    seed_conn = sqlite3.connect(str(db_path))
    try:
        seed_conn.execute(
            """
            CREATE TABLE ddl_migrations (
                version_num TEXT PRIMARY KEY,
                version_type TEXT,
                execution_sequence INTEGER,
                description TEXT,
                applied_at TIMESTAMP,
                execution_time_ms INTEGER,
                checksum TEXT,
                applied_by TEXT
            )
            """
        )
        seed_conn.commit()
    finally:
        seed_conn.close()

    migrations_dir = tmp_path / f"migrations_schema_{is_async}_{simulate_schema_error}"
    migrations_dir.mkdir()
    (migrations_dir / "0001_first.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE locked_items (id INTEGER PRIMARY KEY);\n", encoding="utf-8"
    )

    def _raise_schema_target() -> Any:
        msg = "simulated tracking schema error"
        raise RuntimeError(msg)

    lock_checks: list[bool] = []

    def _assert_write_lock_held() -> None:
        probe_conn = sqlite3.connect(str(db_path), timeout=0.01)
        try:
            with pytest.raises(sqlite3.OperationalError):
                probe_conn.execute("BEGIN IMMEDIATE")
            lock_checks.append(True)
        finally:
            probe_conn.close()

    if is_async:
        cfg_async = AiosqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_async = AsyncMigrationCommands(cfg_async)
        orig_migrate_async = cmds_async.tracker._migrate_schema_if_needed  # pyright: ignore[reportPrivateUsage]

        async def _wrapped_migrate_async(driver: Any) -> None:
            await orig_migrate_async(driver)
            assert driver._connection_in_transaction() is True
            _assert_write_lock_held()

        monkeypatch.setattr(cmds_async.tracker, "_migrate_schema_if_needed", _wrapped_migrate_async)
        if simulate_schema_error:
            monkeypatch.setattr(cmds_async.tracker, "_tracking_schema_target", _raise_schema_target)
        try:
            await cmds_async.upgrade(echo=False)
        finally:
            await cfg_async.close_pool()
    else:
        cfg_sync = SqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_sync = SyncMigrationCommands(cfg_sync)
        orig_migrate_sync = cmds_sync.tracker._migrate_schema_if_needed  # pyright: ignore[reportPrivateUsage]

        def _wrapped_migrate_sync(driver: Any) -> None:
            orig_migrate_sync(driver)
            assert driver._connection_in_transaction() is True
            _assert_write_lock_held()

        monkeypatch.setattr(cmds_sync.tracker, "_migrate_schema_if_needed", _wrapped_migrate_sync)
        if simulate_schema_error:
            monkeypatch.setattr(cmds_sync.tracker, "_tracking_schema_target", _raise_schema_target)
        try:
            cmds_sync.upgrade(echo=False)
        finally:
            cfg_sync.close_pool()

    assert lock_checks == [True]
    inspect_conn = sqlite3.connect(str(db_path))
    try:
        cols = {row[1] for row in inspect_conn.execute("PRAGMA table_info(ddl_migrations)").fetchall()}
        applied = [
            row[0]
            for row in inspect_conn.execute("SELECT version_num FROM ddl_migrations ORDER BY version_num").fetchall()
        ]
    finally:
        inspect_conn.close()
    assert ("replaces" in cols) is (not simulate_schema_error)
    assert applied == ["0001"]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_sqlite_locked_downgrade_persists_completed_steps_on_partial_failure(
    tmp_path: Path, is_async: bool
) -> None:
    """SQLite downgrade commits completed step reverts via savepoint recovery when a later step fails."""
    db_path = tmp_path / f"locked_downgrade_{is_async}.db"
    migrations_dir = tmp_path / f"migrations_downgrade_{is_async}"
    migrations_dir.mkdir()
    (migrations_dir / "0001_one.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE step_one (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0001-down\nDROP TABLE step_one;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_two.sql").write_text(
        "-- name: migrate-0002-up\nCREATE TABLE step_two (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0002-down\nDROP TABLE nonexistent_step_two_target;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0003_three.sql").write_text(
        "-- name: migrate-0003-up\nCREATE TABLE step_three (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0003-down\nDROP TABLE step_three;\n",
        encoding="utf-8",
    )

    if is_async:
        cfg_async = AiosqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_async = AsyncMigrationCommands(cfg_async)
        try:
            await cmds_async.upgrade(echo=False)
            with pytest.raises(MigrationError):
                await cmds_async.downgrade("0001", echo=False)
            async with cfg_async.provide_session() as verify_driver:
                tables = {
                    row["name"]
                    for row in await verify_driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                applied_rows = await verify_driver.select(
                    "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence"
                )
        finally:
            await cfg_async.close_pool()
    else:
        cfg_sync = SqliteConfig(
            connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
        )
        cmds_sync = SyncMigrationCommands(cfg_sync)
        try:
            cmds_sync.upgrade(echo=False)
            with pytest.raises(MigrationError):
                cmds_sync.downgrade("0001", echo=False)
            with cfg_sync.provide_session() as verify_driver:
                tables = {
                    row["name"] for row in verify_driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                applied_rows = verify_driver.select(
                    "SELECT version_num FROM ddl_migrations ORDER BY execution_sequence"
                )
        finally:
            cfg_sync.close_pool()

    assert "step_two" in tables
    assert "step_three" not in tables
    assert [row["version_num"] for row in applied_rows] == ["0001", "0002"]

    post_conn = sqlite3.connect(str(db_path), timeout=0.5)
    try:
        post_conn.execute("BEGIN IMMEDIATE")
        post_conn.rollback()
    finally:
        post_conn.close()


def test_tracker_lock_backend_kind_skips_instance_slots_and_preserves_unique_long_keys() -> None:
    """Bridge drivers with slotted instance dialects stay excluded and long version table lock keys do not collide."""

    class _InstanceDialectDriver:
        __slots__ = ("dialect",)

        def __init__(self, dialect: str) -> None:
            self.dialect = dialect

    assert SyncMigrationTracker._lock_backend_kind(_InstanceDialectDriver("sqlite")) == ""  # pyright: ignore[reportPrivateUsage]
    assert SyncMigrationTracker._lock_backend_kind(MagicMock()) == ""  # pyright: ignore[reportPrivateUsage]

    prefix = "tenant_organization_billing_schema.custom_ddl_migrations_"
    tracker_a = SyncMigrationTracker(version_table_name=f"{prefix}v1")
    tracker_b = SyncMigrationTracker(version_table_name=f"{prefix}v2")
    name_a, id_a = tracker_a._migration_lock_key()  # pyright: ignore[reportPrivateUsage]
    name_b, id_b = tracker_b._migration_lock_key()  # pyright: ignore[reportPrivateUsage]
    assert len(name_a) <= 64
    assert len(name_b) <= 64
    assert name_a != name_b
    assert id_a != id_b


@pytest.mark.anyio
@pytest.mark.parametrize(("is_async", "locked"), [(False, True), (False, False), (True, True), (True, False)])
async def test_tracker_migrate_schema_cleans_up_savepoint_and_tx_on_cancellation(
    is_async: bool, locked: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BaseException interrupts roll back and release schema savepoints before propagating."""
    events: list[tuple[str, str | None]] = []

    class _Driver:
        def create_savepoint(self, name: str) -> None:
            events.append(("create_sp", name))

        def rollback_to_savepoint(self, name: str) -> None:
            events.append(("rollback_sp", name))

        def release_savepoint(self, name: str) -> None:
            events.append(("release_sp", name))

        def rollback(self) -> None:
            events.append(("rollback", None))

    class _AsyncDriver:
        async def create_savepoint(self, name: str) -> None:
            events.append(("create_sp", name))

        async def rollback_to_savepoint(self, name: str) -> None:
            events.append(("rollback_sp", name))

        async def release_savepoint(self, name: str) -> None:
            events.append(("release_sp", name))

        async def rollback(self) -> None:
            events.append(("rollback", None))

    def _interrupt_sync(*_args: Any, **_kwargs: Any) -> Any:
        raise KeyboardInterrupt

    async def _interrupt_async(*_args: Any, **_kwargs: Any) -> Any:
        raise asyncio.CancelledError

    if is_async:
        tracker_async = AsyncMigrationTracker()
        driver_async = _AsyncDriver()
        if locked:
            tracker_async._sqlite_lock_owners.add(id(driver_async))  # pyright: ignore[reportPrivateUsage]
        monkeypatch.setattr("sqlspec.migrations.tracker.ensure_schema_async", _interrupt_async)
        with pytest.raises(asyncio.CancelledError):
            await tracker_async._migrate_schema_if_needed(driver_async)  # pyright: ignore[reportPrivateUsage,reportArgumentType]
    else:
        tracker_sync = SyncMigrationTracker()
        driver_sync = _Driver()
        if locked:
            tracker_sync._sqlite_lock_owners.add(id(driver_sync))  # pyright: ignore[reportPrivateUsage]
        monkeypatch.setattr("sqlspec.migrations.tracker.ensure_schema_sync", _interrupt_sync)
        with pytest.raises(KeyboardInterrupt):
            tracker_sync._migrate_schema_if_needed(driver_sync)  # pyright: ignore[reportPrivateUsage,reportArgumentType]

    if locked:
        assert events == [
            ("create_sp", "sqlspec_tracker_schema"),
            ("rollback_sp", "sqlspec_tracker_schema"),
            ("release_sp", "sqlspec_tracker_schema"),
        ]
    else:
        assert events == [("rollback", None)]


@pytest.mark.anyio
@pytest.mark.parametrize("dialect", ["postgres", "mysql", "mssql", "sqlite"])
async def test_async_acquire_lock_uses_transaction_open_and_unlocks_on_cancellation(dialect: str) -> None:
    """Async lock context manager uses _transaction_open when available and releases locks on CancelledError."""
    events: list[str] = []
    open_tx = {"active": False}

    class _AsyncLockDriver:
        dialect = ""

        async def _transaction_open(self) -> bool:
            events.append("tx_open")
            return open_tx["active"]

        def _connection_in_transaction(self) -> bool:
            raise AssertionError("sync _connection_in_transaction should not be called when _transaction_open exists")

        async def execute(self, stmt: Any, _params: Any = None) -> SQLResult:
            sql_text = str(stmt)
            events.append(f"exec:{sql_text}")
            if "pg_advisory_lock" in sql_text or sql_text == "BEGIN IMMEDIATE":
                open_tx["active"] = True
            if "sp_getapplock" in sql_text:
                return SQLResult(data=[{"res": 0}])
            return SQLResult()

        async def select_value(self, stmt: Any, _params: Any = None) -> int:
            events.append(f"select:{stmt}")
            return 1

        async def rollback(self) -> None:
            events.append("rollback")
            open_tx["active"] = False

        async def commit(self) -> None:
            events.append("commit")
            open_tx["active"] = False

    driver_cls = type(f"_Async{dialect.title()}LockDriver", (_AsyncLockDriver,), {"dialect": dialect})
    driver: Any = driver_cls()
    tracker = AsyncMigrationTracker(version_table_name="ddl_migrations")

    with pytest.raises(asyncio.CancelledError):
        async with tracker.acquire_lock(driver):
            raise asyncio.CancelledError

    assert tracker.holds_transaction_lock(driver) is False
    if dialect == "postgres":
        assert "tx_open" in events
        assert "rollback" in events
        assert any("pg_advisory_unlock" in ev for ev in events)
        assert events.index("rollback") < next(i for i, ev in enumerate(events) if "pg_advisory_unlock" in ev)
    elif dialect == "mysql":
        assert any("RELEASE_LOCK" in ev for ev in events)
    elif dialect == "mssql":
        assert any("sp_releaseapplock" in ev for ev in events)
    else:
        assert "tx_open" in events
        assert "rollback" in events


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_lock_context_chains_commit_error_over_step_error(tmp_path: Path, is_async: bool) -> None:
    """Savepoint recovery commit failures chain the original step error as __cause__."""
    step_error = RuntimeError("step failure")
    commit_error = OSError("commit failure")
    tx_state = {"active": False}

    if is_async:
        cfg_async = AiosqliteConfig(
            connection_config={"database": str(tmp_path / "chain_async.db")},
            migration_config={"script_location": str(tmp_path / "mig_async")},
        )
        cmds_async = AsyncMigrationCommands(cfg_async)

        class _AsyncCommitFailDriver:
            dialect = "sqlite"

            async def _transaction_open(self) -> bool:
                return tx_state["active"]

            def _connection_in_transaction(self) -> bool:
                raise AssertionError("sync _connection_in_transaction should not be called")

            async def execute(self, stmt: Any, _params: Any = None) -> SQLResult:
                if str(stmt) == "BEGIN IMMEDIATE":
                    tx_state["active"] = True
                return SQLResult()

            async def commit(self) -> None:
                raise commit_error

            async def rollback(self) -> None:
                tx_state["active"] = False

        driver_async: Any = _AsyncCommitFailDriver()
        try:
            with pytest.raises(OSError, match="commit failure") as exc_info:
                async with cmds_async._lock_context(driver_async):  # pyright: ignore[reportPrivateUsage]
                    cmds_async.runner._savepoint_recovered_drivers.add(id(driver_async))  # pyright: ignore[reportPrivateUsage]
                    raise step_error
            assert exc_info.value.__cause__ is step_error
        finally:
            await cfg_async.close_pool()
        return

    cfg_sync = SqliteConfig(
        connection_config={"database": str(tmp_path / "chain_sync.db")},
        migration_config={"script_location": str(tmp_path / "mig_sync")},
    )
    cmds_sync = SyncMigrationCommands(cfg_sync)

    class _SyncCommitFailDriver:
        dialect = "sqlite"

        def _connection_in_transaction(self) -> bool:
            return tx_state["active"]

        def execute(self, stmt: Any, _params: Any = None) -> SQLResult:
            if str(stmt) == "BEGIN IMMEDIATE":
                tx_state["active"] = True
            return SQLResult()

        def commit(self) -> None:
            raise commit_error

        def rollback(self) -> None:
            tx_state["active"] = False

    driver_sync: Any = _SyncCommitFailDriver()
    try:
        with pytest.raises(OSError, match="commit failure") as exc_info:
            with cmds_sync._lock_context(driver_sync):  # pyright: ignore[reportPrivateUsage]
                cmds_sync.runner._savepoint_recovered_drivers.add(id(driver_sync))  # pyright: ignore[reportPrivateUsage]
                raise step_error
        assert exc_info.value.__cause__ is step_error
    finally:
        cfg_sync.close_pool()
