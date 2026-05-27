"""Unit tests for migration tracker behavior."""

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from sqlspec.adapters.oracledb.migrations import OracleSyncMigrationTracker
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._sync import SyncDriverAdapterBase
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
        str(tracker._get_create_table_sql()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_current_version_sql()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_applied_migrations_sql()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_next_execution_sequence_sql()),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_record_migration_sql("0001", "sequential", 1, "init", 10, "abc", "tester")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_remove_migration_sql("0001")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_update_version_sql("20250101000000", "0001", "sequential")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_delete_versions_sql(["0001"])),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_record_squashed_migration_sql("0002", "sequential", 2, "squash", 0, "def", "tester", "0001")),  # pyright: ignore[reportPrivateUsage]
        str(tracker._get_check_column_exists_sql()),  # pyright: ignore[reportPrivateUsage]
    ]

    assert all('"history"."ddl_migrations"' in statement for statement in rendered_statements)


def test_sync_tracker_keeps_bare_table_sql_without_schema() -> None:
    """Existing migration tracker SQL should stay unqualified by default."""
    tracker = SyncMigrationTracker(version_table_name="ddl_migrations")

    assert tracker.version_table == "ddl_migrations"
    assert "history.ddl_migrations" not in str(tracker._get_create_table_sql())  # pyright: ignore[reportPrivateUsage]


def test_sync_tracker_introspects_bare_table_name_with_schema() -> None:
    """Qualified tracker SQL should not leak into data-dictionary table-name arguments."""
    tracker = SyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="history")
    driver = Mock()
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": column.name}
        for column in tracker._get_create_table_sql().columns  # pyright: ignore[reportPrivateUsage]
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
            for column in tracker._get_create_table_sql().columns  # pyright: ignore[reportPrivateUsage]
        ]
    )
    driver.execute = AsyncMock()

    await tracker._migrate_schema_if_needed(driver)  # pyright: ignore[reportPrivateUsage]

    driver.data_dictionary.get_columns.assert_awaited_once_with(driver, "ddl_migrations", schema="history")
    driver.execute.assert_not_awaited()


def test_oracle_tracker_uppercases_qualified_tracking_table() -> None:
    """Oracle tracker should qualify migration table names using Oracle's uppercase convention."""
    tracker = OracleSyncMigrationTracker(version_table_name="ddl_migrations", version_table_schema="app_owner")

    assert tracker.version_table == "APP_OWNER.DDL_MIGRATIONS"
    assert tracker.version_table_schema == "app_owner"
    assert tracker.version_table_name == "ddl_migrations"


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

    tracker._add_column = Mock(side_effect=raise_on_add)  # type: ignore[method-assign]

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

    tracker._add_column = AsyncMock(side_effect=raise_on_add)  # type: ignore[method-assign]

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
