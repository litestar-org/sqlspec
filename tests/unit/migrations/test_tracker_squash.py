"""Unit tests for Migration Tracker squash functionality.

Tests for:
- Schema migration to add `replaces` column
- replace_with_squash() method
- is_squash_already_applied() method
"""

from typing import Any
from unittest.mock import Mock

import pytest


def _mock_result(**kwargs: Any) -> Mock:
    """Create a Mock result with get_data() returning .data."""
    m = Mock(**kwargs)
    m.get_data.return_value = m.data
    return m


def test_tracker_schema_squash_create_table_sql_includes_replaces_column() -> None:
    """Test that _tracking_table_ddl includes replaces column."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    create_sql = tracker._tracking_table_ddl()
    column_names = [col.name.lower() for col in create_sql.columns]
    assert "replaces" in column_names


def test_tracker_schema_squash_replaces_column_is_nullable() -> None:
    """Test that replaces column is nullable (not_null=False)."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    create_sql = tracker._tracking_table_ddl()
    replaces_col = next((col for col in create_sql.columns if col.name.lower() == "replaces"), None)
    assert replaces_col is not None
    assert not replaces_col.not_null


def test_sync_tracker_squash_methods_replace_with_squash_deletes_replaced_versions() -> None:
    """Test replace_with_squash deletes all replaced version records."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    driver.execute.side_effect = mock_execute
    tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    delete_calls = [c for c in execute_calls if "DELETE" in c.upper()]
    assert len(delete_calls) >= 1


def test_sync_tracker_squash_methods_replace_with_squash_inserts_squashed_record() -> None:
    """Test replace_with_squash inserts squashed version with replaces metadata."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    driver.execute.side_effect = mock_execute
    tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    insert_calls = [c for c in execute_calls if "INSERT" in c.upper()]
    assert len(insert_calls) >= 1


def test_sync_tracker_squash_methods_is_squash_already_applied_returns_true_when_replaced_exists() -> None:
    """Test is_squash_already_applied returns True when replaced version found."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}])
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is True


def test_sync_tracker_squash_methods_is_squash_already_applied_returns_false_for_fresh_db() -> None:
    """Test is_squash_already_applied returns False when no replaced versions found."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[])
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is False


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_replace_with_squash_deletes_replaced_versions() -> None:
    """Test async replace_with_squash deletes all replaced version records."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    async def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    async def mock_commit() -> None:
        pass

    driver.execute = mock_execute
    driver.commit = mock_commit
    await tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    delete_calls = [c for c in execute_calls if "DELETE" in c.upper()]
    assert len(delete_calls) >= 1


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_is_squash_already_applied_returns_true_when_exists() -> None:
    """Test async is_squash_already_applied returns True when replaced version found."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def mock_execute(sql: Any) -> Mock:
        return _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}])

    driver.execute = mock_execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is True


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_is_squash_already_applied_returns_false_for_fresh_db() -> None:
    """Test async is_squash_already_applied returns False when no replaced versions found."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def mock_execute(sql: Any) -> Mock:
        return _mock_result(data=[])

    driver.execute = mock_execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is False


def test_check_versions_exist_sql_uses_where_in_clause() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    query = str(tracker._check_versions_query(["0001", "0002"])).upper()
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


def test_sync_is_squash_already_applied_executes_filtered_where_in_query() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[{"version_num": "0001"}])
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
    )
    query = str(driver.execute.call_args.args[0]).upper()
    assert result is True
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


def test_sync_is_squash_already_applied_returns_false_for_empty_filtered_result() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[])
    assert (
        tracker.is_squash_already_applied(driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"])
        is False
    )


@pytest.mark.anyio
async def test_async_is_squash_already_applied_executes_filtered_where_in_query() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()
    captured: list[Any] = []

    async def execute(query: Any) -> Mock:
        captured.append(query)
        return _mock_result(data=[{"version_num": "0001"}])

    driver.execute = execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
    )
    query = str(captured[0]).upper()
    assert result is True
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


@pytest.mark.anyio
async def test_async_is_squash_already_applied_returns_true_for_filtered_result() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def execute(query: Any) -> Mock:
        _ = query
        return _mock_result(data=[{"version_num": "0001"}])

    driver.execute = execute
    assert (
        await tracker.is_squash_already_applied(
            driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
        )
        is True
    )


@pytest.mark.anyio
async def test_async_is_squash_already_applied_returns_false_for_empty_filtered_result() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def execute(query: Any) -> Mock:
        _ = query
        return _mock_result(data=[])

    driver.execute = execute
    assert (
        await tracker.is_squash_already_applied(
            driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
        )
        is False
    )
