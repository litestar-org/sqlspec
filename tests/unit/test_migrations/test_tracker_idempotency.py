"""Unit tests for idempotent update_version_record behavior."""

from typing import Any
from unittest.mock import MagicMock, Mock

import pytest

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

    driver.execute.side_effect = [update_result, check_result]

    tracker.update_version_record(driver, "20251011120000", "0001")

    driver.commit.assert_not_called()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    await tracker.update_version_record(driver, "20251011120000", "0001")

    assert driver.execute.call_count == 2


@pytest.mark.asyncio
async def test_async_update_version_record_raises_when_neither_version_exists() -> None:
    """Test async update raises ValueError when neither old nor new version exists."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = [{"version_num": "0002", "version_type": "sequential"}]

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        await tracker.update_version_record(driver, "20251011120000", "0001")


@pytest.mark.asyncio
async def test_async_update_version_record_empty_database() -> None:
    """Test async update raises when database is empty."""
    from unittest.mock import AsyncMock

    tracker = AsyncMigrationTracker()
    driver = MagicMock()

    update_result = Mock()
    update_result.rows_affected = 0

    check_result = Mock()
    check_result.data = []

    call_count = [0]

    async def mock_execute(sql: Any) -> Mock:
        call_count[0] += 1
        if call_count[0] == 1:
            return update_result
        return check_result

    driver.execute = AsyncMock(side_effect=mock_execute)

    with pytest.raises(ValueError, match="Migration version 20251011120000 not found in database"):
        await tracker.update_version_record(driver, "20251011120000", "0001")


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
