"""Unit tests for BaseEventQueueStore and validation utilities."""

import pytest

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import normalize_event_channel_name, normalize_queue_table_name


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_sqlite_event_schema_reconciliation(asynchronous: bool) -> None:
    """First reconciliation creates the queue; subsequent reconciliation is idempotent."""
    if asynchronous:
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.events import AiosqliteEventQueueStore

        async_config = AiosqliteConfig()
        async_store = AiosqliteEventQueueStore(async_config)
        try:
            async with async_config.provide_session() as driver:
                await async_store.prepare_schema_async(driver)
                first = await async_store.reconcile_schema_async(driver)
                second = await async_store.reconcile_schema_async(driver)
                count = await driver.select_value("SELECT COUNT(*) FROM sqlspec_event_queue")
        finally:
            await async_config.close_pool()
    else:
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.adapters.sqlite.events import SqliteEventQueueStore

        config = SqliteConfig()
        store = SqliteEventQueueStore(config)
        try:
            with config.provide_session() as sync_driver:
                store.prepare_schema_sync(sync_driver)
                first = store.reconcile_schema_sync(sync_driver)
                second = store.reconcile_schema_sync(sync_driver)
                count = sync_driver.select_value("SELECT COUNT(*) FROM sqlspec_event_queue")
        finally:
            config.close_pool()
    assert first.created_tables == ["sqlspec_event_queue"]
    assert second.created_tables == []
    assert second.added_columns == {}
    assert count == 0


def test_normalize_queue_table_name_simple() -> None:
    """Simple table names pass validation."""
    result = normalize_queue_table_name("events_queue")
    assert result == "events_queue"


def test_normalize_queue_table_name_underscore_prefix() -> None:
    """Underscore-prefixed names are valid identifiers."""
    result = normalize_queue_table_name("_internal_events")
    assert result == "_internal_events"


def test_normalize_queue_table_name_schema_qualified() -> None:
    """Schema-qualified names are validated segment by segment."""
    result = normalize_queue_table_name("app_schema.events_queue")
    assert result == "app_schema.events_queue"


def test_normalize_queue_table_name_three_segments() -> None:
    """Three-part names (catalog.schema.table) are supported."""
    result = normalize_queue_table_name("catalog.schema.events")
    assert result == "catalog.schema.events"


def test_normalize_queue_table_name_invalid_characters() -> None:
    """Names with invalid characters raise EventChannelError."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events-queue")


def test_normalize_queue_table_name_starts_with_number() -> None:
    """Names starting with numbers are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("123_events")


def test_normalize_queue_table_name_empty_segment() -> None:
    """Empty segments in schema-qualified names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("schema..events")


def test_normalize_queue_table_name_special_chars() -> None:
    """Special characters like @ are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events@queue")


def test_normalize_queue_table_name_spaces() -> None:
    """Spaces in names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        normalize_queue_table_name("events queue")


def test_normalize_event_channel_name_simple() -> None:
    """Simple channel names pass validation."""
    result = normalize_event_channel_name("notifications")
    assert result == "notifications"


def test_normalize_event_channel_name_underscore() -> None:
    """Underscore-separated names are valid."""
    result = normalize_event_channel_name("user_events")
    assert result == "user_events"


def test_normalize_event_channel_name_underscore_prefix() -> None:
    """Underscore-prefixed channel names are valid."""
    result = normalize_event_channel_name("_internal")
    assert result == "_internal"


def test_normalize_event_channel_name_alphanumeric() -> None:
    """Alphanumeric channel names are valid."""
    result = normalize_event_channel_name("events2024")
    assert result == "events2024"


def test_normalize_event_channel_name_invalid_hyphen() -> None:
    """Hyphens in channel names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("user-events")


def test_normalize_event_channel_name_dot_not_allowed() -> None:
    """Dots are not allowed in channel names (unlike table names)."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("app.notifications")


def test_normalize_event_channel_name_starts_with_number() -> None:
    """Channel names starting with numbers are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("123events")


def test_normalize_event_channel_name_empty() -> None:
    """Empty channel names are rejected."""
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        normalize_event_channel_name("")
