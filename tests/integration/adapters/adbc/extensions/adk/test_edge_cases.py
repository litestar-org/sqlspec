"""Tests for ADBC ADK store edge cases and error handling."""

import json
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture()
async def adbc_store(tmp_path: Path) -> AdbcADKStore:
    """Create ADBC ADK store with SQLite backend."""
    db_path = tmp_path / "test_adk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    await store.create_tables()
    return store


async def test_create_tables_idempotent(adbc_store: Any) -> None:
    """Test that create_tables can be called multiple times safely."""
    await adbc_store.create_tables()
    await adbc_store.create_tables()


def test_table_names_validation(tmp_path: Path) -> None:
    """Test that invalid table names are rejected."""
    db_path = tmp_path / "test_validation.db"

    with pytest.raises(ValueError, match="Table name cannot be empty"):
        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
            extension_config={"adk": {"session_table": "", "events_table": "events"}},
        )
        AdbcADKStore(config)

    with pytest.raises(ValueError, match="Invalid table name"):
        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
            extension_config={"adk": {"session_table": "invalid-name", "events_table": "events"}},
        )
        AdbcADKStore(config)

    with pytest.raises(ValueError, match="Invalid table name"):
        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
            extension_config={"adk": {"session_table": "1_starts_with_number", "events_table": "events"}},
        )
        AdbcADKStore(config)

    with pytest.raises(ValueError, match="Table name too long"):
        long_name = "a" * 100
        config = AdbcConfig(
            connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
            extension_config={"adk": {"session_table": long_name, "events_table": "events"}},
        )
        AdbcADKStore(config)


async def test_operations_before_create_tables(tmp_path: Path) -> None:
    """Test operations gracefully handle missing tables."""
    db_path = tmp_path / "test_no_tables.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)

    session = await store.get_session("nonexistent")
    assert session is None

    sessions = await store.list_sessions("app", "user")
    assert sessions == []

    events = await store.get_events("session")
    assert events == []


async def test_custom_table_names(tmp_path: Path) -> None:
    """Test using custom table names."""
    db_path = tmp_path / "test_custom.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"session_table": "custom_sessions", "events_table": "custom_events"}},
    )
    store = AdbcADKStore(config)
    await store.create_tables()

    session_id = "test"
    session = await store.create_session(session_id, "app", "user", {"data": "test"})
    assert session["id"] == session_id

    retrieved = await store.get_session(session_id)
    assert retrieved is not None


async def test_unicode_in_fields(adbc_store: Any) -> None:
    """Test Unicode characters in various fields."""
    session_id = "unicode-session"
    app_name = "\u6d4b\u8bd5\u5e94\u7528"
    user_id = "\u30e6\u30fc\u30b6\u30fc123"
    state = {"message": "Hello \u4e16\u754c"}

    created_session = await adbc_store.create_session(session_id, app_name, user_id, state)
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"]["message"] == "Hello \u4e16\u754c"

    from datetime import datetime, timezone

    from sqlspec.extensions.adk import EventRecord

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "\u30a2\u30b7\u30b9\u30bf\u30f3\u30c8",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {
            "id": "unicode-event",
            "content": {"text": "\u3053\u3093\u306b\u3061\u306f"},
            "app_name": app_name,
            "user_id": user_id,
        },
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["author"] == "\u30a2\u30b7\u30b9\u30bf\u30f3\u30c8"
    event_data = (
        json.loads(events[0]["event_json"]) if isinstance(events[0]["event_json"], str) else events[0]["event_json"]
    )
    assert event_data["content"]["text"] == "\u3053\u3093\u306b\u3061\u306f"


async def test_special_characters_in_json(adbc_store: Any) -> None:
    """Test special characters in JSON fields."""
    session_id = "special-chars"
    state = {
        "quotes": 'He said "Hello"',
        "backslash": "C:\\Users\\test",
        "newline": "Line1\nLine2",
        "tab": "Col1\tCol2",
    }

    await adbc_store.create_session(session_id, "app", "user", state)
    retrieved = await adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"] == state


async def test_very_long_strings(adbc_store: Any) -> None:
    """Test handling very long strings in VARCHAR fields."""
    long_id = "x" * 127
    long_app = "a" * 127
    long_user = "u" * 127

    session = await adbc_store.create_session(long_id, long_app, long_user, {})
    assert session["id"] == long_id
    assert session["app_name"] == long_app
    assert session["user_id"] == long_user


async def test_session_state_with_deeply_nested_data(adbc_store: Any) -> None:
    """Test deeply nested JSON structures."""
    session_id = "deep-nest"
    deeply_nested = {"level1": {"level2": {"level3": {"level4": {"level5": {"value": "deep"}}}}}}

    await adbc_store.create_session(session_id, "app", "user", deeply_nested)
    retrieved = await adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"]["level1"]["level2"]["level3"]["level4"]["level5"]["value"] == "deep"


async def test_concurrent_session_updates(adbc_store: Any) -> None:
    """Test multiple updates to the same session."""
    session_id = "concurrent-test"
    await adbc_store.create_session(session_id, "app", "user", {"version": 1})

    for i in range(10):
        await adbc_store.update_session_state(session_id, {"version": i + 2})

    final_session = await adbc_store.get_session(session_id)
    assert final_session is not None
    assert final_session["state"]["version"] == 11


async def test_event_with_none_values(adbc_store: Any) -> None:
    """Test creating event with explicit None values for optional fields."""
    session_id = "none-test"
    await adbc_store.create_session(session_id, "app", "user", {})

    from datetime import datetime, timezone

    from sqlspec.extensions.adk import EventRecord

    event_record: EventRecord = {
        "session_id": session_id,
        "invocation_id": "",
        "author": "",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "none-event", "app_name": "app", "user_id": "user"},
    }
    await adbc_store.append_event(event_record)

    events = await adbc_store.get_events(session_id)
    assert len(events) == 1
    assert events[0]["session_id"] == session_id
    assert "event_json" in events[0]


async def test_list_sessions_with_same_user_different_apps(adbc_store: Any) -> None:
    """Test listing sessions doesn't mix data across apps."""
    user_id = "user-123"
    app1 = "app1"
    app2 = "app2"

    await adbc_store.create_session("s1", app1, user_id, {})
    await adbc_store.create_session("s2", app1, user_id, {})
    await adbc_store.create_session("s3", app2, user_id, {})

    app1_sessions = await adbc_store.list_sessions(app1, user_id)
    app2_sessions = await adbc_store.list_sessions(app2, user_id)

    assert len(app1_sessions) == 2
    assert len(app2_sessions) == 1


async def test_delete_nonexistent_session(adbc_store: Any) -> None:
    """Test deleting a session that doesn't exist."""
    await adbc_store.delete_session("nonexistent-session")


async def test_update_nonexistent_session(adbc_store: Any) -> None:
    """Test updating a session that doesn't exist."""
    await adbc_store.update_session_state("nonexistent-session", {"data": "test"})


async def test_drop_and_recreate_tables(adbc_store: Any) -> None:
    """Test dropping and recreating tables."""
    session_id = "test-session"
    await adbc_store.create_session(session_id, "app", "user", {"data": "test"})

    drop_sqls = adbc_store._get_drop_tables_sql()
    with adbc_store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            for sql in drop_sqls:
                cursor.execute(sql)
            conn.commit()
        finally:
            cursor.close()

    await adbc_store.create_tables()

    session = await adbc_store.get_session(session_id)
    assert session is None


async def test_json_with_escaped_characters(adbc_store: Any) -> None:
    """Test JSON serialization of escaped characters."""
    session_id = "escaped-json"
    state = {"escaped": r"test\nvalue\t", "quotes": r'"quoted"'}

    await adbc_store.create_session(session_id, "app", "user", state)
    retrieved = await adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"] == state
