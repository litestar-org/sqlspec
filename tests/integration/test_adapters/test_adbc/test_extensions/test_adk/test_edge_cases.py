"""Tests for ADBC ADK store edge cases and error handling."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture()
def adbc_store(tmp_path: Path) -> AdbcADKStore:
    """Create ADBC ADK store with SQLite backend."""
    db_path = tmp_path / "test_adk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    return store


def test_create_tables_idempotent(adbc_store: Any) -> None:
    """Test that create_tables can be called multiple times safely."""
    adbc_store.create_tables()
    adbc_store.create_tables()


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


def test_operations_before_create_tables(tmp_path: Path) -> None:
    """Test operations gracefully handle missing tables."""
    db_path = tmp_path / "test_no_tables.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)

    session = store.get_session("nonexistent")
    assert session is None

    sessions = store.list_sessions("app", "user")
    assert sessions == []

    events = store.list_events("session")
    assert events == []


def test_custom_table_names(tmp_path: Path) -> None:
    """Test using custom table names."""
    db_path = tmp_path / "test_custom.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"session_table": "custom_sessions", "events_table": "custom_events"}},
    )
    store = AdbcADKStore(config)
    store.create_tables()

    session_id = "test"
    session = store.create_session(session_id, "app", "user", {"data": "test"})
    assert session["id"] == session_id

    retrieved = store.get_session(session_id)
    assert retrieved is not None


def test_unicode_in_fields(adbc_store: Any) -> None:
    """Test Unicode characters in various fields."""
    session_id = "unicode-session"
    app_name = "测试应用"
    user_id = "ユーザー123"
    state = {"message": "Hello 世界", "emoji": "🎉"}

    created_session = adbc_store.create_session(session_id, app_name, user_id, state)
    assert created_session["app_name"] == app_name
    assert created_session["user_id"] == user_id
    assert created_session["state"]["message"] == "Hello 世界"
    assert created_session["state"]["emoji"] == "🎉"

    event = adbc_store.create_event(
        event_id="unicode-event",
        session_id=session_id,
        app_name=app_name,
        user_id=user_id,
        author="アシスタント",
        content={"text": "こんにちは 🌍"},
    )

    assert event["author"] == "アシスタント"
    assert event["content"]["text"] == "こんにちは 🌍"


def test_special_characters_in_json(adbc_store: Any) -> None:
    """Test special characters in JSON fields."""
    session_id = "special-chars"
    state = {
        "quotes": 'He said "Hello"',
        "backslash": "C:\\Users\\test",
        "newline": "Line1\nLine2",
        "tab": "Col1\tCol2",
    }

    adbc_store.create_session(session_id, "app", "user", state)
    retrieved = adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"] == state


def test_very_long_strings(adbc_store: Any) -> None:
    """Test handling very long strings in VARCHAR fields."""
    long_id = "x" * 127
    long_app = "a" * 127
    long_user = "u" * 127

    session = adbc_store.create_session(long_id, long_app, long_user, {})
    assert session["id"] == long_id
    assert session["app_name"] == long_app
    assert session["user_id"] == long_user


def test_session_state_with_deeply_nested_data(adbc_store: Any) -> None:
    """Test deeply nested JSON structures."""
    session_id = "deep-nest"
    deeply_nested = {"level1": {"level2": {"level3": {"level4": {"level5": {"value": "deep"}}}}}}

    adbc_store.create_session(session_id, "app", "user", deeply_nested)
    retrieved = adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"]["level1"]["level2"]["level3"]["level4"]["level5"]["value"] == "deep"


def test_concurrent_session_updates(adbc_store: Any) -> None:
    """Test multiple updates to the same session."""
    session_id = "concurrent-test"
    adbc_store.create_session(session_id, "app", "user", {"version": 1})

    for i in range(10):
        adbc_store.update_session_state(session_id, {"version": i + 2})

    final_session = adbc_store.get_session(session_id)
    assert final_session is not None
    assert final_session["state"]["version"] == 11


def test_event_with_none_values(adbc_store: Any) -> None:
    """Test creating event with explicit None values."""
    session_id = "none-test"
    adbc_store.create_session(session_id, "app", "user", {})

    event = adbc_store.create_event(
        event_id="none-event",
        session_id=session_id,
        app_name="app",
        user_id="user",
        invocation_id=None,
        author=None,
        actions=None,
        content=None,
        grounding_metadata=None,
        custom_metadata=None,
        partial=None,
        turn_complete=None,
        interrupted=None,
        error_code=None,
        error_message=None,
    )

    assert event["invocation_id"] is None
    assert event["author"] is None
    assert event["actions"] == b""
    assert event["content"] is None
    assert event["grounding_metadata"] is None
    assert event["custom_metadata"] is None
    assert event["partial"] is None
    assert event["turn_complete"] is None
    assert event["interrupted"] is None


def test_list_sessions_with_same_user_different_apps(adbc_store: Any) -> None:
    """Test listing sessions doesn't mix data across apps."""
    user_id = "user-123"
    app1 = "app1"
    app2 = "app2"

    adbc_store.create_session("s1", app1, user_id, {})
    adbc_store.create_session("s2", app1, user_id, {})
    adbc_store.create_session("s3", app2, user_id, {})

    app1_sessions = adbc_store.list_sessions(app1, user_id)
    app2_sessions = adbc_store.list_sessions(app2, user_id)

    assert len(app1_sessions) == 2
    assert len(app2_sessions) == 1


def test_delete_nonexistent_session(adbc_store: Any) -> None:
    """Test deleting a session that doesn't exist."""
    adbc_store.delete_session("nonexistent-session")


def test_update_nonexistent_session(adbc_store: Any) -> None:
    """Test updating a session that doesn't exist."""
    adbc_store.update_session_state("nonexistent-session", {"data": "test"})


def test_drop_and_recreate_tables(adbc_store: Any) -> None:
    """Test dropping and recreating tables."""
    session_id = "test-session"
    adbc_store.create_session(session_id, "app", "user", {"data": "test"})

    drop_sqls = adbc_store._get_drop_tables_sql()
    with adbc_store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            for sql in drop_sqls:
                cursor.execute(sql)
            conn.commit()
        finally:
            cursor.close()

    adbc_store.create_tables()

    session = adbc_store.get_session(session_id)
    assert session is None


def test_json_with_escaped_characters(adbc_store: Any) -> None:
    """Test JSON serialization of escaped characters."""
    session_id = "escaped-json"
    state = {"escaped": r"test\nvalue\t", "quotes": r'"quoted"'}

    adbc_store.create_session(session_id, "app", "user", state)
    retrieved = adbc_store.get_session(session_id)

    assert retrieved is not None
    assert retrieved["state"] == state
