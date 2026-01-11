"""Integration tests for ADBC ADK store with actual database dialects.

These tests require the actual ADBC drivers to be installed:
- adbc-driver-sqlite (default, always available)
- adbc-driver-postgresql (optional)
- adbc-driver-duckdb (optional)
- adbc-driver-snowflake (optional)

Tests are marked with dialect-specific markers and will be skipped
if the driver is not installed.
"""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = pytest.mark.adbc


@pytest.fixture()
def sqlite_store(tmp_path: Path) -> Any:
    """SQLite ADBC store fixture."""
    db_path = tmp_path / "sqlite_test.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    return store


def test_sqlite_dialect_creates_text_columns(sqlite_store: Any) -> None:
    """Test SQLite dialect creates TEXT columns for JSON."""
    with sqlite_store.config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"PRAGMA table_info({sqlite_store.session_table})")
            columns = cursor.fetchall()

            state_column = next(col for col in columns if col[1] == "state")
            assert state_column[2] == "TEXT"
        finally:
            cursor.close()  # type: ignore[no-untyped-call]


def test_sqlite_dialect_session_operations(sqlite_store: Any) -> None:
    """Test SQLite dialect with full session CRUD."""
    session_id = "sqlite-session-1"
    app_name = "test-app"
    user_id = "user-123"
    state = {"nested": {"key": "value"}, "count": 42}

    created = sqlite_store.create_session(session_id, app_name, user_id, state)
    assert created["id"] == session_id
    assert created["state"] == state

    retrieved = sqlite_store.get_session(session_id)
    assert retrieved["state"] == state

    new_state = {"updated": True}
    sqlite_store.update_session_state(session_id, new_state)

    updated = sqlite_store.get_session(session_id)
    assert updated["state"] == new_state


def test_sqlite_dialect_event_operations(sqlite_store: Any) -> None:
    """Test SQLite dialect with event operations."""
    session_id = "sqlite-session-events"
    app_name = "test-app"
    user_id = "user-123"

    sqlite_store.create_session(session_id, app_name, user_id, {})

    event_id = "event-1"
    actions = b"pickled_actions_data"
    content = {"message": "Hello"}

    event = sqlite_store.create_event(
        event_id=event_id, session_id=session_id, app_name=app_name, user_id=user_id, actions=actions, content=content
    )

    assert event["id"] == event_id
    assert event["content"] == content

    events = sqlite_store.list_events(session_id)
    assert len(events) == 1
    assert events[0]["content"] == content


@pytest.mark.postgres
@pytest.mark.skipif(True, reason="Requires adbc-driver-postgresql and PostgreSQL server")
def test_postgresql_dialect_creates_jsonb_columns() -> None:
    """Test PostgreSQL dialect creates JSONB columns.

    This test is skipped by default. To run:
    1. Install adbc-driver-postgresql
    2. Start PostgreSQL server
    3. Update connection config
    4. Remove skipif marker
    """
    config = AdbcConfig(
        connection_config={"driver_name": "postgresql", "uri": "postgresql://user:pass@localhost/testdb"}
    )
    store = AdbcADKStore(config)
    store.create_tables()

    with store.config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = '{store.session_table}'
                AND column_name = 'state'
                """
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "jsonb"
        finally:
            cursor.close()  # type: ignore[no-untyped-call]  # type: ignore[no-untyped-call]


@pytest.mark.duckdb
@pytest.mark.skipif(True, reason="Requires adbc-driver-duckdb")
def test_duckdb_dialect_creates_json_columns(tmp_path: Path) -> None:
    """Test DuckDB dialect creates JSON columns.

    This test is skipped by default. To run:
    1. Install adbc-driver-duckdb
    2. Remove skipif marker
    """
    db_path = tmp_path / "duckdb_test.db"
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()

    session_id = "duckdb-session-1"
    state = {"analytics": {"count": 1000, "revenue": 50000.00}}

    created = store.create_session(session_id, "app", "user", state)
    assert created["state"] == state


@pytest.mark.snowflake
@pytest.mark.skipif(True, reason="Requires adbc-driver-snowflake and Snowflake account")
def test_snowflake_dialect_creates_variant_columns() -> None:
    """Test Snowflake dialect creates VARIANT columns.

    This test is skipped by default. To run:
    1. Install adbc-driver-snowflake
    2. Configure Snowflake credentials
    3. Remove skipif marker
    """
    config = AdbcConfig(
        connection_config={
            "driver_name": "snowflake",
            "uri": "snowflake://account.region/database?warehouse=wh",
            "username": "user",
            "password": "pass",
        }
    )
    store = AdbcADKStore(config)
    store.create_tables()

    with store.config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = UPPER('{store.session_table}')
                AND column_name = 'STATE'
                """
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "VARIANT"
        finally:
            cursor.close()  # type: ignore[no-untyped-call]


def test_sqlite_with_owner_id_column(tmp_path: Path) -> None:
    """Test SQLite with owner ID column creates proper constraints."""
    db_path = tmp_path / "sqlite_fk_test.db"
    base_config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})

    with base_config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("CREATE TABLE tenants (id INTEGER PRIMARY KEY, name TEXT)")
            cursor.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A')")
            conn.commit()
        finally:
            cursor.close()  # type: ignore[no-untyped-call]

    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"}},
    )
    store = AdbcADKStore(config)
    store.create_tables()

    session = store.create_session("s1", "app", "user", {"data": "test"}, owner_id=1)
    assert session["id"] == "s1"

    retrieved = store.get_session("s1")
    assert retrieved is not None


def test_generic_dialect_fallback(tmp_path: Path) -> None:
    """Test generic dialect is used for unknown drivers."""
    db_path = tmp_path / "generic_test.db"

    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})

    store = AdbcADKStore(config)
    assert store.dialect in ["sqlite", "generic"]

    store.create_tables()

    session = store.create_session("generic-1", "app", "user", {"test": True})
    assert session["state"]["test"] is True
