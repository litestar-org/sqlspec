"""Integration tests for DuckDB ADK session store.

The shared session/event CRUD lifecycle (create_tables, session round-trip, list/delete,
append/get events, get_events filtering) is covered by
tests/integration/adapters/_shared/suite_adk_store_contract.py. This module keeps the
adapter-specific coverage (owner_id_column, storage-type fidelity, timestamp precision,
concurrency, event ordering/JSON details) that is not portable across the contract matrix.
"""

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlspec.adapters.duckdb.adk import DuckdbADKStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.adk import EventRecord

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
def duckdb_adk_store(tmp_path: Path) -> "Generator[DuckdbADKStore, None, None]":
    """Create DuckDB ADK store with temporary file-based database.

    Args:
        tmp_path: Pytest fixture providing unique temporary directory per test.

    Yields:
        Configured DuckDB ADK store instance.

    Notes:
        Uses file-based database for thread-safe testing.
    """
    db_path = tmp_path / "test_adk.duckdb"
    try:
        config = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={"adk": {"session_table": "test_sessions", "events_table": "test_events"}},
        )
        store = DuckdbADKStore(config)
        store.create_tables()
        yield store
    finally:
        if db_path.exists():
            db_path.unlink()


def test_event_with_optional_fields(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test creating events with optional fields stored in event_data."""
    session_id = "session-008"
    duckdb_adk_store.create_session(session_id, "test-app", "user-008", {})

    event_record: EventRecord = {
        "id": "event-full",
        "app_name": "test-app",
        "user_id": "user-008",
        "session_id": session_id,
        "invocation_id": "inv-123",
        "timestamp": datetime.now(timezone.utc),
        "event_data": {
            "id": "event-full",
            "author": "assistant",
            "content": {"text": "Response"},
            "app_name": "test-app",
            "user_id": "user-008",
            "branch": "main",
            "grounding_metadata": {"sources": ["doc1", "doc2"]},
            "custom_metadata": {"priority": "high"},
            "partial": True,
            "turn_complete": False,
            "interrupted": False,
        },
    }
    duckdb_adk_store.append_event(event_record)

    events = duckdb_adk_store.get_events("test-app", "user-008", session_id)
    assert len(events) == 1

    assert events[0]["invocation_id"] == "inv-123"

    event_data = events[0]["event_data"]
    assert event_data["branch"] == "main"
    assert event_data["grounding_metadata"] == {"sources": ["doc1", "doc2"]}
    assert event_data["partial"] is True
    assert event_data["turn_complete"] is False


def test_event_ordering_by_timestamp(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test events are ordered by timestamp ascending."""
    session_id = "session-009"
    duckdb_adk_store.create_session(session_id, "test-app", "user-009", {})

    t1 = datetime.now(timezone.utc)
    t2 = datetime.now(timezone.utc)
    t3 = datetime.now(timezone.utc)

    ev_middle: EventRecord = {
        "id": "event-middle",
        "app_name": "test-app",
        "user_id": "user-009",
        "session_id": session_id,
        "invocation_id": "",
        "timestamp": t2,
        "event_data": {"id": "event-middle", "app_name": "test-app", "user_id": "user-009"},
    }
    ev_last: EventRecord = {
        "id": "event-last",
        "app_name": "test-app",
        "user_id": "user-009",
        "session_id": session_id,
        "invocation_id": "",
        "timestamp": t3,
        "event_data": {"id": "event-last", "app_name": "test-app", "user_id": "user-009"},
    }
    ev_first: EventRecord = {
        "id": "event-first",
        "app_name": "test-app",
        "user_id": "user-009",
        "session_id": session_id,
        "invocation_id": "",
        "timestamp": t1,
        "event_data": {"id": "event-first", "app_name": "test-app", "user_id": "user-009"},
    }

    duckdb_adk_store.append_event(ev_middle)
    duckdb_adk_store.append_event(ev_last)
    duckdb_adk_store.append_event(ev_first)

    events = duckdb_adk_store.get_events("test-app", "user-009", session_id)

    assert len(events) == 3
    # Events should be ordered by timestamp ASC
    event_ids = [event["event_data"]["id"] for event in events]
    assert event_ids == ["event-first", "event-middle", "event-last"]


def test_session_state_with_complex_data(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test session state with nested JSON structures."""
    session_id = "session-complex"
    complex_state = {
        "user": {"name": "Alice", "preferences": {"theme": "dark", "language": "en"}},
        "conversation": {
            "topics": ["weather", "news", "sports"],
            "turn_count": 5,
            "metadata": {"started_at": "2025-10-06T12:00:00Z"},
        },
        "flags": [True, False, True],
    }

    duckdb_adk_store.create_session(session_id, "test-app", "user-010", complex_state)

    session = duckdb_adk_store.get_session("test-app", "user-010", session_id)
    assert session is not None
    assert session["state"] == complex_state
    assert session["state"]["user"]["preferences"]["theme"] == "dark"
    assert session["state"]["conversation"]["turn_count"] == 5


def test_event_data_round_trip(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test storing and retrieving event data via event_data."""
    session_id = "session-json-rt"
    duckdb_adk_store.create_session(session_id, "test-app", "user-012", {})

    event_record: EventRecord = {
        "id": "event-json",
        "app_name": "test-app",
        "user_id": "user-012",
        "session_id": session_id,
        "invocation_id": "",
        "timestamp": datetime.now(timezone.utc),
        "event_data": {
            "id": "event-json",
            "author": "system",
            "content": {"data": "value"},
            "app_name": "test-app",
            "user_id": "user-012",
        },
    }
    duckdb_adk_store.append_event(event_record)

    events = duckdb_adk_store.get_events("test-app", "user-012", session_id)
    assert len(events) == 1
    assert events[0]["event_data"]["content"] == {"data": "value"}


def test_concurrent_session_updates(duckdb_adk_store: DuckdbADKStore) -> None:
    """Test multiple updates to same session."""
    session_id = "session-concurrent"
    duckdb_adk_store.create_session(session_id, "test-app", "user-013", {"counter": 0})

    for i in range(10):
        session = duckdb_adk_store.get_session("test-app", "user-013", session_id)
        assert session is not None
        current_counter = session["state"]["counter"]
        duckdb_adk_store.update_session_state("test-app", "user-013", session_id, {"counter": current_counter + 1})

    final_session = duckdb_adk_store.get_session("test-app", "user-013", session_id)
    assert final_session is not None
    assert final_session["state"]["counter"] == 10


def test_owner_id_column_with_integer(tmp_path: Path) -> None:
    """Test owner ID column with INTEGER type."""
    db_path = tmp_path / "test_owner_id_int.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE tenants (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A'), (2, 'Tenant B')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_tenant",
                    "events_table": "events_with_tenant",
                    "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        assert store.owner_id_column_name == "tenant_id"
        assert store.owner_id_column_ddl == "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"

        session = store.create_session(
            session_id="session-tenant-1", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=1
        )

        assert session["id"] == "session-tenant-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT tenant_id FROM sessions_with_tenant WHERE id = ?", ("session-tenant-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_with_ubigint(tmp_path: Path) -> None:
    """Test owner ID column with DuckDB UBIGINT type."""
    db_path = tmp_path / "test_owner_id_ubigint.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE users (id UBIGINT PRIMARY KEY, email VARCHAR)")
            conn.execute("INSERT INTO users (id, email) VALUES (18446744073709551615, 'user@example.com')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_user",
                    "events_table": "events_with_user",
                    "owner_id_column": "owner_id UBIGINT REFERENCES users(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        assert store.owner_id_column_name == "owner_id"

        session = store.create_session(
            session_id="session-user-1",
            app_name="test-app",
            user_id="user-001",
            state={"data": "test"},
            owner_id=18446744073709551615,
        )

        assert session["id"] == "session-user-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT owner_id FROM sessions_with_user WHERE id = ?", ("session-user-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 18446744073709551615
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_foreign_key_constraint(tmp_path: Path) -> None:
    """Test that FK constraint is enforced."""
    db_path = tmp_path / "test_owner_id_constraint.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE organizations (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO organizations (id, name) VALUES (100, 'Org A')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_org",
                    "events_table": "events_with_org",
                    "owner_id_column": "org_id INTEGER NOT NULL REFERENCES organizations(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        store.create_session(
            session_id="session-org-1", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=100
        )

        with pytest.raises(Exception) as exc_info:
            store.create_session(
                session_id="session-org-invalid",
                app_name="test-app",
                user_id="user-002",
                state={"data": "test"},
                owner_id=999,
            )

        assert "FOREIGN KEY constraint" in str(exc_info.value) or "Constraint Error" in str(exc_info.value)
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_without_value(tmp_path: Path) -> None:
    """Test creating session without owner_id when column is configured but nullable."""
    db_path = tmp_path / "test_owner_id_nullable.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_nullable_fk",
                    "events_table": "events_nullable_fk",
                    "owner_id_column": "account_id INTEGER REFERENCES accounts(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        session = store.create_session(
            session_id="session-no-fk", app_name="test-app", user_id="user-001", state={"data": "test"}, owner_id=None
        )

        assert session["id"] == "session-no-fk"

        retrieved = store.get_session("test-app", "user-001", "session-no-fk")
        assert retrieved is not None
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_with_varchar(tmp_path: Path) -> None:
    """Test owner ID column with VARCHAR type."""
    db_path = tmp_path / "test_owner_id_varchar.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE companies (code VARCHAR PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO companies (code, name) VALUES ('ACME', 'Acme Corp'), ('INIT', 'Initech')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_company",
                    "events_table": "events_with_company",
                    "owner_id_column": "company_code VARCHAR NOT NULL REFERENCES companies(code)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        session = store.create_session(
            session_id="session-company-1",
            app_name="test-app",
            user_id="user-001",
            state={"data": "test"},
            owner_id="ACME",
        )

        assert session["id"] == "session-company-1"

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT company_code FROM sessions_with_company WHERE id = ?", ("session-company-1",))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "ACME"
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_multiple_sessions(tmp_path: Path) -> None:
    """Test multiple sessions with same FK value."""
    db_path = tmp_path / "test_owner_id_multiple.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO departments (id, name) VALUES (10, 'Engineering'), (20, 'Sales')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_dept",
                    "events_table": "events_with_dept",
                    "owner_id_column": "dept_id INTEGER NOT NULL REFERENCES departments(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        for i in range(5):
            store.create_session(
                session_id=f"session-dept-{i}",
                app_name="test-app",
                user_id=f"user-{i}",
                state={"index": i},
                owner_id=10,
            )

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM sessions_with_dept WHERE dept_id = ?", (10,))
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 5
    finally:
        if db_path.exists():
            db_path.unlink()


def test_owner_id_column_query_by_fk(tmp_path: Path) -> None:
    """Test querying sessions by FK column value."""
    db_path = tmp_path / "test_owner_id_query.duckdb"
    try:
        config = DuckDBConfig(connection_config={"database": str(db_path)})

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY, name VARCHAR)")
            conn.execute("INSERT INTO projects (id, name) VALUES (1, 'Project Alpha'), (2, 'Project Beta')")
            conn.commit()

        config_with_extension = DuckDBConfig(
            connection_config={"database": str(db_path)},
            extension_config={
                "adk": {
                    "session_table": "sessions_with_project",
                    "events_table": "events_with_project",
                    "owner_id_column": "project_id INTEGER NOT NULL REFERENCES projects(id)",
                }
            },
        )
        store = DuckdbADKStore(config_with_extension)
        store.create_tables()

        store.create_session("s1", "app", "u1", {"val": 1}, owner_id=1)
        store.create_session("s2", "app", "u2", {"val": 2}, owner_id=1)
        store.create_session("s3", "app", "u3", {"val": 3}, owner_id=2)

        with config.provide_connection() as conn:
            cursor = conn.execute("SELECT id FROM sessions_with_project WHERE project_id = ? ORDER BY id", (1,))
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "s1"
            assert rows[1][0] == "s2"

            cursor = conn.execute("SELECT id FROM sessions_with_project WHERE project_id = ?", (2,))
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "s3"
    finally:
        if db_path.exists():
            db_path.unlink()
