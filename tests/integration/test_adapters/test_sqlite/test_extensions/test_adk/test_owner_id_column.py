"""Tests for SQLite ADK store owner_id_column functionality.

This test module verifies that the SQLite ADK store correctly handles optional
user foreign key columns for multi-tenant scenarios and referential integrity.
"""

import uuid
from datetime import datetime
from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.sqlite, pytest.mark.integration]


def _make_shared_memory_db_name() -> str:
    """Generate unique shared memory database URI for each test."""
    return f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=shared"


def _create_tenants_table(config: SqliteConfig) -> None:
    """Create a tenants reference table for FK testing."""
    with config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
        """)
        conn.commit()


def _insert_tenant(config: SqliteConfig, tenant_name: str) -> int | None:
    """Insert a tenant and return its ID."""
    with config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.execute("INSERT INTO tenants (name) VALUES (?)", (tenant_name,))
        tenant_id = cursor.lastrowid
        conn.commit()
        return tenant_id


def _create_users_table(config: SqliteConfig) -> None:
    """Create a users reference table for FK testing with TEXT primary key."""
    with config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
            )
        """)
        conn.commit()


def _insert_user(config: SqliteConfig, username: str, email: str) -> None:
    """Insert a user."""
    with config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("INSERT INTO users (username, email) VALUES (?, ?)", (username, email))
        conn.commit()


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Provide unique shared in-memory SQLite config for each test.

    Uses cache=shared mode with a unique database name per test to:
    - Allow multiple connections within the same test to share the database
    - Prevent table schema conflicts between different tests
    - Enable foreign key relationships across connections
    """
    return SqliteConfig(connection_config={"database": _make_shared_memory_db_name(), "uri": True})


@pytest.fixture
def session_id() -> str:
    """Generate unique session ID."""
    return str(uuid.uuid4())


@pytest.fixture
def app_name() -> str:
    """Provide test app name."""
    return "test_app"


@pytest.fixture
def user_id() -> str:
    """Provide test user ID."""
    return "user_123"


@pytest.fixture
def initial_state() -> "dict[str, Any]":
    """Provide initial session state."""
    return {"key": "value", "count": 0}


async def test_owner_id_column_integer_reference(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test owner ID column with INTEGER foreign key."""
    _create_tenants_table(sqlite_config)
    tenant_id = _insert_tenant(sqlite_config, "tenant_alpha")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={
            "adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"}
        },
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session = await store.create_session(session_id, app_name, user_id, initial_state, owner_id=tenant_id)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == initial_state
    assert isinstance(session["create_time"], datetime)
    assert isinstance(session["update_time"], datetime)

    retrieved = await store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["id"] == session_id
    assert retrieved["state"] == initial_state


async def test_owner_id_column_text_reference(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test owner ID column with TEXT foreign key."""
    _create_users_table(sqlite_config)
    username = "alice"
    _insert_user(sqlite_config, username, "alice@example.com")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "user_ref TEXT REFERENCES users(username) ON DELETE CASCADE"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session = await store.create_session(session_id, app_name, user_id, initial_state, owner_id=username)

    assert session["id"] == session_id
    assert session["state"] == initial_state

    retrieved = await store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["id"] == session_id


async def test_owner_id_column_cascade_delete(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test CASCADE DELETE on owner ID column."""
    _create_tenants_table(sqlite_config)
    tenant_id = _insert_tenant(sqlite_config, "tenant_beta")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={
            "adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"}
        },
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    await store.create_session(session_id, app_name, user_id, initial_state, owner_id=tenant_id)

    retrieved_before = await store.get_session(session_id)
    assert retrieved_before is not None

    with sqlite_config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM tenants WHERE id = ?", (tenant_id,))
        conn.commit()

    retrieved_after = await store.get_session(session_id)
    assert retrieved_after is None


async def test_owner_id_column_constraint_violation(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test FK constraint violation with invalid tenant_id."""
    _create_tenants_table(sqlite_config)

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    invalid_tenant_id = 99999

    with pytest.raises(Exception) as exc_info:
        await store.create_session(session_id, app_name, user_id, initial_state, owner_id=invalid_tenant_id)

    assert "FOREIGN KEY constraint failed" in str(exc_info.value) or "constraint" in str(exc_info.value).lower()


async def test_owner_id_column_not_null_constraint(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test NOT NULL constraint on owner ID column."""
    _create_tenants_table(sqlite_config)

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    with pytest.raises(Exception) as exc_info:
        await store.create_session(session_id, app_name, user_id, initial_state, owner_id=None)

    assert "NOT NULL constraint failed" in str(exc_info.value) or "not null" in str(exc_info.value).lower()


async def test_owner_id_column_nullable(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test nullable owner ID column."""
    _create_tenants_table(sqlite_config)
    tenant_id = _insert_tenant(sqlite_config, "tenant_gamma")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session_without_fk = await store.create_session(str(uuid.uuid4()), app_name, user_id, initial_state, owner_id=None)
    assert session_without_fk is not None

    session_with_fk = await store.create_session(session_id, app_name, user_id, initial_state, owner_id=tenant_id)
    assert session_with_fk is not None


async def test_without_owner_id_column(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test store without owner ID column configured."""
    store = SqliteADKStore(sqlite_config)
    await store.create_tables()

    session = await store.create_session(session_id, app_name, user_id, initial_state)

    assert session["id"] == session_id
    assert session["state"] == initial_state

    retrieved = await store.get_session(session_id)
    assert retrieved is not None
    assert retrieved["id"] == session_id


async def test_foreign_keys_pragma_enabled(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test that PRAGMA foreign_keys = ON is properly enabled."""
    _create_tenants_table(sqlite_config)
    tenant_id = _insert_tenant(sqlite_config, "tenant_delta")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    await store.create_session(session_id, app_name, user_id, initial_state, owner_id=tenant_id)

    with sqlite_config.provide_connection() as conn:
        cursor = conn.execute("PRAGMA foreign_keys")
        fk_enabled = cursor.fetchone()[0]
        assert fk_enabled == 1


async def test_multi_tenant_isolation(
    sqlite_config: SqliteConfig, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test multi-tenant isolation with different tenant IDs."""
    _create_tenants_table(sqlite_config)
    tenant1_id = _insert_tenant(sqlite_config, "tenant_one")
    tenant2_id = _insert_tenant(sqlite_config, "tenant_two")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={
            "adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"}
        },
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session1_id = str(uuid.uuid4())
    session2_id = str(uuid.uuid4())

    await store.create_session(session1_id, app_name, user_id, initial_state, owner_id=tenant1_id)
    await store.create_session(session2_id, app_name, user_id, {"data": "tenant2"}, owner_id=tenant2_id)

    session1 = await store.get_session(session1_id)
    session2 = await store.get_session(session2_id)

    assert session1 is not None
    assert session2 is not None
    assert session1["state"] == initial_state
    assert session2["state"] == {"data": "tenant2"}

    with sqlite_config.provide_connection() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM tenants WHERE id = ?", (tenant1_id,))
        conn.commit()

    session1_after = await store.get_session(session1_id)
    session2_after = await store.get_session(session2_id)

    assert session1_after is None
    assert session2_after is not None


async def test_owner_id_column_ddl_extraction(sqlite_config: SqliteConfig) -> None:
    """Test that column name is correctly extracted from DDL."""
    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={
            "adk": {"owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"}
        },
    )
    store = SqliteADKStore(config_with_extension)

    assert store._owner_id_column_name == "tenant_id"  # pyright: ignore[reportPrivateUsage]
    assert store._owner_id_column_ddl == "tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"  # pyright: ignore[reportPrivateUsage]


async def test_create_session_without_fk_when_not_required(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test creating session without owner_id when column is nullable."""
    _create_tenants_table(sqlite_config)

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"}},
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session = await store.create_session(session_id, app_name, user_id, initial_state)

    assert session["id"] == session_id
    assert session["state"] == initial_state


async def test_owner_id_with_default_value(
    sqlite_config: SqliteConfig, session_id: str, app_name: str, user_id: str, initial_state: "dict[str, Any]"
) -> None:
    """Test owner ID column with DEFAULT value."""
    _create_tenants_table(sqlite_config)
    default_tenant_id = _insert_tenant(sqlite_config, "default_tenant")

    config_with_extension = SqliteConfig(
        connection_config=sqlite_config.connection_config,
        extension_config={
            "adk": {"owner_id_column": f"tenant_id INTEGER DEFAULT {default_tenant_id} REFERENCES tenants(id)"}
        },
    )
    store = SqliteADKStore(config_with_extension)
    await store.create_tables()

    session = await store.create_session(session_id, app_name, user_id, initial_state)

    assert session["id"] == session_id
    retrieved = await store.get_session(session_id)
    assert retrieved is not None
