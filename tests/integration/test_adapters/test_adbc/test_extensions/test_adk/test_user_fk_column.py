"""Tests for ADBC ADK store user FK column support."""

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore


@pytest.fixture()
def adbc_store_with_fk(tmp_path):
    """Create ADBC ADK store with user FK column (SQLite)."""
    db_path = tmp_path / "test_fk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})

    store = AdbcADKStore(config, user_fk_column="tenant_id INTEGER")

    with config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("CREATE TABLE tenants (id INTEGER PRIMARY KEY, name TEXT)")
            cursor.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A')")
            cursor.execute("INSERT INTO tenants (id, name) VALUES (2, 'Tenant B')")
            conn.commit()
        finally:
            cursor.close()

    store.create_tables()
    return store


@pytest.fixture()
def adbc_store_no_fk(tmp_path):
    """Create ADBC ADK store without user FK column (SQLite)."""
    db_path = tmp_path / "test_no_fk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    return store


def test_create_session_with_user_fk(adbc_store_with_fk):
    """Test creating session with user FK value."""
    session_id = "test-session-1"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}
    tenant_id = 1

    session = adbc_store_with_fk.create_session(session_id, app_name, user_id, state, user_fk=tenant_id)

    assert session["id"] == session_id
    assert session["state"] == state


def test_create_session_without_user_fk_value(adbc_store_with_fk):
    """Test creating session without providing user FK value still works."""
    session_id = "test-session-2"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}

    session = adbc_store_with_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id


def test_create_session_no_fk_column_configured(adbc_store_no_fk):
    """Test creating session when no FK column configured."""
    session_id = "test-session-3"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}

    session = adbc_store_no_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id
    assert session["state"] == state


def test_user_fk_column_name_parsed_correctly():
    """Test user FK column name is parsed correctly."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config, user_fk_column="organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE")

    assert store._user_fk_column_name == "organization_id"
    assert "UUID REFERENCES" in store._user_fk_column_ddl


def test_user_fk_column_complex_ddl():
    """Test complex user FK column DDL is preserved."""
    config = AdbcConfig(connection_config={"driver_name": "postgresql", "uri": ":memory:"})
    complex_ddl = "workspace_id UUID NOT NULL DEFAULT gen_random_uuid() REFERENCES workspaces(id)"
    store = AdbcADKStore(config, user_fk_column=complex_ddl)

    assert store._user_fk_column_name == "workspace_id"
    assert store._user_fk_column_ddl == complex_ddl


def test_multiple_tenants_isolation(adbc_store_with_fk):
    """Test sessions are properly isolated by tenant."""
    app_name = "test-app"
    user_id = "user-123"

    adbc_store_with_fk.create_session("session-tenant1", app_name, user_id, {"data": "tenant1"}, user_fk=1)
    adbc_store_with_fk.create_session("session-tenant2", app_name, user_id, {"data": "tenant2"}, user_fk=2)

    retrieved1 = adbc_store_with_fk.get_session("session-tenant1")
    retrieved2 = adbc_store_with_fk.get_session("session-tenant2")

    assert retrieved1["state"]["data"] == "tenant1"
    assert retrieved2["state"]["data"] == "tenant2"


def test_user_fk_properties():
    """Test user FK column properties are accessible."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config, user_fk_column="tenant_id INTEGER")

    assert store.user_fk_column_name == "tenant_id"
    assert store.user_fk_column_ddl == "tenant_id INTEGER"


def test_no_user_fk_properties_when_none():
    """Test user FK properties are None when not configured."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)

    assert store.user_fk_column_name is None
    assert store.user_fk_column_ddl is None
