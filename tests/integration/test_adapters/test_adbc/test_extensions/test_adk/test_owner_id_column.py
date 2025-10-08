"""Tests for ADBC ADK store owner ID column support."""

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.adk import AdbcADKStore

pytestmark = [pytest.mark.xdist_group("sqlite"), pytest.mark.adbc, pytest.mark.integration]


@pytest.fixture()
def adbc_store_with_fk(tmp_path):  # type: ignore[no-untyped-def]
    """Create ADBC ADK store with owner ID column (SQLite)."""
    db_path = tmp_path / "test_fk.db"
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"},
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER"}},
    )

    store = AdbcADKStore(config)

    with config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("CREATE TABLE tenants (id INTEGER PRIMARY KEY, name TEXT)")
            cursor.execute("INSERT INTO tenants (id, name) VALUES (1, 'Tenant A')")
            cursor.execute("INSERT INTO tenants (id, name) VALUES (2, 'Tenant B')")
            conn.commit()
        finally:
            cursor.close()  # type: ignore[no-untyped-call]

    store.create_tables()
    return store


@pytest.fixture()
def adbc_store_no_fk(tmp_path):  # type: ignore[no-untyped-def]
    """Create ADBC ADK store without owner ID column (SQLite)."""
    db_path = tmp_path / "test_no_fk.db"
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": f"file:{db_path}"})
    store = AdbcADKStore(config)
    store.create_tables()
    return store


def test_create_session_with_owner_id(adbc_store_with_fk):  # type: ignore[no-untyped-def]
    """Test creating session with owner ID value."""
    session_id = "test-session-1"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}
    tenant_id = 1

    session = adbc_store_with_fk.create_session(session_id, app_name, user_id, state, owner_id=tenant_id)

    assert session["id"] == session_id
    assert session["state"] == state


def test_create_session_without_owner_id_value(adbc_store_with_fk):  # type: ignore[no-untyped-def]
    """Test creating session without providing owner ID value still works."""
    session_id = "test-session-2"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}

    session = adbc_store_with_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id


def test_create_session_no_fk_column_configured(adbc_store_no_fk):  # type: ignore[no-untyped-def]
    """Test creating session when no FK column configured."""
    session_id = "test-session-3"
    app_name = "test-app"
    user_id = "user-123"
    state = {"key": "value"}

    session = adbc_store_no_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id
    assert session["state"] == state


def test_owner_id_column_name_parsed_correctly() -> None:
    """Test owner ID column name is parsed correctly."""
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": ":memory:"},
        extension_config={
            "adk": {"owner_id_column": "organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE"}
        },
    )
    store = AdbcADKStore(config)

    assert store.owner_id_column_name == "organization_id"
    assert store.owner_id_column_ddl and "UUID REFERENCES" in store.owner_id_column_ddl


def test_owner_id_column_complex_ddl() -> None:
    """Test complex owner ID column DDL is preserved."""
    complex_ddl = "workspace_id UUID NOT NULL DEFAULT gen_random_uuid() REFERENCES workspaces(id)"
    config = AdbcConfig(
        connection_config={"driver_name": "postgresql", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": complex_ddl}},
    )
    store = AdbcADKStore(config)

    assert store.owner_id_column_name == "workspace_id"
    assert store._owner_id_column_ddl == complex_ddl  # pyright: ignore[reportPrivateUsage]


def test_multiple_tenants_isolation(adbc_store_with_fk):  # type: ignore[no-untyped-def]
    """Test sessions are properly isolated by tenant."""
    app_name = "test-app"
    user_id = "user-123"

    adbc_store_with_fk.create_session("session-tenant1", app_name, user_id, {"data": "tenant1"}, owner_id=1)
    adbc_store_with_fk.create_session("session-tenant2", app_name, user_id, {"data": "tenant2"}, owner_id=2)

    retrieved1 = adbc_store_with_fk.get_session("session-tenant1")
    retrieved2 = adbc_store_with_fk.get_session("session-tenant2")

    assert retrieved1["state"]["data"] == "tenant1"
    assert retrieved2["state"]["data"] == "tenant2"


def test_owner_id_properties() -> None:
    """Test owner ID column properties are accessible."""
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite", "uri": ":memory:"},
        extension_config={"adk": {"owner_id_column": "tenant_id INTEGER"}},
    )
    store = AdbcADKStore(config)

    assert store.owner_id_column_name == "tenant_id"
    assert store.owner_id_column_ddl == "tenant_id INTEGER"


def test_no_owner_id_properties_when_none() -> None:
    """Test owner ID properties are None when not configured."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})
    store = AdbcADKStore(config)

    assert store.owner_id_column_name is None
    assert store.owner_id_column_ddl is None
