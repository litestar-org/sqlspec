"""Test user_fk_column support for BigQuery ADK store."""

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from sqlspec.adapters.bigquery.config import BigQueryConfig


@pytest.fixture
async def bigquery_adk_store_with_fk(bigquery_service):
    """Create BigQuery ADK store with user_fk_column configured."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),
        }
    )
    store = BigQueryADKStore(config, dataset_id=bigquery_service.dataset, user_fk_column="tenant_id INT64 NOT NULL")
    await store.create_tables()
    yield store


@pytest.mark.asyncio
async def test_user_fk_column_in_ddl(bigquery_adk_store_with_fk):
    """Test that user_fk_column appears in CREATE TABLE DDL."""
    ddl = bigquery_adk_store_with_fk._get_create_sessions_table_sql()
    assert "tenant_id INT64 NOT NULL" in ddl


@pytest.mark.asyncio
async def test_create_session_with_user_fk(bigquery_adk_store_with_fk):
    """Test creating a session with user_fk value."""
    session_id = "session-with-fk"
    app_name = "app1"
    user_id = "user1"
    state = {"test": True}
    user_fk = "12345"

    session = await bigquery_adk_store_with_fk.create_session(session_id, app_name, user_id, state, user_fk=user_fk)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


@pytest.mark.asyncio
async def test_create_session_without_user_fk_when_configured(bigquery_adk_store_with_fk):
    """Test creating a session without user_fk value when column is configured."""
    session_id = "session-no-fk"
    app_name = "app1"
    user_id = "user1"
    state = {"test": True}

    session = await bigquery_adk_store_with_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id


@pytest.mark.asyncio
async def test_user_fk_column_name_parsed(bigquery_service):
    """Test that user_fk_column_name is correctly parsed from DDL."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),
        }
    )

    store = BigQueryADKStore(config, dataset_id=bigquery_service.dataset, user_fk_column="account_id STRING")

    assert store._user_fk_column_name == "account_id"
    assert store._user_fk_column_ddl == "account_id STRING"


@pytest.mark.asyncio
async def test_bigquery_no_fk_enforcement(bigquery_adk_store_with_fk):
    """Test that BigQuery doesn't enforce FK constraints (documentation check)."""
    ddl = bigquery_adk_store_with_fk._get_create_sessions_table_sql()

    assert "REFERENCES" not in ddl
    assert "tenant_id INT64 NOT NULL" in ddl


@pytest.mark.asyncio
async def test_user_fk_column_with_different_types(bigquery_service):
    """Test user_fk_column with different BigQuery types."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),
        }
    )

    store_int = BigQueryADKStore(config, dataset_id=bigquery_service.dataset, user_fk_column="org_id INT64 NOT NULL")
    ddl_int = store_int._get_create_sessions_table_sql()
    assert "org_id INT64 NOT NULL" in ddl_int

    store_string = BigQueryADKStore(config, dataset_id=bigquery_service.dataset, user_fk_column="tenant_uuid STRING")
    ddl_string = store_string._get_create_sessions_table_sql()
    assert "tenant_uuid STRING" in ddl_string
