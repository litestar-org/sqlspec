"""Test owner_id_column support for BigQuery ADK store."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from sqlspec.adapters.bigquery.config import BigQueryConfig

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
async def bigquery_adk_store_with_fk(bigquery_service: Any) -> "AsyncGenerator[Any, None]":
    """Create BigQuery ADK store with owner_id_column configured."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),  # type: ignore[no-untyped-call]
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        extension_config={"adk": {"owner_id_column": "tenant_id INT64 NOT NULL"}},
    )
    store = BigQueryADKStore(config)
    await store.create_tables()
    yield store


async def test_owner_id_column_in_ddl(bigquery_adk_store_with_fk: Any) -> None:
    """Test that owner_id_column appears in CREATE TABLE DDL."""
    ddl = bigquery_adk_store_with_fk._get_create_sessions_table_sql()
    assert "tenant_id INT64 NOT NULL" in ddl


async def test_create_session_with_owner_id(bigquery_adk_store_with_fk: Any) -> None:
    """Test creating a session with owner_id value."""
    session_id = "session-with-fk"
    app_name = "app1"
    user_id = "user1"
    state = {"test": True}
    owner_id = "12345"

    session = await bigquery_adk_store_with_fk.create_session(session_id, app_name, user_id, state, owner_id=owner_id)

    assert session["id"] == session_id
    assert session["app_name"] == app_name
    assert session["user_id"] == user_id
    assert session["state"] == state


async def test_create_session_without_owner_id_when_configured(bigquery_adk_store_with_fk: Any) -> None:
    """Test creating a session without owner_id value when column is configured."""
    session_id = "session-no-fk"
    app_name = "app1"
    user_id = "user1"
    state = {"test": True}

    session = await bigquery_adk_store_with_fk.create_session(session_id, app_name, user_id, state)

    assert session["id"] == session_id


async def test_owner_id_column_name_parsed(bigquery_service: Any) -> None:
    """Test that owner_id_column_name is correctly parsed from DDL."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),  # type: ignore[no-untyped-call]
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        extension_config={"adk": {"owner_id_column": "account_id STRING"}},
    )

    store = BigQueryADKStore(config)

    assert store._owner_id_column_name == "account_id"  # pyright: ignore[reportPrivateUsage]
    assert store._owner_id_column_ddl == "account_id STRING"  # pyright: ignore[reportPrivateUsage]


async def test_bigquery_no_fk_enforcement(bigquery_adk_store_with_fk: Any) -> None:
    """Test that BigQuery doesn't enforce FK constraints (documentation check)."""
    ddl = bigquery_adk_store_with_fk._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]

    assert "REFERENCES" not in ddl
    assert "tenant_id INT64 NOT NULL" in ddl


async def test_owner_id_column_with_different_types(bigquery_service: Any) -> None:
    """Test owner_id_column with different BigQuery types."""
    config_int = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),  # type: ignore[no-untyped-call]
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        extension_config={"adk": {"owner_id_column": "org_id INT64 NOT NULL"}},
    )

    store_int = BigQueryADKStore(config_int)
    ddl_int = store_int._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]
    assert "org_id INT64 NOT NULL" in ddl_int

    config_string = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),  # type: ignore[no-untyped-call]
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        extension_config={"adk": {"owner_id_column": "tenant_uuid STRING"}},
    )
    store_string = BigQueryADKStore(config_string)
    ddl_string = store_string._get_create_sessions_table_sql()  # pyright: ignore[reportPrivateUsage]
    assert "tenant_uuid STRING" in ddl_string
