"""BigQuery ADK test fixtures."""

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from sqlspec.adapters.bigquery.config import BigQueryConfig


@pytest.fixture
async def bigquery_adk_store(bigquery_service):
    """Create BigQuery ADK store with emulator backend."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),
        }
    )
    store = BigQueryADKStore(config, dataset_id=bigquery_service.dataset)
    await store.create_tables()
    yield store


@pytest.fixture
async def session_fixture(bigquery_adk_store):
    """Create a test session."""
    session_id = "test-session"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}
    await bigquery_adk_store.create_session(session_id, app_name, user_id, state)
    return {"session_id": session_id, "app_name": app_name, "user_id": user_id}
