from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.adk import SpannerSyncADKStore

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


@pytest.fixture(scope="session")
def spanner_adk_config(spanner_service: SpannerService, spanner_database: "Database") -> SpannerSyncConfig:
    api_endpoint = f"{spanner_service.host}:{spanner_service.port}"

    return SpannerSyncConfig(
        connection_config={
            "project": spanner_service.project,
            "instance_id": spanner_service.instance_name,
            "database_id": spanner_service.database_name,
            "credentials": spanner_service.credentials,
            "client_options": {"api_endpoint": api_endpoint},
            "min_sessions": 1,
            "max_sessions": 5,
        },
        extension_config={"adk": {"session_table": "adk_sessions", "events_table": "adk_events"}},
    )


@pytest.fixture
def spanner_adk_store(spanner_adk_config: SpannerSyncConfig) -> Generator[SpannerSyncADKStore, None, None]:
    store = SpannerSyncADKStore(spanner_adk_config)
    store.create_tables()
    yield store
