from collections.abc import Generator
from typing import Any, cast

import pytest
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.adk import SpannerSyncADKStore


@pytest.fixture(scope="session")
def spanner_adk_config(spanner_service: Any) -> SpannerSyncConfig:
    host = getattr(spanner_service, "host", "localhost")
    port = getattr(spanner_service, "port", 9010)
    project_id = getattr(spanner_service, "project", "test-project")
    instance_id = getattr(spanner_service, "instance_id", getattr(spanner_service, "instance", "test-instance"))
    database_id = getattr(spanner_service, "database_id", getattr(spanner_service, "database", "test-database"))
    api_endpoint = f"{host}:{port}"

    return SpannerSyncConfig(
        pool_config={
            "project": project_id,
            "instance_id": instance_id,
            "database_id": database_id,
            "credentials": cast(Any, AnonymousCredentials()),  # type: ignore[no-untyped-call]
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
