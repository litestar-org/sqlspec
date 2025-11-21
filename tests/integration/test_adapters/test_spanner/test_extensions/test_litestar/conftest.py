from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.litestar import SpannerSyncStore


@pytest.fixture(scope="session")
def spanner_litestar_config(spanner_service: Any) -> SpannerSyncConfig:
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
        extension_config={"litestar": {"session_table": "litestar_sessions"}},
    )


@pytest.fixture
async def spanner_store(spanner_litestar_config: SpannerSyncConfig) -> AsyncGenerator[SpannerSyncStore, None]:
    store = SpannerSyncStore(spanner_litestar_config)
    await store.create_table()
    try:
        yield store
    finally:
        await store.delete_all()
