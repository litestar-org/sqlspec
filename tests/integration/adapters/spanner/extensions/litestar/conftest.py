from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.litestar import SpannerSyncStore

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


@pytest.fixture(scope="session")
def spanner_litestar_config(spanner_service: SpannerService, spanner_database: "Database") -> SpannerSyncConfig:
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
