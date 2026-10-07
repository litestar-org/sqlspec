"""Fixtures for Spanner Litestar session store integration tests."""

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, cast

import pytest
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.litestar import SpannerAsyncStore, SpannerSyncStore
from tests.integration.fixtures.spanner import build_spanner_connection_config

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

__all__ = ("spanner_async_litestar_config", "spanner_litestar_config", "spanner_litestar_mode_config", "spanner_store")


@pytest.fixture(scope="session")
def spanner_litestar_config(
    spanner_service: SpannerService, spanner_database: "Database"
) -> "Generator[SpannerSyncConfig, None, None]":
    """Create a sync Spanner configuration for the Litestar session store."""
    del spanner_database
    config = SpannerSyncConfig(
        connection_config=build_spanner_connection_config(spanner_service),
        extension_config={"litestar": {"session_table": "litestar_sessions"}},
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
async def spanner_async_litestar_config(
    spanner_service: SpannerService, spanner_database: "Database"
) -> "AsyncGenerator[SpannerAsyncConfig, None]":
    """Create an async Spanner configuration for the Litestar session store."""
    del spanner_database
    config = SpannerAsyncConfig(
        connection_config=build_spanner_connection_config(spanner_service),
        extension_config={"litestar": {"session_table": "litestar_sessions_async"}},
    )
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture(params=("sync", "async"))
def spanner_litestar_mode_config(request: pytest.FixtureRequest) -> "SpannerSyncConfig | SpannerAsyncConfig":
    """Select the sync or async Spanner configuration for the Litestar store."""
    fixture_name = "spanner_litestar_config" if request.param == "sync" else "spanner_async_litestar_config"
    return cast("SpannerSyncConfig | SpannerAsyncConfig", request.getfixturevalue(fixture_name))


@pytest.fixture
async def spanner_store(
    spanner_litestar_mode_config: "SpannerSyncConfig | SpannerAsyncConfig",
) -> "AsyncGenerator[SpannerSyncStore | SpannerAsyncStore, None]":
    """Provide a sync or async Spanner Litestar store with an empty session table."""
    store: SpannerSyncStore | SpannerAsyncStore
    if isinstance(spanner_litestar_mode_config, SpannerAsyncConfig):
        store = SpannerAsyncStore(spanner_litestar_mode_config)
    else:
        store = SpannerSyncStore(spanner_litestar_mode_config)
    await store.create_table()
    try:
        yield store
    finally:
        await store.delete_all()
