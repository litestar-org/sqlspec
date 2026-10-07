"""Fixtures for Spanner event queue tests."""

from collections.abc import AsyncGenerator
from inspect import isawaitable
from typing import TYPE_CHECKING, cast

import pytest
from google.api_core import exceptions as api_exceptions
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore
from tests.integration.fixtures.spanner import build_spanner_connection_config

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

    from sqlspec.config import ExtensionConfigs

__all__ = ("spanner_async_events_config", "spanner_event_store", "spanner_events_config", "spanner_events_mode_config")


def _events_extension_config() -> "ExtensionConfigs":
    return {"events": {"queue_table": "sqlspec_event_queue"}}


@pytest.fixture(scope="session")
def spanner_events_config(spanner_service: SpannerService, spanner_database: "Database") -> SpannerSyncConfig:
    """Create SpannerSyncConfig with events extension enabled."""
    del spanner_database
    return SpannerSyncConfig(
        connection_config=build_spanner_connection_config(spanner_service), extension_config=_events_extension_config()
    )


@pytest.fixture(scope="session")
async def spanner_async_events_config(
    spanner_service: SpannerService, spanner_database: "Database"
) -> "AsyncGenerator[SpannerAsyncConfig, None]":
    """Create SpannerAsyncConfig with events extension enabled."""
    del spanner_database
    config = SpannerAsyncConfig(
        connection_config=build_spanner_connection_config(spanner_service), extension_config=_events_extension_config()
    )
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture(params=("sync", "async"))
def spanner_events_mode_config(request: pytest.FixtureRequest) -> "SpannerSyncConfig | SpannerAsyncConfig":
    """Select the sync or async Spanner configuration with events enabled."""
    fixture_name = "spanner_events_config" if request.param == "sync" else "spanner_async_events_config"
    return cast("SpannerSyncConfig | SpannerAsyncConfig", request.getfixturevalue(fixture_name))


@pytest.fixture
async def spanner_event_store(
    spanner_events_mode_config: "SpannerSyncConfig | SpannerAsyncConfig",
) -> "AsyncGenerator[SpannerSyncEventQueueStore | SpannerAsyncEventQueueStore, None]":
    """Create the event queue table for the selected adapter and drop it afterwards."""
    store: SpannerSyncEventQueueStore | SpannerAsyncEventQueueStore
    if isinstance(spanner_events_mode_config, SpannerAsyncConfig):
        store = SpannerAsyncEventQueueStore(spanner_events_mode_config)
    else:
        store = SpannerSyncEventQueueStore(spanner_events_mode_config)
    try:
        created = store.create_table()
        if isawaitable(created):
            await created
    except api_exceptions.AlreadyExists:
        pass
    try:
        yield store
    finally:
        try:
            dropped = store.drop_table()
            if isawaitable(dropped):
                await dropped
        except api_exceptions.NotFound:
            pass
