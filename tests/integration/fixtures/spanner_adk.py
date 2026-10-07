"""Fixtures for Spanner ADK store integration tests."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.adk import (
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from tests.integration.fixtures.spanner import build_spanner_connection_config

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

__all__ = ("spanner_adk_memory_store_factory", "spanner_adk_store_factory")


def _spanner_adk_extension_config(suffix: str) -> "dict[str, Any]":
    return {
        "adk": {
            "session_table": f"adk_s_{suffix}",
            "events_table": f"adk_e_{suffix}",
            "app_state_table": f"adk_app_{suffix}",
            "user_state_table": f"adk_user_{suffix}",
            "metadata_table": f"adk_meta_{suffix}",
            "memory_table": f"adk_mem_{suffix}",
        }
    }


@pytest.fixture(params=("sync", "async"))
def spanner_adk_store_factory(
    request: pytest.FixtureRequest, spanner_service: SpannerService, spanner_database: "Database"
) -> "Callable[[], tuple[Any, Any]]":
    """Build a sync or async Spanner ADK session store with isolated tables per call."""
    del spanner_database
    connection_config = build_spanner_connection_config(spanner_service)

    def make() -> "tuple[Any, Any]":
        extension_config = _spanner_adk_extension_config(uuid4().hex[:8])
        if request.param == "sync":
            sync_config = SpannerSyncConfig(connection_config=connection_config, extension_config=extension_config)
            return sync_config, SpannerSyncADKStore(sync_config)
        async_config = SpannerAsyncConfig(connection_config=connection_config, extension_config=extension_config)
        return async_config, SpannerAsyncADKStore(async_config)

    return make


@pytest.fixture(params=("sync", "async"))
def spanner_adk_memory_store_factory(
    request: pytest.FixtureRequest, spanner_service: SpannerService, spanner_database: "Database"
) -> "Callable[[], tuple[Any, Any]]":
    """Build a sync or async Spanner ADK memory store with an isolated table per call."""
    del spanner_database
    connection_config = build_spanner_connection_config(spanner_service)

    def make() -> "tuple[Any, Any]":
        extension_config = _spanner_adk_extension_config(uuid4().hex[:8])
        if request.param == "sync":
            sync_config = SpannerSyncConfig(connection_config=connection_config, extension_config=extension_config)
            return sync_config, SpannerSyncADKMemoryStore(sync_config)
        async_config = SpannerAsyncConfig(connection_config=connection_config, extension_config=extension_config)
        return async_config, SpannerAsyncADKMemoryStore(async_config)

    return make
