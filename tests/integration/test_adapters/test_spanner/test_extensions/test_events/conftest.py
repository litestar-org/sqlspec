"""Fixtures for Spanner event queue tests."""

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from google.api_core import exceptions as api_exceptions
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerSyncEventQueueStore

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


@pytest.fixture(scope="session")
def spanner_events_config(spanner_service: SpannerService, spanner_database: "Database") -> SpannerSyncConfig:
    """Create SpannerSyncConfig with events extension enabled."""
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
        extension_config={"events": {"queue_table": "sqlspec_event_queue"}},
    )


@pytest.fixture
def spanner_event_store(
    spanner_events_config: SpannerSyncConfig, spanner_database: "Database"
) -> Generator[SpannerSyncEventQueueStore, None, None]:
    """Create event queue store and ensure table exists."""
    store = SpannerSyncEventQueueStore(spanner_events_config)

    try:
        store.create_table()
    except api_exceptions.AlreadyExists:
        pass

    yield store

    try:
        store.drop_table()
    except api_exceptions.NotFound:
        pass
