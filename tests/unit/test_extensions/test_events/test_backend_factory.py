"""Backend factory selection tests for database event channels."""

import pytest

pytest.importorskip("asyncpg")
pytest.importorskip("psycopg")
pytest.importorskip("psqlpy")

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend, create_event_backend as asyncpg_factory
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.events.backend import (
    PsycopgEventsBackend,
    PsycopgHybridEventsBackend,
    create_event_backend as psycopg_factory,
)
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.events.backend import (
    PsqlpyEventsBackend,
    PsqlpyHybridEventsBackend,
    create_event_backend as psqlpy_factory,
)


def test_asyncpg_hybrid_backend_factory() -> None:
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://example"}, extension_config={"events": {}})
    backend = asyncpg_factory(config, "listen_notify_durable", {"queue_table": "app_events"})

    assert isinstance(backend, AsyncpgHybridEventsBackend)


def test_psycopg_listen_notify_backends() -> None:
    async_config = PsycopgAsyncConfig(pool_config={"dbname": "example"})
    sync_config = PsycopgSyncConfig(pool_config={"dbname": "example"})

    async_backend = psycopg_factory(async_config, "listen_notify", {})
    sync_backend = psycopg_factory(sync_config, "listen_notify", {})

    assert isinstance(async_backend, PsycopgEventsBackend)
    assert isinstance(sync_backend, PsycopgEventsBackend)


def test_psycopg_hybrid_backend_factory() -> None:
    config = PsycopgAsyncConfig(pool_config={"dbname": "example"}, extension_config={"events": {}})
    backend = psycopg_factory(config, "listen_notify_durable", {"queue_table": "sqlspec_event_queue"})

    assert isinstance(backend, PsycopgHybridEventsBackend)


def test_psqlpy_backends() -> None:
    config = PsqlpyConfig(pool_config={"dsn": "postgresql://example"}, extension_config={"events": {}})

    native_backend = psqlpy_factory(config, "listen_notify", {})
    hybrid_backend = psqlpy_factory(config, "listen_notify_durable", {})

    assert isinstance(native_backend, PsqlpyEventsBackend)
    assert isinstance(hybrid_backend, PsqlpyHybridEventsBackend)
