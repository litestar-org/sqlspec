# pyright: reportPrivateUsage=false
"""Batch publication contract tests for event channels and backends."""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig
from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import AsyncEventChannel, AsyncTableEventQueue, SyncEventChannel, SyncTableEventQueue
from sqlspec.observability import ObservabilityRuntime

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _events(count: int = 1_000) -> "list[tuple[str, dict[str, Any], dict[str, Any] | None]]":
    return [(f"channel_{index % 2}", {"index": index}, {"source": "batch"}) for index in range(count)]


class _SyncFallbackBackend:
    backend_name = "sync-fallback"
    supports_sync = True

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []

    def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        self.published.append((channel, payload, metadata))
        return f"event-{len(self.published)}"


class _AsyncFallbackBackend:
    backend_name = "async-fallback"
    supports_async = True

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []

    async def publish(self, channel: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None) -> str:
        self.published.append((channel, payload, metadata))
        return f"event-{len(self.published)}"


class _AsyncBatchBackend(_AsyncFallbackBackend):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[tuple[str, dict[str, Any], dict[str, Any] | None]]] = []

    async def publish_many(self, events: "list[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        self.batches.append(events)
        return [f"batch-{index}" for index in range(len(events))]


def test_sync_event_channel_publish_many_falls_back_after_prevalidation(tmp_path: Any) -> None:
    backend = _SyncFallbackBackend()
    channel = SyncEventChannel(SqliteConfig(connection_config={"database": str(tmp_path / "sync.db")}))
    channel._backend = backend  # type: ignore[assignment]
    channel._backend_name = backend.backend_name

    ids = channel.publish_many([("first", {"index": 1}, None), ("second", {"index": 2}, {"x": 1})])

    assert ids == ["event-1", "event-2"]
    assert [event[0] for event in backend.published] == ["first", "second"]
    backend.published.clear()
    with pytest.raises(EventChannelError, match="Invalid events channel name"):
        channel.publish_many([("valid", {}, None), ("invalid-channel", {}, None)])
    assert backend.published == []


async def test_async_event_channel_publish_many_falls_back_and_empty_is_noop(tmp_path: Any) -> None:
    backend = _AsyncFallbackBackend()
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "async.db")}))
    channel._backend = backend  # type: ignore[assignment]
    channel._backend_name = backend.backend_name

    assert await channel.publish_many([]) == []
    ids = await channel.publish_many([("first", {"index": 1}, None), ("second", {"index": 2}, None)])

    assert ids == ["event-1", "event-2"]
    assert [event[0] for event in backend.published] == ["first", "second"]
    snapshot = channel._runtime.metrics_snapshot()
    assert snapshot["AiosqliteConfig.events.publish.batch_fallback"] == pytest.approx(1.0)


async def test_async_event_channel_publish_many_prefers_backend_batch(tmp_path: Any) -> None:
    backend = _AsyncBatchBackend()
    channel = AsyncEventChannel(AiosqliteConfig(connection_config={"database": str(tmp_path / "native.db")}))
    channel._backend = backend  # type: ignore[assignment]
    channel._backend_name = backend.backend_name
    events = [("first", {"index": 1}, None), ("second", {"index": 2}, None)]

    ids = await channel.publish_many(events)

    assert ids == ["batch-0", "batch-1"]
    assert backend.batches == [events]
    assert backend.published == []


class _SyncDriver:
    def __init__(self) -> None:
        self.commits = 0
        self.execute_many_calls: list[tuple[Any, list[dict[str, Any]], Any]] = []

    def execute_many(self, statement: Any, parameters: Any, *, statement_config: Any = None) -> None:
        self.execute_many_calls.append((statement, list(parameters), statement_config))

    def commit(self) -> None:
        self.commits += 1


class _AsyncDriver:
    def __init__(self) -> None:
        self.commits = 0
        self.execute_many_calls: list[tuple[Any, list[dict[str, Any]], Any]] = []

    async def execute_many(self, statement: Any, parameters: Any, *, statement_config: Any = None) -> None:
        self.execute_many_calls.append((statement, list(parameters), statement_config))

    async def commit(self) -> None:
        self.commits += 1


class _SyncQueueConfig:
    is_async = False

    def __init__(self) -> None:
        self.driver = _SyncDriver()
        self.sessions = 0
        self.statement_config = StatementConfig(dialect="sqlite")
        self.runtime = ObservabilityRuntime(config_name="SyncBatchConfig")

    def get_observability_runtime(self) -> ObservabilityRuntime:
        return self.runtime

    @contextmanager
    def provide_session(self, **_: Any) -> Any:
        self.sessions += 1
        yield self.driver


class _AsyncQueueConfig:
    is_async = True

    def __init__(self) -> None:
        self.driver = _AsyncDriver()
        self.sessions = 0
        self.statement_config = StatementConfig(dialect="sqlite")
        self.runtime = ObservabilityRuntime(config_name="AsyncBatchConfig")

    def get_observability_runtime(self) -> ObservabilityRuntime:
        return self.runtime

    @asynccontextmanager
    async def provide_session(self, **_: Any) -> "AsyncIterator[Any]":
        self.sessions += 1
        yield self.driver


def test_sync_table_queue_publish_many_uses_one_session_statement_and_commit() -> None:
    config = _SyncQueueConfig()
    queue = SyncTableEventQueue(config)  # type: ignore[arg-type]

    ids = queue.publish_many(_events())

    assert len(ids) == 1_000
    assert len(set(ids)) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 1
    assert len(config.driver.execute_many_calls[0][1]) == 1_000
    assert config.driver.commits == 1


async def test_async_table_queue_publish_many_uses_one_session_statement_and_commit() -> None:
    config = _AsyncQueueConfig()
    queue = AsyncTableEventQueue(config)  # type: ignore[arg-type]

    ids = await queue.publish_many(_events())

    assert len(ids) == 1_000
    assert len(set(ids)) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 1
    assert len(config.driver.execute_many_calls[0][1]) == 1_000
    assert config.driver.commits == 1


class _NativeAsyncConfig:
    is_async = True

    def __init__(self) -> None:
        self.connection_config: dict[str, Any] = {}
        self.driver = _AsyncDriver()
        self.sessions = 0
        self.runtime = ObservabilityRuntime(config_name=type(self).__name__)

    def get_observability_runtime(self) -> ObservabilityRuntime:
        return self.runtime

    @asynccontextmanager
    async def provide_session(self, **_: Any) -> "AsyncIterator[Any]":
        self.sessions += 1
        yield self.driver


class _AsyncpgConfig(_NativeAsyncConfig):
    pass


class _PsycopgAsyncConfig(_NativeAsyncConfig):
    pass


class _PsqlpyConfig(_NativeAsyncConfig):
    pass


_AsyncpgConfig.__module__ = "sqlspec.adapters.asyncpg.config"
_PsycopgAsyncConfig.__module__ = "sqlspec.adapters.psycopg.config"
_PsqlpyConfig.__module__ = "sqlspec.adapters.psqlpy.config"


class _NativeSyncConfig:
    is_async = False

    def __init__(self) -> None:
        self.connection_config: dict[str, Any] = {}
        self.driver = _SyncDriver()
        self.sessions = 0
        self.runtime = ObservabilityRuntime(config_name=type(self).__name__)

    def get_observability_runtime(self) -> ObservabilityRuntime:
        return self.runtime

    @contextmanager
    def provide_session(self, **_: Any) -> Any:
        self.sessions += 1
        yield self.driver


class _PsycopgSyncConfig(_NativeSyncConfig):
    pass


_PsycopgSyncConfig.__module__ = "sqlspec.adapters.psycopg.config"


class _HybridQueue:
    _insert_statement = "INSERT INTO sqlspec_event_queue VALUES (:event_id)"
    _statement_config = StatementConfig(dialect="postgres")


def _asyncpg_notify_backend(config: Any) -> Any:
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgEventsBackend

    return AsyncpgEventsBackend(config)


def _psycopg_async_notify_backend(config: Any) -> Any:
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncEventsBackend

    return PsycopgAsyncEventsBackend(config)


def _psqlpy_notify_backend(config: Any) -> Any:
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyEventsBackend

    return PsqlpyEventsBackend(config)


@pytest.mark.parametrize(
    "config_type,backend_factory",
    [
        (_AsyncpgConfig, _asyncpg_notify_backend),
        (_PsycopgAsyncConfig, _psycopg_async_notify_backend),
        (_PsqlpyConfig, _psqlpy_notify_backend),
    ],
)
async def test_async_postgres_notify_publish_many_uses_one_session_and_batch_statement(
    config_type: Any, backend_factory: Any
) -> None:
    config = config_type()
    backend = backend_factory(config)

    ids = await backend.publish_many(_events())

    assert len(ids) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 1
    assert len(config.driver.execute_many_calls[0][1]) == 1_000
    assert config.driver.commits == 1


def test_psycopg_sync_notify_publish_many_uses_one_session_and_batch_statement() -> None:
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.events.backend import PsycopgSyncEventsBackend

    config = _PsycopgSyncConfig()
    backend = PsycopgSyncEventsBackend(config)  # type: ignore[arg-type]

    ids = backend.publish_many(_events())

    assert len(ids) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 1
    assert len(config.driver.execute_many_calls[0][1]) == 1_000
    assert config.driver.commits == 1


def _asyncpg_hybrid_backend(config: Any) -> Any:
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg.events.backend import AsyncpgHybridEventsBackend

    return AsyncpgHybridEventsBackend(config, cast("Any", _HybridQueue()))


def _psycopg_async_hybrid_backend(config: Any) -> Any:
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.events.backend import PsycopgAsyncHybridEventsBackend

    return PsycopgAsyncHybridEventsBackend(config, cast("Any", _HybridQueue()))


def _psqlpy_hybrid_backend(config: Any) -> Any:
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy.events.backend import PsqlpyHybridEventsBackend

    return PsqlpyHybridEventsBackend(config, cast("Any", _HybridQueue()))


@pytest.mark.parametrize(
    "config_type,backend_factory",
    [
        (_AsyncpgConfig, _asyncpg_hybrid_backend),
        (_PsycopgAsyncConfig, _psycopg_async_hybrid_backend),
        (_PsqlpyConfig, _psqlpy_hybrid_backend),
    ],
)
async def test_async_postgres_hybrid_publish_many_bulk_inserts_and_marks_each_channel_once(
    config_type: Any, backend_factory: Any
) -> None:
    config = config_type()
    backend = backend_factory(config)

    ids = await backend.publish_many(_events())

    assert len(ids) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 2
    assert [len(call[1]) for call in config.driver.execute_many_calls] == [1_000, 2]
    assert config.driver.commits == 1


def test_psycopg_sync_hybrid_publish_many_bulk_inserts_and_marks_each_channel_once() -> None:
    pytest.importorskip("psycopg")
    from sqlspec.adapters.psycopg.events.backend import PsycopgSyncHybridEventsBackend

    config = _PsycopgSyncConfig()
    backend = PsycopgSyncHybridEventsBackend(config, _HybridQueue())  # type: ignore[arg-type]

    ids = backend.publish_many(_events())

    assert len(ids) == 1_000
    assert config.sessions == 1
    assert len(config.driver.execute_many_calls) == 2
    assert [len(call[1]) for call in config.driver.execute_many_calls] == [1_000, 2]
    assert config.driver.commits == 1
