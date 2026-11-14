"""Registry read providers retain selected configuration ownership."""

import asyncio
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import (
    SQLSpec,
    _RuntimeAsyncContext,
    _RuntimeReadAsyncContext,
    _RuntimeReadSyncContext,
    _RuntimeSyncContext,
)
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.observability import LifecycleContext, ObservabilityConfig, TelemetryConfig
from sqlspec.utils.routing import RoutingContext


def _watch(config: SqliteConfig | AiosqliteConfig, events: list[tuple[str, str]], name: str) -> None:
    runtime = config.get_observability_runtime()
    for event in ("connection_create", "session_start", "session_end", "connection_destroy"):

        def record(_context: LifecycleContext, event: str = event) -> None:
            events.append((name, event))

        runtime.register_lifecycle_hook(f"on_{event}", record)


def test_read_session_uses_selected_runtime_and_distinct_replica_metrics() -> None:
    replica = SqliteConfig()
    analytics = SqliteConfig()
    primary = SqliteConfig(bind_key="orders", replicas={"default": [replica], "analytics": [analytics]})
    spec = SQLSpec(observability_config=ObservabilityConfig(telemetry=TelemetryConfig()))
    spec.add_config(replica)
    spec.add_config(primary)
    events: list[tuple[str, str]] = []
    _watch(primary, events, "primary")
    _watch(replica, events, "replica")
    try:
        with spec.provide_read_session(primary) as session:
            assert session.select_value("SELECT 1") == 1
        with spec.provide_read_session(primary, bind_group="analytics") as session:
            assert session.select_value("SELECT 2") == 2
        assert events == [
            ("replica", "connection_create"),
            ("replica", "session_start"),
            ("replica", "session_end"),
            ("replica", "connection_destroy"),
        ]
        primary.get_observability_runtime().record_metric("read_probe", 1.0)
        replica.get_observability_runtime().record_metric("read_probe", 2.0)
        analytics.get_observability_runtime().record_metric("read_probe", 3.0)
        snapshot = spec.telemetry_snapshot()
        assert snapshot["orders.read_probe"] == 1.0
        assert snapshot["orders:replica:0.read_probe"] == 2.0
        assert snapshot["orders:replica:analytics:0.read_probe"] == 3.0
    finally:
        asyncio.run(spec.close_all_pools())


@pytest.mark.parametrize("fail", [False, True])
def test_read_connection_emits_balanced_selected_events(fail: bool) -> None:
    replica = SqliteConfig()
    primary = SqliteConfig(replicas=[replica])
    spec = SQLSpec()
    spec.add_config(primary)
    events: list[tuple[str, str]] = []
    _watch(primary, events, "primary")
    _watch(replica, events, "replica")
    try:
        try:
            with spec.provide_read_connection(primary) as connection:
                assert connection.execute("SELECT 3").fetchone()[0] == 3
                if fail:
                    raise ValueError("caller failed")
        except ValueError as error:
            assert str(error) == "caller failed"
        assert events == [("replica", "connection_create"), ("replica", "connection_destroy")]
    finally:
        asyncio.run(spec.close_all_pools())


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", ["normal", "error", "cancel"])
async def test_async_read_session_emits_balanced_selected_events(outcome: str) -> None:
    replica = AiosqliteConfig()
    primary = AiosqliteConfig(replicas=[replica])
    spec = SQLSpec()
    spec.add_config(primary)
    events: list[tuple[str, str]] = []
    _watch(primary, events, "primary")
    _watch(replica, events, "replica")
    try:
        try:
            async with spec.provide_read_session(primary) as session:
                assert await session.select_value("SELECT 4") == 4
                if outcome == "error":
                    raise ValueError("caller failed")
                if outcome == "cancel":
                    raise asyncio.CancelledError("caller cancelled")
        except (ValueError, asyncio.CancelledError) as error:
            assert str(error) == ("caller failed" if outcome == "error" else "caller cancelled")
        assert events == [
            ("replica", "connection_create"),
            ("replica", "session_start"),
            ("replica", "session_end"),
            ("replica", "connection_destroy"),
        ]
    finally:
        await spec.close_all_pools()


def test_read_provider_without_replicas_preserves_primary_identity() -> None:
    spec = SQLSpec()
    first = SqliteConfig()
    second = SqliteConfig()
    try:
        with spec.provide_read_session(first) as session:
            session.execute("CREATE TABLE only_first (id INTEGER)")
        with spec.provide_read_session(second) as session:
            assert session.select_value("SELECT COUNT(*) FROM sqlite_master WHERE name = 'only_first'") == 0
        assert spec.configs[id(first)] is first
        assert spec.configs[id(second)] is second
    finally:
        asyncio.run(spec.close_all_pools())


def test_forced_primary_read_retains_primary_runtime() -> None:
    replica = SqliteConfig()
    primary = SqliteConfig(bind_key="orders", replicas=[replica])
    spec = SQLSpec()
    spec.add_config(primary)
    events: list[tuple[str, str]] = []
    _watch(primary, events, "primary")
    _watch(replica, events, "replica")
    try:
        with RoutingContext.primary("orders"), spec.provide_read_session(primary) as session:
            assert session.select_value("SELECT 5") == 5
        assert events == [
            ("primary", "connection_create"),
            ("primary", "session_start"),
            ("primary", "session_end"),
            ("primary", "connection_destroy"),
        ]
    finally:
        asyncio.run(spec.close_all_pools())


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_failed_primary_pool_close_retains_retry_and_closes_replicas(asynchronous: bool) -> None:
    from unittest.mock import AsyncMock, MagicMock

    pool = MagicMock()
    pool.is_closed = False
    pool.close = (
        AsyncMock(side_effect=OSError("primary close failed"))
        if asynchronous
        else MagicMock(side_effect=OSError("primary close failed"))
    )
    replica = AiosqliteConfig() if asynchronous else SqliteConfig()
    primary = (
        AiosqliteConfig(connection_instance=pool, replicas=[replica])
        if asynchronous
        else SqliteConfig(connection_instance=pool, replicas=[replica])
    )
    if isinstance(replica, AiosqliteConfig):
        await replica.create_pool()
    else:
        replica.create_pool()
    with pytest.raises(OSError, match="primary close failed"):
        if isinstance(primary, AiosqliteConfig):
            await primary.close_pool()
        else:
            primary.close_pool()
    assert primary.connection_instance is pool
    assert replica.connection_instance is None
    pool.close.side_effect = None
    if isinstance(primary, AiosqliteConfig):
        await primary.close_pool()
    else:
        primary.close_pool()
    assert primary.connection_instance is None


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_runtime_contexts_cleanup_on_lifecycle_hook_failures(asynchronous: bool) -> None:
    config = AiosqliteConfig() if asynchronous else SqliteConfig()
    destroyed: list[Any] = []
    exited: list[bool] = []

    class _BoomRuntime:
        def __init__(self, *, fail_start: bool, fail_end: bool) -> None:
            self.fail_start = fail_start
            self.fail_end = fail_end

        def emit_connection_create_sync(self, connection: Any) -> None:
            return None

        async def emit_connection_create_async(self, connection: Any) -> None:
            return None

        def emit_session_start_sync(self, session: Any) -> None:
            if self.fail_start:
                raise RuntimeError("session_start boom")

        async def emit_session_start_async(self, session: Any) -> None:
            if self.fail_start:
                raise RuntimeError("session_start boom")

        def emit_session_end_sync(self, session: Any) -> None:
            if self.fail_end:
                raise RuntimeError("session_end boom")

        async def emit_session_end_async(self, session: Any) -> None:
            if self.fail_end:
                raise RuntimeError("session_end boom")

        def emit_connection_destroy_sync(self, connection: Any) -> None:
            destroyed.append(connection)

        async def emit_connection_destroy_async(self, connection: Any) -> None:
            destroyed.append(connection)

    class _FakeReadSession:
        def __init__(self) -> None:
            self.connection: Any = object()

        def _detach_connection(self) -> None:
            self.connection = _DETACHED_CONNECTION

    class _FakeConfig:
        def __init__(self, runtime: _BoomRuntime) -> None:
            self._runtime = runtime

        def get_observability_runtime(self) -> _BoomRuntime:
            return self._runtime

    class _FakeReadProvider:
        def __init__(self, runtime: _BoomRuntime) -> None:
            self._selection_receipt = type("Receipt", (), {"config": _FakeConfig(runtime)})()
            self.session = _FakeReadSession()

        def __enter__(self) -> Any:
            return self.session

        def __exit__(self, *args: Any) -> None:
            exited.append(True)

        async def __aenter__(self) -> Any:
            return self.session

        async def __aexit__(self, *args: Any) -> None:
            exited.append(True)

    captured: list[Any] = []
    end_boom_runtime: Any = _BoomRuntime(fail_start=False, fail_end=True)
    try:
        with pytest.raises(RuntimeError, match="session_end boom"):
            if isinstance(config, AiosqliteConfig):
                async with _RuntimeAsyncContext(config.provide_session(), end_boom_runtime, config) as session:
                    captured.append(session)
            else:
                with _RuntimeSyncContext(config.provide_session(), end_boom_runtime, config) as session:
                    captured.append(session)
        assert len(destroyed) == 1
        assert captured[0].connection is _DETACHED_CONNECTION

        destroyed.clear()
        start_boom_runtime = _BoomRuntime(fail_start=True, fail_end=False)
        failing_read_provider = _FakeReadProvider(start_boom_runtime)
        with pytest.raises(RuntimeError, match="session_start boom"):
            if asynchronous:
                async with _RuntimeReadAsyncContext(failing_read_provider, session=True):
                    pass
            else:
                with _RuntimeReadSyncContext(failing_read_provider, session=True):
                    pass
        assert exited == [True]
        assert failing_read_provider.session.connection is _DETACHED_CONNECTION
    finally:
        if isinstance(config, AiosqliteConfig):
            await config.close_pool()
        else:
            config.close_pool()


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_runtime_read_context_detaches_session_and_skips_detached_destroy(asynchronous: bool) -> None:
    config = AiosqliteConfig() if asynchronous else SqliteConfig()
    destroyed: list[Any] = []
    config.get_observability_runtime().register_lifecycle_hook(
        "on_connection_destroy", lambda ctx: destroyed.append(ctx.get("connection"))
    )

    class _FakeReadSession:
        def __init__(self, pre_detached: bool) -> None:
            self.connection: Any = _DETACHED_CONNECTION if pre_detached else object()

        def _detach_connection(self) -> None:
            self.connection = _DETACHED_CONNECTION

    class _FakeReadProvider:
        def __init__(self, pre_detached: bool) -> None:
            self._selection_receipt = type("Receipt", (), {"config": config})()
            self.session = _FakeReadSession(pre_detached)

        def __enter__(self) -> Any:
            return self.session

        def __exit__(self, *args: Any) -> None:
            return None

        async def __aenter__(self) -> Any:
            return self.session

        async def __aexit__(self, *args: Any) -> None:
            return None

    try:
        provider_detached = _FakeReadProvider(pre_detached=True)
        provider_live = _FakeReadProvider(pre_detached=False)
        if asynchronous:
            async with _RuntimeReadAsyncContext(provider_detached, session=True):
                pass
            async with _RuntimeReadAsyncContext(provider_live, session=True):
                pass
        else:
            with _RuntimeReadSyncContext(provider_detached, session=True):
                pass
            with _RuntimeReadSyncContext(provider_live, session=True):
                pass

        assert provider_live.session.connection is _DETACHED_CONNECTION
        assert len(destroyed) == 1
        assert destroyed[0] is not _DETACHED_CONNECTION
    finally:
        if isinstance(config, AiosqliteConfig):
            await config.close_pool()
        else:
            config.close_pool()
