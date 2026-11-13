"""Registry read providers retain selected configuration ownership."""

import asyncio

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
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
