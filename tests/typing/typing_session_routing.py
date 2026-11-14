"""Concrete config and driver types survive read and routing providers."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from contextvars import Token
from typing import assert_type

from sqlspec import (
    SQL,
    AsyncRoutingSession,
    ReadOnlyTransactionError,
    ReplicaUnavailableError,
    RoutingContextError,
    RoutingScope,
    SQLSpec,
    SyncRoutingSession,
)
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.config import ReadReplicaGroup
from sqlspec.core.compiler import OperationProfile
from sqlspec.core.routing import RandomSelector, ReplicaHealthBreaker, ReplicaSpec, RoundRobinSelector
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload, RoutingEntry


def check_routing_primitives() -> None:
    profile = OperationProfile(True, False, True)
    assert_type(profile.is_read_only, bool)
    assert_type(SQL("SELECT 1").is_read_only(), bool)
    assert_type(ReadOnlyTransactionError("read only"), ReadOnlyTransactionError)
    assert_type(ReplicaUnavailableError("unavailable"), ReplicaUnavailableError)
    assert_type(RoutingContextError("invalid context"), RoutingContextError)
    spec = ReplicaSpec({"database": "replica.db"}, weight=2, name="reader", max_staleness_seconds=5.0)
    assert_type(spec, ReplicaSpec)
    breaker = ReplicaHealthBreaker(2, failure_threshold=3, recovery_seconds=10.0)
    assert_type(breaker.is_available(0), bool)
    assert_type(RoundRobinSelector([spec]).select_order(breaker), list[int])
    assert_type(RandomSelector([spec]).select_order(breaker), list[int])
    assert_type(RoutingContext.current(), RoutingContextPayload | None)
    assert_type(RoutingContext.get_entry("primary"), RoutingEntry | None)
    assert_type(RoutingContext.mark_write("primary"), bool)
    assert_type(RoutingContext.scope(), RoutingScope)
    token = RoutingContext.set(None)
    assert_type(token, Token[RoutingContextPayload | None])
    RoutingContext.reset(token)
    with RoutingContext.primary("analytics") as primary_context:
        assert_type(primary_context, RoutingContextPayload)
    with RoutingContext.replica(bind_group="analytics", max_staleness_seconds=5.0) as replica_context:
        assert_type(replica_context, RoutingContextPayload)
    with RoutingContext.auto("analytics") as auto_context:
        assert_type(auto_context, RoutingContextPayload)


async def check_async_routing_scopes() -> None:
    async with RoutingContext.scope() as scope:
        assert_type(scope, RoutingContextPayload)
    async with RoutingContext.primary() as primary:
        assert_type(primary, RoutingContextPayload)
    async with RoutingContext.replica(bind_group="reporting") as replica:
        assert_type(replica, RoutingContextPayload)


def check_sync_generics(primary: SqliteConfig, replica: SqliteConfig) -> None:
    group = ReadReplicaGroup([replica])
    assert_type(group, ReadReplicaGroup[SqliteConfig])
    with primary.provide_read_session() as read_driver:
        assert_type(read_driver, SqliteDriver)
    with primary.provide_routing_session() as routing_session:
        assert_type(routing_session, SyncRoutingSession[SqliteDriver])
    service = SQLSpecSyncService(config=primary, read_config=replica)
    assert_type(service, SQLSpecSyncService[SqliteDriver])
    assert_type(service.provide_session(read_only=True), AbstractContextManager[SqliteDriver])
    assert_type(service.provide_read_session(), AbstractContextManager[SqliteDriver])
    adk_store = SqliteADKStore(primary, read_config=replica)
    assert_type(adk_store.read_config, SqliteConfig | None)
    registry = SQLSpec()
    with registry.provide_read_session(primary) as registry_driver:
        assert_type(registry_driver, SqliteDriver)
    with registry.provide_routing_session(primary) as registry_routing:
        assert_type(registry_routing, SyncRoutingSession[SqliteDriver])


async def check_async_generics(primary: AiosqliteConfig, replica: AiosqliteConfig) -> None:
    group = ReadReplicaGroup([replica])
    assert_type(group, ReadReplicaGroup[AiosqliteConfig])
    async with primary.provide_read_session() as read_driver:
        assert_type(read_driver, AiosqliteDriver)
    async with primary.provide_routing_session() as routing_session:
        assert_type(routing_session, AsyncRoutingSession[AiosqliteDriver])
    service = SQLSpecAsyncService(config=primary, read_config=replica)
    assert_type(service, SQLSpecAsyncService[AiosqliteDriver])
    assert_type(service.provide_session(read_only=True), AbstractAsyncContextManager[AiosqliteDriver])
    assert_type(service.provide_read_session(), AbstractAsyncContextManager[AiosqliteDriver])
    registry = SQLSpec()
    async with registry.provide_read_session(primary) as registry_driver:
        assert_type(registry_driver, AiosqliteDriver)
    async with registry.provide_routing_session(primary) as registry_routing:
        assert_type(registry_routing, AsyncRoutingSession[AiosqliteDriver])
