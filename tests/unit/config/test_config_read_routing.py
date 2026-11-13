from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import (
    ADKConfig,
    FastAPIConfig,
    FlaskConfig,
    LitestarConfig,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    ReadReplicaGroup,
    SanicConfig,
    StarletteConfig,
)
from sqlspec.core.routing import ReplicaSpec
from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError
from sqlspec.protocols import ReplicaRoutableConfigProtocol
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.tenant import TenantContext, TenantContextPayload, TenantRequestBinding


@pytest.mark.parametrize(
    "replicas",
    ["replica.db", {"database": "replica.db"}, ["replica.db"], [ReplicaSpec({"database": "replica.db"}, weight=2)]],
)
def test_replica_shorthands_preserve_primary_identity(replicas: Any) -> None:
    config = SqliteConfig(connection_config={"database": "primary.db"}, replicas=replicas, bind_key="app")
    assert isinstance(config, ReplicaRoutableConfigProtocol)
    replica = config.replica_groups["default"].configs[0]
    assert replica.is_replica
    assert replica._tenant_key is config
    assert replica.connection_config["database"] == "replica.db"
    assert not replica.has_replicas


@pytest.mark.parametrize("config_type", [SqliteConfig, AiosqliteConfig])
async def test_read_routing_honors_pins_explicit_groups_and_zero_replica_fallback(config_type: Any) -> None:
    default = config_type()
    analytics = config_type()
    primary = config_type(bind_key="app", replicas={"default": [default], "analytics": [analytics]})
    context = primary.provide_read_session()
    if primary.is_async:
        async with context as session:
            assert await session.select_value("SELECT 1") == 1
            assert context._selection_receipt.config is default
    else:
        with context as session:
            assert session.select_value("SELECT 1") == 1
            assert context._selection_receipt.config is default
    with RoutingContext.scope():
        RoutingContext.mark_write("app")
        pinned = primary.provide_read_session()
        explicit = primary.provide_read_session(bind_group="analytics")
        if primary.is_async:
            async with pinned as session:
                assert await session.select_value("SELECT 2") == 2
                assert pinned._selection_receipt.config is primary
            async with explicit as session:
                assert await session.select_value("SELECT 3") == 3
                assert explicit._selection_receipt.config is analytics
        else:
            with pinned as session:
                assert session.select_value("SELECT 2") == 2
                assert pinned._selection_receipt.config is primary
            with explicit as session:
                assert session.select_value("SELECT 3") == 3
                assert explicit._selection_receipt.config is analytics
    with pytest.raises(RoutingContextError, match="missing"):
        primary.provide_read_session(bind_group="missing")
    plain = config_type()
    fallback = plain.provide_read_session()
    if plain.is_async:
        async with fallback as session:
            assert await session.select_value("SELECT 4") == 4
            assert fallback._selection_receipt.config is plain
        await primary.close_pool()
        await plain.close_pool()
    else:
        with fallback as session:
            assert session.select_value("SELECT 4") == 4
            assert fallback._selection_receipt.config is plain
        primary.close_pool()
        plain.close_pool()


def test_replica_topology_rejects_mixed_nested_and_unsupported_modes() -> None:
    nested = SqliteConfig(replicas=[SqliteConfig()])
    for replicas in (
        [nested],
        [AiosqliteConfig()],
        {"default": ReadReplicaGroup([SqliteConfig()]), "database": "replica.db"},
    ):
        with pytest.raises(ImproperConfigurationError):
            SqliteConfig(replicas=replicas)
    with pytest.raises(ImproperConfigurationError, match="replicas=True"):
        NoPoolSyncConfig(replicas=True)
    assert not SqliteConfig(replicas=False).has_replicas
    assert not SqliteConfig(replicas={}).has_replicas


def test_extension_routing_options_are_documented_optional_fields() -> None:
    fields = {
        "read_session_key",
        "read_connection_key",
        "routing_session_key",
        "default_routing_mode",
        "enable_routing_middleware",
        "routing_header",
        "write_pin_cookie",
        "routing_secret",
    }
    for schema in (FlaskConfig, LitestarConfig, StarletteConfig, FastAPIConfig, SanicConfig):
        assert fields <= schema.__optional_keys__
    for schema in (LitestarConfig, StarletteConfig, FastAPIConfig, SanicConfig):
        assert "read_pool_key" in schema.__optional_keys__
    assert {"read_config_enabled", "read_bind_group"} <= ADKConfig.__optional_keys__


class TenantEndpoint(NoPoolAsyncConfig[Any, Any]):
    __slots__ = ()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        yield await TenantContext.arequire(self._tenant_key)


async def test_async_request_tenant_resolution_is_lazy_and_shared_with_replicas() -> None:
    seen: list[object] = []

    async def resolve(key: object) -> TenantContextPayload:
        seen.append(key)
        return TenantContextPayload(tenant_id="acme")

    primary = TenantEndpoint(replicas=[TenantEndpoint()], bind_key="app")
    binding = TenantRequestBinding(resolve, keys=(primary,))
    token = TenantContext.bind_request(binding)
    try:
        context = primary.provide_read_session()
        assert seen == []
        async with context as payload:
            assert payload.tenant_id == "acme"
        await binding.afreeze()
        with RoutingContext.primary("app"):
            async with primary.provide_read_session() as payload:
                assert payload.tenant_id == "acme"
        assert seen == [primary]
    finally:
        TenantContext.reset(token)
