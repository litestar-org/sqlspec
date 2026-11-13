"""Litestar read and routing dependencies against separate SQLite databases."""

from typing import Any, cast

import pytest
from litestar import Request, get, post
from litestar.di import NamedDependency
from litestar.testing import create_test_client

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import LitestarConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.routing import RoutingContext
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


def test_store_accepts_tenant_read_and_routing_settings() -> None:
    config = AiosqliteConfig(
        extension_config={
            "litestar": {"tenant_header": "x-tenant", "read_session_key": "reader", "routing_session_key": "router"}
        }
    )
    assert BaseSQLSpecStore.extension_config_options == frozenset(
        LitestarConfig.__required_keys__ | LitestarConfig.__optional_keys__
    )
    assert AiosqliteStore(config).config is config


@pytest.mark.parametrize(
    "settings",
    [
        {"session_key": "same", "read_session_key": "same"},
        {"connection_key": "db_session"},
        {"read_pool_key": "read_connection"},
    ],
)
def test_litestar_rejects_cross_field_dependency_collisions(settings: Any) -> None:
    config = SqliteConfig(extension_config={"litestar": settings})
    plugin = SQLSpecPlugin(config)
    with pytest.raises(ImproperConfigurationError, match="unique"):
        with create_test_client([], plugins=[plugin]):
            pass


def test_sync_read_and_routing_di_use_separate_databases(tmp_path: Any) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = SqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"litestar": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    plugin = SQLSpecPlugin(config)

    @get("/read", sync_to_thread=False)
    def read(read_session: NamedDependency[Any]) -> dict[str, str]:
        return {"origin": read_session.select_value("SELECT origin FROM notes WHERE id=1")}

    @post("/route", sync_to_thread=False)
    def route(router: NamedDependency[Any]) -> dict[str, str]:
        before = router.select_value("SELECT origin FROM notes WHERE id=1")
        router.execute("INSERT INTO notes VALUES (4,'inserted')")
        after = router.select_value("SELECT origin FROM notes WHERE id=1")
        return {"before": before, "after": after}

    with create_test_client([read, route], plugins=[plugin]) as client:
        assert client.get("/read").json() == {"origin": "replica"}
        assert "read_pool" in client.app.state
        assert client.post("/route").json() == {"before": "replica", "after": "primary"}
        with config.provide_session() as session:
            assert session.select_value("SELECT origin FROM notes WHERE id=4") == "inserted"
    assert RoutingContext.current() is None


def test_async_imperative_deferred_read_and_context_isolation(tmp_path: Any) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"litestar": {"routing_session_key": "router"}},
    )
    plugin = SQLSpecPlugin(config)

    @get("/read")
    async def read(request: Request) -> dict[str, Any]:
        session = plugin.get_read_session(request)
        assert isinstance(session, DeferredAsyncSession)
        with pytest.raises(ImproperConfigurationError, match="asynchronous"):
            plugin.get_read_connection(request)
        before = RoutingContext.get_entry()
        value = await session.select_value("SELECT origin FROM notes WHERE id=1")
        RoutingContext.mark_write()
        return {"origin": value, "was_pinned": bool(before and before.pinned_by_write)}

    @get("/route")
    async def route(router: NamedDependency[Any]) -> dict[str, str]:
        return {"origin": await router.select_value("SELECT origin FROM notes WHERE id=1")}

    with create_test_client([read, route], plugins=[plugin]) as client:
        assert client.get("/read").json() == {"origin": "replica", "was_pinned": False}
        assert client.get("/read").json() == {"origin": "replica", "was_pinned": False}
        assert client.get("/route").json() == {"origin": "replica"}
    assert RoutingContext.current() is None


def test_config_default_primary_routing_applies_to_request(tmp_path: Any) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = SqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"litestar": {"routing_session_key": "router", "default_routing_mode": "primary"}},
    )

    @get("/", sync_to_thread=False)
    def read(router: NamedDependency[Any]) -> dict[str, str]:
        return {"origin": cast(str, router.select_value("SELECT origin FROM notes WHERE id=1"))}

    with create_test_client([read], plugins=[SQLSpecPlugin(config)]) as client:
        assert client.get("/").json() == {"origin": "primary"}
