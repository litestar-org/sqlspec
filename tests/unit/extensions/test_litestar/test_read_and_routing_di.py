"""Litestar read and routing dependencies against separate SQLite databases."""

from typing import Any, cast

import pytest
from litestar import Request, get, post
from litestar.di import NamedDependency
from litestar.testing import create_test_client

from sqlspec import sign_write_pin
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.litestar import AiosqliteStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import LitestarConfig
from sqlspec.driver._routing import SyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import DeferredAsyncSession
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.routing import RoutingContext, RoutingEntry
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


def test_autocommit_skips_commit_and_rollback_on_unpinned_routing_session(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = SqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"litestar": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    commits: list[bool] = []
    rollbacks: list[bool] = []
    orig_commit = SyncRoutingSession.commit
    orig_rollback = SyncRoutingSession.rollback

    def spy_commit(self: SyncRoutingSession) -> None:
        commits.append(self.pinned_to_primary)
        orig_commit(self)

    def spy_rollback(self: SyncRoutingSession) -> None:
        rollbacks.append(self.pinned_to_primary)
        orig_rollback(self)

    monkeypatch.setattr(SyncRoutingSession, "commit", spy_commit)
    monkeypatch.setattr(SyncRoutingSession, "rollback", spy_rollback)

    @get("/read-only", sync_to_thread=False)
    def read_only(router: NamedDependency[Any]) -> dict[str, str]:
        return {"origin": cast(str, router.select_value("SELECT origin FROM notes WHERE id=1"))}

    @post("/write", sync_to_thread=False)
    def write_pin(router: NamedDependency[Any]) -> dict[str, str]:
        router.execute("INSERT INTO notes VALUES (4,'autocommit')")
        return {"status": "ok"}

    with create_test_client([read_only, write_pin], plugins=[SQLSpecPlugin(config)]) as client:
        assert client.get("/read-only").json() == {"origin": "replica"}
        assert commits == []
        assert rollbacks == []
        assert client.post("/write").json() == {"status": "ok"}
        assert commits == [True]
        assert rollbacks == []


def test_litestar_safe_default_routing_signed_cookie_and_deferred_cache(tmp_path: Any) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={
            "litestar": {"routing_session_key": "router", "write_pin_cookie": "wp", "routing_secret": "litestar-secret"}
        },
    )
    plugin = SQLSpecPlugin(config)

    @get("/inspect")
    async def inspect_route(request: Request) -> dict[str, Any]:
        first = plugin.get_routing_session(request, "router")
        second = plugin.get_routing_session(request, "router")
        assert isinstance(first, DeferredAsyncSession)
        assert first is second
        with pytest.raises(ImproperConfigurationError, match="provide_request_routing_session_async"):
            _ = first.primary_session
        origin = await first.select_value("SELECT origin FROM notes WHERE id=1")
        entry = RoutingContext.get_entry("shop") or RoutingEntry()
        return {"origin": origin, "token": entry.causal_token}

    with create_test_client([inspect_route], plugins=[plugin]) as client:
        assert client.get("/inspect", headers={"x-sqlspec-routing": "primary"}).json() == {
            "origin": "replica",
            "token": None,
        }
        assert client.get("/inspect", headers={"cookie": "wp=1"}).json() == {"origin": "replica", "token": None}
        signed = sign_write_pin("litestar-secret", bind_key="shop", causal_token="lsn-9")
        assert client.get("/inspect", headers={"cookie": f"wp={signed}"}).json() == {
            "origin": "primary",
            "token": "lsn-9",
        }


def test_litestar_disabled_routing_middleware_and_outer_context_precedence(tmp_path: Any) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    disabled_config = SqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"litestar": {"enable_routing_middleware": False}},
    )
    default_primary_config = SqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={"litestar": {"routing_session_key": "router", "default_routing_mode": "primary"}},
    )

    @get("/scope-active", sync_to_thread=False)
    def scope_active() -> dict[str, bool]:
        return {"has_scope": RoutingContext.current() is not None}

    @get("/origin", sync_to_thread=False)
    def origin(router: NamedDependency[Any]) -> dict[str, str]:
        return {"origin": cast(str, router.select_value("SELECT origin FROM notes WHERE id=1"))}

    with create_test_client([scope_active], plugins=[SQLSpecPlugin(disabled_config)]) as client:
        assert client.get("/scope-active").json() == {"has_scope": False}

    with (
        RoutingContext.replica("shop"),
        create_test_client([origin], plugins=[SQLSpecPlugin(default_primary_config)]) as client,
    ):
        assert client.get("/origin").json() == {"origin": "replica"}
