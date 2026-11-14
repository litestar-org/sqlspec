"""Sanic read/routing request leases through native test clients and signals."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic.response import json

from sqlspec import sign_write_pin
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.driver._routing import AsyncRoutingSession
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.extensions._deferred_session import DeferredAsyncSession
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.utils.routing import RoutingContext, RoutingEntry
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


@pytest.mark.parametrize("async_config", [False, True])
def test_sanic_read_routing_and_request_scope_reset(tmp_path: Path, async_config: bool) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config_type = AiosqliteConfig if async_config else SqliteConfig
    config = config_type(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={"sanic": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app: Sanic[Any, Any] = Sanic("routing_" + uuid4().hex)
    plugin = SQLSpecPlugin(config, app)

    @app.get("/read")
    async def read(request: Any) -> Any:
        before = RoutingContext.get_entry("shop")
        session = plugin.get_read_session(request)
        if async_config:
            assert isinstance(session, DeferredAsyncSession)
            with pytest.raises(ImproperConfigurationError):
                plugin.get_read_connection(request)
            one, two = await asyncio.gather(
                plugin.get_read_session_async(request), plugin.get_read_session_async(request)
            )
            assert one is two
            origin = await session.select_value("SELECT origin FROM notes WHERE id=1")
        else:
            origin = session.select_value("SELECT origin FROM notes WHERE id=1")
        return json({"origin": origin, "was_pinned": bool(before and before.pinned_by_write)})

    @app.post("/route")
    async def route(request: Any) -> Any:
        session = plugin.get_routing_session(request)
        if async_config:
            before = await session.select_value("SELECT origin FROM notes WHERE id=1")
            await session.execute("INSERT INTO notes VALUES (4,'new')")
            after = await session.select_value("SELECT origin FROM notes WHERE id=1")
        else:
            before = session.select_value("SELECT origin FROM notes WHERE id=1")
            session.execute("INSERT INTO notes VALUES (4,'new')")
            after = session.select_value("SELECT origin FROM notes WHERE id=1")
        assert cast(RoutingEntry, RoutingContext.get_entry("shop")).pinned_by_write
        return json({"before": before, "after": after})

    assert app.test_client.get("/read")[1].json == {"origin": "replica", "was_pinned": False}
    assert app.test_client.post("/route")[1].json == {"before": "replica", "after": "primary"}
    assert app.test_client.get("/read")[1].json == {"origin": "replica", "was_pinned": False}
    assert RoutingContext.current() is None


def test_sanic_complete_signal_closes_residual_routing_lease(tmp_path: Path) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"sanic": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app: Sanic[Any, Any] = Sanic("disconnect_" + uuid4().hex)
    plugin = SQLSpecPlugin(config, app)

    @app.get("/abort")
    async def abort(request: Any) -> Any:
        session = await plugin.get_routing_session_async(request)
        await session.execute("INSERT INTO notes VALUES (4,'uncommitted')")
        await app.dispatch("http.lifecycle.complete", context={"request": request}, inline=True)
        with pytest.raises(SQLSpecError, match="closed"):
            await session.select_value("SELECT 1")
        return json({"closed": True})

    assert app.test_client.get("/abort")[1].json == {"closed": True}
    import sqlite3

    with sqlite3.connect(primary) as connection:
        assert connection.execute("SELECT count(*) FROM notes").fetchone() == (3,)
    assert RoutingContext.current() is None


def test_sanic_safe_routing_defaults_and_deferred_routing_error(tmp_path: Path) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={
            "sanic": {"routing_session_key": "router", "write_pin_cookie": "wp", "routing_secret": "sanic-secret"}
        },
    )
    app: Sanic[Any, Any] = Sanic("safe_routing_" + uuid4().hex)
    plugin = SQLSpecPlugin(config, app)

    @app.get("/inspect")
    async def inspect_route(request: Any) -> Any:
        deferred = plugin.get_routing_session(request)
        assert isinstance(deferred, DeferredAsyncSession)
        with pytest.raises(ImproperConfigurationError, match="get_routing_session_async"):
            _ = deferred.primary_session
        origin = await deferred.select_value("SELECT origin FROM notes WHERE id=1")
        entry = RoutingContext.get_entry("shop") or RoutingEntry()
        return json({"origin": origin, "token": entry.causal_token})

    assert app.test_client.get("/inspect", headers={"x-sqlspec-routing": "primary"})[1].json == {
        "origin": "replica",
        "token": None,
    }
    assert app.test_client.get("/inspect", headers={"cookie": "wp=1"})[1].json == {"origin": "replica", "token": None}
    signed = sign_write_pin("sanic-secret", bind_key="shop", causal_token="lsn-sanic")
    assert app.test_client.get("/inspect", headers={"cookie": f"wp={signed}"})[1].json == {
        "origin": "primary",
        "token": "lsn-sanic",
    }


@pytest.mark.anyio
@pytest.mark.parametrize("aborted", [False, True], ids=["response", "abort"])
async def test_sanic_unawaited_deferred_sessions_are_detached(tmp_path: Path, aborted: bool) -> None:
    """Unawaited primary, read, and routing DeferredAsyncSessions in Sanic are detached after completion or abort."""
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"sanic": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app: Sanic[Any, Any] = Sanic("deferred_detach_" + uuid4().hex)
    plugin = SQLSpecPlugin(config, app)
    leaked: list[DeferredAsyncSession] = []

    @app.get("/leaked")
    async def leaked_route(request: Any) -> Any:
        primary_deferred = plugin.get_session(request)
        read_deferred = plugin.get_read_session(request)
        routing_deferred = plugin.get_routing_session(request)
        assert isinstance(primary_deferred, DeferredAsyncSession)
        assert isinstance(read_deferred, DeferredAsyncSession)
        assert isinstance(routing_deferred, DeferredAsyncSession)
        leaked.extend([primary_deferred, read_deferred, routing_deferred])
        if aborted:
            await app.dispatch("http.lifecycle.complete", context={"request": request}, inline=True)
        return json({"ok": True})

    _, response = await app.asgi_client.get("/leaked")
    assert response.json == {"ok": True}
    assert len(leaked) == 3
    for handle in leaked:
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            await handle.select_value("SELECT 1")


@pytest.mark.anyio
async def test_sanic_routing_commit_failure_rolls_back_before_closing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When Sanic routing session commit raises during teardown, rollback runs before closing the context."""
    primary, replica = seed_routing_databases(tmp_path)
    config = AiosqliteConfig(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        extension_config={"sanic": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app: Sanic[Any, Any] = Sanic("routing_commit_fail_" + uuid4().hex)
    plugin = SQLSpecPlugin(config, app)
    rollbacks = 0
    orig_rollback = AsyncRoutingSession.rollback

    async def fail_commit(self: AsyncRoutingSession) -> None:
        raise RuntimeError("sanic routing commit failed")

    async def tracking_rollback(self: AsyncRoutingSession) -> None:
        nonlocal rollbacks
        rollbacks += 1
        await orig_rollback(self)

    monkeypatch.setattr(AsyncRoutingSession, "commit", fail_commit)
    monkeypatch.setattr(AsyncRoutingSession, "rollback", tracking_rollback)

    @app.post("/write")
    async def write_route(request: Any) -> Any:
        router = await plugin.get_routing_session_async(request)
        await router.execute("INSERT INTO notes VALUES (4,'uncommitted')")
        return json({"ok": True})

    _, response = await app.asgi_client.post("/write")
    assert response.status == 500
    assert rollbacks == 1

    with sqlite3.connect(primary) as connection:
        assert connection.execute("SELECT count(*) FROM notes").fetchone() == (3,)
