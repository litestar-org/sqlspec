"""Sanic read/routing request leases through native test clients and signals."""

import asyncio
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic.response import json

from sqlspec import sign_write_pin
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
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
