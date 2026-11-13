"""Flask request routing and portal context receipts with real embedded drivers."""

import asyncio
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from flask import Flask

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions._deferred_session import call_portal_with_context
from sqlspec.extensions.flask import SQLSpecPlugin
from sqlspec.utils.portal import Portal, PortalProvider
from sqlspec.utils.routing import RoutingContext, RoutingEntry
from sqlspec.utils.tenant import TenantContext
from tests.unit.driver.test_sync_routing_session import seed_routing_databases


@pytest.fixture
def portal() -> Iterator[Portal]:
    provider = PortalProvider()
    provider.start()
    try:
        yield provider.portal
    finally:
        provider.stop()


@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
def test_portal_receipts_preserve_identity_and_write_pins(portal: Portal, outcome: str) -> None:
    async def operation() -> str:
        assert TenantContext.get_tenant_id() == "acme"
        assert cast(RoutingEntry, RoutingContext.get_entry("shop")).mode == "replica"
        RoutingContext.mark_write("shop", causal_token="lsn-42")
        if outcome == "error":
            raise ValueError("after write")
        if outcome == "cancel":
            raise asyncio.CancelledError
        return "done"

    with TenantContext.scope("acme"), RoutingContext.replica("shop"):
        if outcome == "error":
            with pytest.raises(ValueError, match="after write"):
                call_portal_with_context(portal, operation, routing_bind_key="shop")
        elif outcome == "cancel":
            with pytest.raises(asyncio.CancelledError):
                call_portal_with_context(portal, operation, routing_bind_key="shop")
        else:
            assert call_portal_with_context(portal, operation, routing_bind_key="shop") == "done"
        entry = cast(RoutingEntry, RoutingContext.get_entry("shop"))
        assert (entry.mode, entry.pinned_by_write, entry.causal_token) == ("primary", True, "lsn-42")
        assert TenantContext.get_tenant_id() == "acme"
    assert RoutingContext.current() is None


def test_portal_timeout_pins_named_bind_without_waiting_for_late_receipt(portal: Portal) -> None:
    started = threading.Event()
    release = threading.Event()
    finalized = threading.Event()

    async def operation() -> None:
        RoutingContext.mark_write("shop", causal_token="unconfirmed")
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.to_thread(release.wait, 3)
            finalized.set()

    async def ping() -> str:
        return "done"

    try:
        with RoutingContext.scope():
            RoutingContext.mark_write("other", causal_token="other-token")
            RoutingContext.mark_write("shop", causal_token="previous")
            with pytest.raises(ImproperConfigurationError, match="timed out"):
                call_portal_with_context(portal, operation, timeout=0.1, routing_bind_key="shop")
            assert started.is_set()
            assert not finalized.is_set()
            assert cast(RoutingEntry, RoutingContext.get_entry("shop")).causal_token == "previous"
            assert cast(RoutingEntry, RoutingContext.get_entry("other")).causal_token == "other-token"
        with RoutingContext.scope():
            release.set()
            assert finalized.wait(3)
            assert call_portal_with_context(portal, ping) == "done"
            assert RoutingContext.get_entry("shop") is None
    finally:
        release.set()


def test_portal_without_completion_conservatively_pins_and_overrides_tenant(portal: Portal) -> None:
    async def observe() -> str | None:
        return TenantContext.get_tenant_id()

    with TenantContext.scope("ambient"), RoutingContext.scope():
        assert call_portal_with_context(portal, observe, tenant_override=None) is None
        assert TenantContext.get_tenant_id() == "ambient"
    provider = PortalProvider()
    with RoutingContext.scope():
        with pytest.raises(ImproperConfigurationError, match="not running"):
            call_portal_with_context(provider.portal, observe, routing_bind_key="named")
        assert cast(RoutingEntry, RoutingContext.get_entry("named")).pinned_by_write
    assert RoutingContext.current() is None


@pytest.mark.parametrize("async_config", [False, True])
def test_flask_read_and_routing_sessions_with_autocommit(tmp_path: Path, async_config: bool) -> None:
    primary, replica = seed_routing_databases(tmp_path)
    config_type = AiosqliteConfig if async_config else SqliteConfig
    config = config_type(
        connection_config={"database": primary},
        replicas=[{"database": replica}],
        bind_key="shop",
        extension_config={"flask": {"routing_session_key": "router", "commit_mode": "autocommit"}},
    )
    app = Flask(__name__)
    plugin = SQLSpecPlugin(config, app)

    @app.get("/read")
    def read() -> dict[str, Any]:
        before = RoutingContext.get_entry("shop")
        session = plugin.get_read_session()
        assert plugin.get_read_session() is session
        return {
            "origin": session.select_value("SELECT origin FROM notes WHERE id=1"),
            "was_pinned": bool(before and before.pinned_by_write),
        }

    @app.post("/route")
    def route() -> dict[str, str]:
        router = plugin.get_routing_session()
        before = router.select_value("SELECT origin FROM notes WHERE id=1")
        router.execute("INSERT INTO notes VALUES (4,'new')")
        assert cast(RoutingEntry, RoutingContext.get_entry("shop")).pinned_by_write
        after = plugin.get_read_session().select_value("SELECT origin FROM notes WHERE id=1")
        return {"before": before, "after": after}

    try:
        client = app.test_client()
        assert client.get("/read").json == {"origin": "replica", "was_pinned": False}
        assert client.post("/route").json == {"before": "replica", "after": "primary"}
        assert client.get("/read").json == {"origin": "replica", "was_pinned": False}
    finally:
        plugin.shutdown()
    assert RoutingContext.current() is None
