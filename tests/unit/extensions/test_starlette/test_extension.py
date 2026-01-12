"""Tests for Starlette SQLSpec plugin."""

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.starlette import SQLSpecPlugin
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY


def test_default_session_key_is_db_session() -> None:
    """Starlette should default to 'db_session' for consistency."""
    assert DEFAULT_SESSION_KEY == "db_session"


def test_uses_default_session_key_when_not_configured() -> None:
    """Plugin should use DEFAULT_SESSION_KEY when no extension_config provided."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY  # pyright: ignore[reportPrivateUsage]


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key in extension_config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == custom_key  # pyright: ignore[reportPrivateUsage]


def test_get_session_works_in_route() -> None:
    """Test that get_session() works correctly in Starlette routes."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    plugin_ref: SQLSpecPlugin | None = None

    async def test_route(request: Request) -> JSONResponse:  # type: ignore[no-untyped-def]
        assert plugin_ref is not None
        db = plugin_ref.get_session(request)  # pyright: ignore
        result = await db.execute("SELECT 1 as value")
        return JSONResponse({"value": result.scalar()})

    routes = [Route("/test", test_route)]
    app = Starlette(routes=routes)
    plugin_ref = SQLSpecPlugin(sqlspec, app)

    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}
