"""Tests for FastAPI SQLSpec plugin."""

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.fastapi import SQLSpecPlugin
from sqlspec.extensions.starlette.extension import DEFAULT_SESSION_KEY


def test_provide_session_method_exists() -> None:
    """Test that provide_session() method exists (not session_dependency())."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    # Should have provide_session method
    assert hasattr(plugin, "provide_session")
    assert callable(plugin.provide_session)

    # Should NOT have old method names from incorrect documentation
    assert not hasattr(plugin, "session_dependency")


def test_provide_connection_method_exists() -> None:
    """Test that provide_connection() method exists (not connection_dependency())."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    # Should have provide_connection method
    assert hasattr(plugin, "provide_connection")
    assert callable(plugin.provide_connection)

    # Should NOT have old method names from incorrect documentation
    assert not hasattr(plugin, "connection_dependency")


def test_uses_starlette_default_session_key() -> None:
    """FastAPI inherits from Starlette and should use same DEFAULT_SESSION_KEY."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY  # pyright: ignore[reportPrivateUsage]
    assert DEFAULT_SESSION_KEY == "db_session"


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key via starlette config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == custom_key  # pyright: ignore[reportPrivateUsage]


def test_provide_session_works_in_route() -> None:
    """Test that provide_session() works correctly in FastAPI routes."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"starlette": {"commit_mode": "autocommit"}}
    )
    sqlspec.add_config(config)

    app = FastAPI()
    plugin = SQLSpecPlugin(sqlspec, app)

    @app.get("/test")
    async def test_route(db=Depends(plugin.provide_session())):
        result = await db.execute("SELECT 1 as value")
        return {"value": result.scalar()}

    with TestClient(app) as client:
        response = client.get("/test")
        assert response.status_code == 200
        assert response.json() == {"value": 1}
