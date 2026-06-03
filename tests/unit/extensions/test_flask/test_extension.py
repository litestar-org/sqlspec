# pyright: reportArgumentType=false
"""Tests for Flask SQLSpec plugin lifecycle and configuration."""

import tempfile
from typing import TYPE_CHECKING, Any, cast

import pytest
from flask import Flask, g

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import FlaskConfigState, SQLSpecPlugin
from sqlspec.extensions.flask._utils import get_or_create_session
from sqlspec.extensions.flask.extension import DEFAULT_SESSION_KEY

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

pytest.importorskip("flask")


def test_shutdown_closes_sync_pools(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown should dispose sync pools exactly once."""
    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)
    app = Flask(__name__)
    plugin = SQLSpecPlugin(sqlspec, app)
    close_calls = 0
    original_close_pool = SqliteConfig.close_pool

    def tracking_close_pool(self: SqliteConfig) -> None:
        nonlocal close_calls
        close_calls += 1
        return original_close_pool(self)

    monkeypatch.setattr(SqliteConfig, "close_pool", tracking_close_pool)
    plugin.shutdown()
    plugin.shutdown()
    assert close_calls == 1


def test_shutdown_closes_async_pools_and_stops_portal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown should dispose async pools and stop portal."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sqlspec = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sqlspec.add_config(config)
        app = Flask(__name__)
        plugin = SQLSpecPlugin(sqlspec, app)
        close_calls = 0
        original_close_pool = AiosqliteConfig.close_pool

        async def tracking_close_pool(self: AiosqliteConfig) -> None:
            nonlocal close_calls
            close_calls += 1
            await original_close_pool(self)

        monkeypatch.setattr(AiosqliteConfig, "close_pool", tracking_close_pool)
        plugin.shutdown()
        assert close_calls == 1
        assert plugin._portal is None


def test_default_session_key_is_db_session() -> None:
    """Flask should default to 'db_session' for consistency with other frameworks."""
    assert DEFAULT_SESSION_KEY == "db_session"


def test_uses_default_session_key_when_not_configured() -> None:
    """Plugin should use DEFAULT_SESSION_KEY when no extension_config provided."""
    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key in extension_config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec)
    assert len(plugin._config_states) == 1
    assert plugin._config_states[0].session_key == custom_key


pytest.importorskip("flask")


class _Driver:
    def __init__(self, *, connection: Any, statement_config: Any, driver_features: dict[str, Any]) -> None:
        self.connection = connection
        self.statement_config = statement_config
        self.driver_features = driver_features


class _Config:
    driver_type = _Driver
    driver_features = {"returning_support": True}
    statement_config = object()


def _make_state() -> FlaskConfigState:
    return FlaskConfigState(
        config=cast("DatabaseConfigProtocol[Any, Any, Any]", _Config()),
        connection_key="sqlspec_connection",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )


def test_utils_get_or_create_session_passes_driver_features() -> None:
    app = Flask(__name__)
    state = _make_state()
    connection = object()
    with app.app_context():
        setattr(g, state.connection_key, connection)
        session = get_or_create_session(state, portal=None)
    assert session.connection is connection
    assert session.statement_config is _Config.statement_config
    assert session.driver_features == _Config.driver_features


def test_utils_get_or_create_session_returns_cached_session() -> None:
    app = Flask(__name__)
    state = _make_state()
    with app.app_context():
        setattr(g, state.connection_key, object())
        first = get_or_create_session(state, portal=None)
        second = get_or_create_session(state, portal=None)
    assert second is first
