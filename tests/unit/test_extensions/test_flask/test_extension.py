"""Tests for Flask SQLSpec plugin lifecycle and configuration."""

import tempfile

import pytest

pytest.importorskip("flask")

from flask import Flask

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin
from sqlspec.extensions.flask.extension import DEFAULT_SESSION_KEY


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
        assert plugin._portal is None  # pyright: ignore[reportPrivateUsage]


def test_default_session_key_is_db_session() -> None:
    """Flask should default to 'db_session' for consistency with other frameworks."""
    assert DEFAULT_SESSION_KEY == "db_session"


def test_uses_default_session_key_when_not_configured() -> None:
    """Plugin should use DEFAULT_SESSION_KEY when no extension_config provided."""
    sqlspec = SQLSpec()
    config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == DEFAULT_SESSION_KEY  # pyright: ignore[reportPrivateUsage]


def test_respects_custom_session_key() -> None:
    """Plugin should respect custom session_key in extension_config."""
    custom_key = "custom_db"
    sqlspec = SQLSpec()
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"flask": {"session_key": custom_key}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == custom_key  # pyright: ignore[reportPrivateUsage]
