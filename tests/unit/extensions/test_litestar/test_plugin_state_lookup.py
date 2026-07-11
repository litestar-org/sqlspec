"""Regression tests for SQLSpecPlugin key-lookup parity across state accessors."""

from litestar.config.app import AppConfig

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.litestar.plugin import DEFAULT_CONNECTION_KEY, DEFAULT_SESSION_KEY, SQLSpecPlugin


def _build_initialized_plugin() -> SQLSpecPlugin:
    """Build a plugin and run on_app_init, which populates PluginConfigState.annotation."""
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    plugin.on_app_init(AppConfig())
    return plugin


def test_get_annotation_resolves_by_session_key() -> None:
    """get_annotation() must resolve the session key like get_config()/_get_plugin_state()."""
    plugin = _build_initialized_plugin()
    by_connection = plugin.get_annotation(DEFAULT_CONNECTION_KEY)
    by_session = plugin.get_annotation(DEFAULT_SESSION_KEY)
    assert by_session is by_connection


def test_get_config_and_get_annotation_agree_on_session_key() -> None:
    """Both accessors resolve the same config for the session key."""
    plugin = _build_initialized_plugin()
    config = plugin.get_config(DEFAULT_SESSION_KEY)
    assert plugin.get_config(DEFAULT_CONNECTION_KEY) is config
    assert plugin.get_annotation(DEFAULT_SESSION_KEY) is type(config)


def test_plugin_state_precomputes_session_instance_key() -> None:
    """Request session state uses a stable precomputed scope key."""
    plugin = _build_initialized_plugin()

    state = plugin._get_plugin_state(DEFAULT_SESSION_KEY)

    assert state.session_key_instance == f"{DEFAULT_SESSION_KEY}_instance"
