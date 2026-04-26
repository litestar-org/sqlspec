"""Tests for Sanic SQLSpec plugin skeleton."""

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.config import SanicConfig
from sqlspec.extensions.sanic import SanicConfigState, SQLSpecPlugin, get_connection_from_request, get_or_create_session


def test_sanic_config_typing_is_exported() -> None:
    """SanicConfig should expose the Sanic extension configuration surface."""
    assert SanicConfig.__required_keys__ == frozenset()
    assert "connection_key" in SanicConfig.__annotations__
    assert "pool_key" in SanicConfig.__annotations__
    assert "session_key" in SanicConfig.__annotations__
    assert "commit_mode" in SanicConfig.__annotations__


def test_sanic_public_api_imports_without_sanic_dependency() -> None:
    """The extension module should expose its public API without importing Sanic."""
    assert SQLSpecPlugin is not None
    assert SanicConfigState is not None
    assert callable(get_connection_from_request)
    assert callable(get_or_create_session)


def test_sanic_plugin_reads_sanic_extension_config() -> None:
    """The skeleton plugin should build config state from extension_config['sanic']."""
    sqlspec = SQLSpec()
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"sanic": {"session_key": "sanic_db"}}
    )
    sqlspec.add_config(config)

    plugin = SQLSpecPlugin(sqlspec)

    assert len(plugin._config_states) == 1  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].session_key == "sanic_db"  # pyright: ignore[reportPrivateUsage]
    assert plugin._config_states[0].sqlcommenter_framework == "sanic"  # pyright: ignore[reportPrivateUsage]
