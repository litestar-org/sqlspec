"""Regression tests for SQLSpecPlugin key-lookup parity across state accessors."""

from typing import TYPE_CHECKING, Any, cast

import pytest
from litestar.config.app import AppConfig
from litestar.datastructures.state import State

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.litestar._utils import set_sqlspec_scope_state
from sqlspec.extensions.litestar.plugin import DEFAULT_CONNECTION_KEY, DEFAULT_SESSION_KEY, SQLSpecPlugin

if TYPE_CHECKING:
    from litestar.types import HTTPScope

REPLICA_KEYS: "dict[str, Any]" = {
    "connection_key": "replica_connection",
    "pool_key": "replica_pool",
    "session_key": "replica_session",
}
ALIASED_KEYS: "dict[str, Any]" = {
    "connection_key": "aliased_connection",
    "pool_key": "aliased_pool",
    "session_key": "aliased_session",
}


def _build_initialized_plugin() -> SQLSpecPlugin:
    """Build a plugin and run on_app_init, which populates PluginConfigState.annotation."""
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    plugin.on_app_init(AppConfig())
    return plugin


def _build_dual_plugin() -> "tuple[SQLSpecPlugin, AiosqliteConfig, AiosqliteConfig]":
    """Build a plugin holding two configurations of the same concrete type."""
    sqlspec = SQLSpec()
    primary = sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}, bind_key="primary"))
    replica = sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"}, bind_key="replica", extension_config={"litestar": REPLICA_KEYS}
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec), primary, replica


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


def test_post_registration_identifiers_agree() -> None:
    """Bind key, unique config type, instance, and dependency keys resolve to one config."""
    sqlspec = SQLSpec()
    config = sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}, bind_key="primary"))
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    plugin.on_app_init(AppConfig())

    assert plugin.get_config("primary") is config
    assert plugin.get_config(AiosqliteConfig) is config
    assert plugin.get_config(config) is config
    assert plugin.get_config(DEFAULT_SESSION_KEY) is config
    assert plugin.get_config(DEFAULT_CONNECTION_KEY) is config


def test_distinct_bind_keys_resolve_before_registration() -> None:
    """Two configs of the same type stay distinguishable by bind key at construction."""
    plugin, primary, replica = _build_dual_plugin()
    assert plugin.get_config("primary") is primary
    assert plugin.get_config("replica") is replica


def test_distinct_bind_keys_resolve_after_registration() -> None:
    """Bind-key resolution is unchanged once on_app_init has run."""
    plugin, primary, replica = _build_dual_plugin()
    plugin.on_app_init(AppConfig())
    assert plugin.get_config("primary") is primary
    assert plugin.get_config("replica") is replica


def test_repeated_config_type_is_ambiguous_before_registration() -> None:
    """A repeated concrete type never falls back to first-match selection."""
    plugin, _, _ = _build_dual_plugin()
    with pytest.raises(KeyError) as exc_info:
        plugin.get_config(AiosqliteConfig)
    message = str(exc_info.value)
    assert "primary" in message
    assert "replica" in message


def test_repeated_config_type_is_ambiguous_after_registration() -> None:
    """Registration does not resolve the ambiguity of a repeated concrete type."""
    plugin, _, _ = _build_dual_plugin()
    plugin.on_app_init(AppConfig())
    with pytest.raises(KeyError) as exc_info:
        plugin.get_config(AiosqliteConfig)
    message = str(exc_info.value)
    assert "primary" in message
    assert "replica" in message


def test_unbound_config_type_ambiguity_lists_marker() -> None:
    """Configs without a bind key are reported with an explicit marker."""
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    sqlspec.add_config(
        AiosqliteConfig(connection_config={"database": ":memory:"}, extension_config={"litestar": REPLICA_KEYS})
    )
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    with pytest.raises(KeyError, match="<unbound>"):
        plugin.get_config(AiosqliteConfig)


def test_bind_key_wins_over_colliding_dependency_key() -> None:
    """get_config() prefers a registry bind key while request accessors keep dependency-key meaning."""
    sqlspec = SQLSpec()
    aliased = sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            bind_key=DEFAULT_SESSION_KEY,
            extension_config={"litestar": ALIASED_KEYS},
        )
    )
    default = sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    plugin.on_app_init(AppConfig())

    assert plugin.get_config(DEFAULT_SESSION_KEY) is aliased
    assert plugin.get_config(default) is default

    scope = cast("HTTPScope", {"type": "http"})
    connection = object()
    set_sqlspec_scope_state(scope, DEFAULT_CONNECTION_KEY, connection)
    assert plugin.provide_request_connection(DEFAULT_SESSION_KEY, State(), scope) is connection
