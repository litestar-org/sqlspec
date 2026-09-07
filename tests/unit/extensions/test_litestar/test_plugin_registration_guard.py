"""Tests for SQLSpecPlugin state access before on_app_init registration."""

from typing import Any

import pytest
from litestar.config.app import AppConfig

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.plugin import (
    DEFAULT_CONNECTION_KEY,
    DEFAULT_POOL_KEY,
    DEFAULT_SESSION_KEY,
    SQLSpecPlugin,
)

CUSTOM_KEYS = {"connection_key": "custom_connection", "pool_key": "custom_pool", "session_key": "custom_session"}


def _build_unregistered_plugin(
    bind_key: "str | None" = None, extension_config: "dict[str, Any] | None" = None
) -> "tuple[SQLSpecPlugin, AiosqliteConfig]":
    """Build a plugin without running on_app_init."""
    sqlspec = SQLSpec()
    config = sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"}, bind_key=bind_key, extension_config=extension_config or {}
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec), config


def test_get_annotations_before_registration_raises_clear_error() -> None:
    """get_annotations() before on_app_init raises ImproperConfigurationError, not AttributeError."""
    plugin, _ = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_annotations()


def test_get_annotation_before_registration_raises_clear_error() -> None:
    """get_annotation() before on_app_init raises ImproperConfigurationError, not AttributeError."""
    plugin, _ = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_annotation(DEFAULT_SESSION_KEY)


def test_get_config_by_type_before_registration_returns_config() -> None:
    """A concrete config type is a registry identity available at plugin construction."""
    plugin, config = _build_unregistered_plugin()
    assert plugin.get_config(AiosqliteConfig) is config


def test_get_config_by_instance_before_registration_returns_config() -> None:
    """A config instance resolves to itself at plugin construction."""
    plugin, config = _build_unregistered_plugin()
    assert plugin.get_config(config) is config


def test_get_config_by_bind_key_before_registration_returns_config() -> None:
    """A non-null bind_key resolves at plugin construction."""
    plugin, config = _build_unregistered_plugin(bind_key="primary")
    assert plugin.get_config("primary") is config


@pytest.mark.parametrize("di_key", [DEFAULT_CONNECTION_KEY, DEFAULT_POOL_KEY, DEFAULT_SESSION_KEY])
def test_get_config_by_default_di_key_before_registration_raises(di_key: str) -> None:
    """Generated Litestar dependency keys stay registration-bound."""
    plugin, _ = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_config(di_key)


@pytest.mark.parametrize("di_key", list(CUSTOM_KEYS.values()))
def test_get_config_by_custom_di_key_before_registration_raises(di_key: str) -> None:
    """Custom Litestar dependency keys stay registration-bound."""
    plugin, _ = _build_unregistered_plugin(bind_key="primary", extension_config={"litestar": CUSTOM_KEYS})
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_config(di_key)


def test_get_config_unknown_string_before_registration_raises_key_error() -> None:
    """An unknown identifier reports available keys instead of a registration error."""
    plugin, _ = _build_unregistered_plugin(bind_key="primary")
    with pytest.raises(KeyError, match="Available keys"):
        plugin.get_config("missing")


def test_get_config_unknown_type_before_registration_raises_key_error() -> None:
    """An unmatched config type is an unknown identifier, not a registration error."""
    plugin, _ = _build_unregistered_plugin(bind_key="primary")
    with pytest.raises(KeyError, match="Available keys"):
        plugin.get_config(SqliteConfig)


def test_get_config_foreign_instance_before_registration_raises_key_error() -> None:
    """A config instance from another registry is an unknown identifier."""
    plugin, _ = _build_unregistered_plugin(bind_key="primary")
    foreign = AiosqliteConfig(connection_config={"database": ":memory:"}, bind_key="foreign")
    with pytest.raises(KeyError, match="Available keys"):
        plugin.get_config(foreign)


def test_get_config_unknown_string_after_registration_raises_key_error() -> None:
    """The unknown-identifier contract holds in both lifecycle phases."""
    plugin, _ = _build_unregistered_plugin(bind_key="primary")
    plugin.on_app_init(AppConfig())
    with pytest.raises(KeyError, match="Available keys"):
        plugin.get_config("missing")
