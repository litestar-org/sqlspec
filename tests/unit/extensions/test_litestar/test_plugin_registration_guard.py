"""Tests for PluginConfigState access before on_app_init registration."""

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.plugin import DEFAULT_SESSION_KEY, SQLSpecPlugin


def _build_unregistered_plugin() -> SQLSpecPlugin:
    """Build a plugin without running on_app_init."""
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


def test_get_annotations_before_registration_raises_clear_error() -> None:
    """get_annotations() before on_app_init raises ImproperConfigurationError, not AttributeError."""
    plugin = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_annotations()


def test_get_annotation_before_registration_raises_clear_error() -> None:
    """get_annotation() before on_app_init raises ImproperConfigurationError, not AttributeError."""
    plugin = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_annotation(DEFAULT_SESSION_KEY)


def test_get_config_by_type_before_registration_raises_clear_error() -> None:
    """get_config(config_type) before on_app_init raises ImproperConfigurationError, not AttributeError."""
    plugin = _build_unregistered_plugin()
    with pytest.raises(ImproperConfigurationError, match="on_app_init"):
        plugin.get_config(AiosqliteConfig)
