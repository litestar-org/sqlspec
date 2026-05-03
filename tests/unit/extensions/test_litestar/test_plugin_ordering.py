"""Regression tests for SQLSpecPlugin's interaction with downstream plugins.

Litestar walks ``app_config.plugins`` with a generator expression that captures
the list reference once. Plugins are allowed to register follow-on plugins by
mutating that list in place (``.append`` / ``.extend``). If any plugin rebinds
``app_config.plugins`` to a brand-new list, Litestar's iterator continues
walking the old list and any plugin appended afterwards is silently dropped.

These tests pin SQLSpecPlugin to the in-place mutation contract.
"""

from __future__ import annotations

import pytest
from litestar import Litestar
from litestar.config.app import AppConfig
from litestar.plugins import InitPluginProtocol

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin

pytestmark = pytest.mark.xdist_group("extensions_litestar")


def _build_sqlspec_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


class _FollowOnPlugin(InitPluginProtocol):
    """Marker plugin that records when its ``on_app_init`` fires."""

    def __init__(self, fired: list[str], name: str) -> None:
        self._fired = fired
        self._name = name

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        self._fired.append(self._name)
        return app_config


class _AppendsFollowOnPlugin(InitPluginProtocol):
    """Plugin that registers a follow-on plugin during its own ``on_app_init``.

    Mirrors the ``litestar-vite`` ``VitePlugin`` -> ``InertiaPlugin`` pattern
    that originally surfaced this bug.
    """

    def __init__(self, follow_on: InitPluginProtocol) -> None:
        self._follow_on = follow_on

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        app_config.plugins.append(self._follow_on)
        return app_config


def test_on_app_init_preserves_plugins_list_identity() -> None:
    """``app_config.plugins`` must be the same list object after ``on_app_init``.

    Rebinding to a new list breaks Litestar's mid-iteration plugin discovery —
    the iterator captured the old list reference and silently skips anything
    a later plugin appends to ``app_config.plugins``.
    """
    plugin = _build_sqlspec_plugin()
    app_config = AppConfig()
    original_plugins = app_config.plugins

    plugin.on_app_init(app_config)

    assert app_config.plugins is original_plugins, (
        "SQLSpecPlugin.on_app_init rebound app_config.plugins to a new list; "
        "this breaks downstream plugins that append to it during init."
    )


def test_follow_on_plugin_fires_when_sqlspec_registered_first() -> None:
    """[SQLSpec, Appender] order: appended follow-on plugin must still init."""
    fired: list[str] = []
    follow_on = _FollowOnPlugin(fired, "follow_on")
    appender = _AppendsFollowOnPlugin(follow_on)

    Litestar(plugins=[_build_sqlspec_plugin(), appender])

    assert "follow_on" in fired, (
        "Plugin appended to app_config.plugins during init was never called. "
        "SQLSpecPlugin must mutate app_config.plugins in place, not rebind it."
    )


def test_follow_on_plugin_fires_when_sqlspec_registered_second() -> None:
    """[Appender, SQLSpec] order: control case — must also fire."""
    fired: list[str] = []
    follow_on = _FollowOnPlugin(fired, "follow_on")
    appender = _AppendsFollowOnPlugin(follow_on)

    Litestar(plugins=[appender, _build_sqlspec_plugin()])

    assert "follow_on" in fired
