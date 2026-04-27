"""Tests for SQLSpecPlugin's serialization wiring.

The plugin merges :data:`sqlspec.utils.serializers.DEFAULT_TYPE_ENCODERS`
into :class:`AppConfig` once at app-init, then relies on Litestar's own
per-handler ``resolve_type_encoders()`` machinery to merge user-supplied
route/controller/router-level encoders on top — no bidirectional thread
into sqlspec's serializer is needed.
"""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from litestar.config.app import AppConfig

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin
from sqlspec.utils.serializers import DEFAULT_TYPE_ENCODERS

pytestmark = pytest.mark.xdist_group("extensions_litestar")


def _build_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


def test_plugin_registers_default_encoders_into_app_config() -> None:
    plugin = _build_plugin()
    app_config = AppConfig()

    plugin.on_app_init(app_config)

    assert app_config.type_encoders is not None
    for key in DEFAULT_TYPE_ENCODERS:
        assert key in app_config.type_encoders


def test_user_encoder_overrides_sqlspec_default() -> None:
    """User encoder for an existing default key wins on conflict."""

    def user_dt(_v: datetime.datetime) -> str:
        return "USER"

    plugin = _build_plugin()
    app_config = AppConfig(type_encoders={datetime.datetime: user_dt})

    plugin.on_app_init(app_config)

    assert app_config.type_encoders is not None
    assert app_config.type_encoders[datetime.datetime] is user_dt


def test_user_decoders_take_precedence_in_list_order() -> None:
    """User-supplied decoders are placed before SQLSpec's, so Litestar resolves them first."""

    def user_predicate(_t: Any) -> bool:
        return False

    def user_decoder(_t: type, _v: Any) -> Any:
        return None

    plugin = _build_plugin()
    app_config = AppConfig(type_decoders=[(user_predicate, user_decoder)])

    plugin.on_app_init(app_config)

    assert app_config.type_decoders is not None
    assert app_config.type_decoders[0] == (user_predicate, user_decoder)


def test_plugin_does_not_set_before_or_after_response_hooks() -> None:
    """Lifecycle hooks are not used: route-level encoder resolution is Litestar's own concern."""
    plugin = _build_plugin()
    app_config = AppConfig()

    plugin.on_app_init(app_config)

    assert app_config.before_request is None
    assert app_config.after_response is None
