"""Regression tests for Litestar missing-connection control flow."""

from typing import NoReturn

from litestar.config.app import AppConfig

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin


def _build_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


def test_raise_missing_connection_always_raises() -> None:
    plugin = _build_plugin()

    try:
        plugin._raise_missing_connection("db_connection")  # pyright: ignore[reportPrivateUsage]
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:  # pragma: no cover - defensive assertion
        msg = "_raise_missing_connection should not return"
        raise AssertionError(msg)


def test_get_plugin_state_raises_for_unknown_key() -> None:
    plugin = _build_plugin()
    plugin.on_app_init(AppConfig())

    try:
        plugin._get_plugin_state("unknown")  # pyright: ignore[reportPrivateUsage]
    except KeyError as exc:
        assert "unknown" in str(exc)
    else:  # pragma: no cover - defensive assertion
        msg = "_get_plugin_state should raise for unknown keys"
        raise AssertionError(msg)


def test_provide_request_connection_raises_when_connection_missing() -> None:
    plugin = _build_plugin()
    state = AppConfig().state
    scope = {"type": "http"}

    try:
        plugin.provide_request_connection("db_connection", state, scope)  # type: ignore[arg-type]
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:  # pragma: no cover - defensive assertion
        msg = "provide_request_connection should raise when no connection is scoped"
        raise AssertionError(msg)


def test_provide_request_session_raises_when_connection_missing() -> None:
    plugin = _build_plugin()
    state = AppConfig().state
    scope = {"type": "http"}

    try:
        plugin.provide_request_session("db_connection", state, scope)  # type: ignore[arg-type]
    except ImproperConfigurationError as exc:
        assert "db_connection" in str(exc)
    else:  # pragma: no cover - defensive assertion
        msg = "provide_request_session should raise when no connection is scoped"
        raise AssertionError(msg)


def test_raise_missing_connection_annotation_is_noreturn() -> None:
    assert SQLSpecPlugin._raise_missing_connection.__annotations__["return"] is NoReturn
