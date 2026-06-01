"""Tests for SQLSpecPlugin Litestar middleware assembly."""

from litestar.config.app import AppConfig
from litestar.middleware import DefineMiddleware

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core import StatementConfig
from sqlspec.extensions.litestar.plugin import CorrelationMiddleware, SQLCommenterMiddleware, SQLSpecPlugin


class _ExistingMiddleware:
    pass


def _build_plugin(*, correlation: bool = False, sqlcommenter: bool = False) -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            statement_config=StatementConfig(enable_sqlcommenter=sqlcommenter),
            extension_config={
                "litestar": {
                    "enable_correlation_middleware": correlation,
                    "enable_sqlcommenter_middleware": sqlcommenter,
                }
            },
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec)


def _middleware_types(app_config: AppConfig) -> list[type[object]]:
    return [middleware.middleware for middleware in app_config.middleware or []]


def test_on_app_init_appends_both_middlewares_when_enabled() -> None:
    app_config = AppConfig()

    _build_plugin(correlation=True, sqlcommenter=True).on_app_init(app_config)

    assert _middleware_types(app_config)[-2:] == [CorrelationMiddleware, SQLCommenterMiddleware]


def test_on_app_init_appends_only_correlation_middleware() -> None:
    app_config = AppConfig()

    _build_plugin(correlation=True, sqlcommenter=False).on_app_init(app_config)

    assert _middleware_types(app_config) == [CorrelationMiddleware]


def test_on_app_init_appends_only_sqlcommenter_middleware() -> None:
    app_config = AppConfig()

    _build_plugin(correlation=False, sqlcommenter=True).on_app_init(app_config)

    assert _middleware_types(app_config) == [SQLCommenterMiddleware]


def test_on_app_init_appends_no_observability_middlewares_when_disabled() -> None:
    app_config = AppConfig()

    _build_plugin(correlation=False, sqlcommenter=False).on_app_init(app_config)

    assert app_config.middleware == []


def test_on_app_init_preserves_existing_middlewares() -> None:
    existing = DefineMiddleware(_ExistingMiddleware)
    app_config = AppConfig(middleware=[existing])

    _build_plugin(correlation=True, sqlcommenter=True).on_app_init(app_config)

    assert app_config.middleware[0] is existing
    assert _middleware_types(app_config)[1:] == [CorrelationMiddleware, SQLCommenterMiddleware]
