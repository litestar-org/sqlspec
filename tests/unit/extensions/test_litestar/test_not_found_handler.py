"""Tests for the NotFoundError -> HTTP 404 default handler in the Litestar plugin."""

from typing import Any

from litestar import get
from litestar.config.app import AppConfig
from litestar.exceptions import NotFoundException
from litestar.testing import create_test_client

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import NotFoundError
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin, not_found_error_handler


def _build_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


def test_default_handler_registered_for_not_found_error() -> None:
    """Plugin should register NotFoundError -> not_found_error_handler by default."""
    plugin = _build_plugin()
    app_config = AppConfig()

    plugin.on_app_init(app_config)

    handlers = app_config.exception_handlers or {}
    assert handlers.get(NotFoundError) is not_found_error_handler


def test_user_handler_takes_precedence() -> None:
    """A user-supplied NotFoundError handler must not be overwritten."""

    def user_handler(_request: Any, exc: NotFoundError) -> Any:
        raise NotFoundException(detail=f"custom: {exc}") from exc

    plugin = _build_plugin()
    app_config = AppConfig(exception_handlers={NotFoundError: user_handler})

    plugin.on_app_init(app_config)

    handlers = app_config.exception_handlers or {}
    assert handlers[NotFoundError] is user_handler


def test_handler_translates_to_404_in_real_app() -> None:
    """End-to-end: a handler raising NotFoundError yields a 404 response."""

    @get("/missing")
    async def raise_missing() -> None:
        raise NotFoundError("nothing here")

    plugin = _build_plugin()

    with create_test_client(route_handlers=[raise_missing], plugins=[plugin]) as client:
        response = client.get("/missing")
        assert response.status_code == 404
        assert "nothing here" in response.text
