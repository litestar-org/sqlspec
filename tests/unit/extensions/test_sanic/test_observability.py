"""Tests for Sanic correlation and SQLCommenter middleware."""

from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext


async def test_correlation_context_is_set_and_restored() -> None:
    """Correlation middleware should use request headers and restore prior context."""
    CorrelationContext.set("outer-context")
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))
    request = _make_request(headers={"x-request-id": "request-123"})
    response = SimpleNamespace(status=500, headers={})

    try:
        await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

        assert CorrelationContext.get() == "request-123"
        assert request.ctx.correlation_id == "request-123"

        await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]

        assert response.headers["X-Correlation-ID"] == "request-123"
        assert CorrelationContext.get() == "outer-context"
    finally:
        CorrelationContext.clear()


async def test_sqlcommenter_context_is_set_with_sanic_framework_and_restored() -> None:
    """SQLCommenter middleware should set Sanic request attributes."""
    previous = {"route": "/outer"}
    SQLCommenterContext.set(previous)
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_sqlcommenter=True)}))
    request = _make_request(path="/items/1", uri_template="/items/<item_id:int>", endpoint="App.get_item")
    response = SimpleNamespace(status=200, headers={})

    try:
        await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]

        assert SQLCommenterContext.get() == {
            "framework": "sanic",
            "route": "/items/<item_id:int>",
            "action": "get_item",
        }

        await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]

        assert SQLCommenterContext.get() is previous
    finally:
        SQLCommenterContext.set(None)


def test_observability_registers_middleware_when_di_disabled() -> None:
    """disable_di should not suppress enabled observability middleware."""
    app = Sanic(f"SQLSpecObservability{uuid4().hex}")
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))

    plugin.init_app(app)

    assert len(app.request_middleware) == 1
    assert len(app.response_middleware) == 1


def _make_config(*, enable_correlation: bool = False, enable_sqlcommenter: bool = False) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = True
    config.extension_config = {
        "sanic": {
            "disable_di": True,
            "enable_correlation_middleware": enable_correlation,
            "enable_sqlcommenter_middleware": enable_sqlcommenter,
        }
    }
    config.statement_config = SimpleNamespace(enable_sqlcommenter=enable_sqlcommenter)
    return config


def _make_request(
    *,
    headers: dict[str, str] | None = None,
    path: str = "/test",
    uri_template: str | None = None,
    endpoint: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace()),
        ctx=SimpleNamespace(),
        endpoint=endpoint,
        headers=headers or {},
        path=path,
        uri_template=uri_template,
    )
