"""Tests for Sanic correlation and SQLCommenter middleware."""

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("sanic")

from sanic import Sanic

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.observability import ObservabilityConfig
from sqlspec.utils.correlation import CorrelationContext

if TYPE_CHECKING:
    from sqlspec.config import LifecycleConfig

pytestmark = pytest.mark.anyio


async def test_correlation_context_is_set_and_restored() -> None:
    """Correlation middleware should use request headers and restore prior context."""
    CorrelationContext.set("outer-context")
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": _make_config(enable_correlation=True)}))
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
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": _make_config(enable_sqlcommenter=True)}))
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
    plugin = SQLSpecPlugin(MagicMock(spec=SQLSpec, configs={"default": _make_config(enable_correlation=True)}))

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


async def test_sanic_session_prepares_driver_emits_lifecycle_events_and_detaches() -> None:
    """Sanic prepares request sessions via _prepare_driver, emits session start and end, and detaches on release."""
    events: list[tuple[str, Any]] = []
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"sanic": {"commit_mode": "manual"}},
        observability_config=ObservabilityConfig(
            lifecycle=cast(
                "LifecycleConfig",
                {
                    "on_session_start": [lambda ctx: events.append(("start", ctx["session"]))],
                    "on_session_end": [lambda ctx: events.append(("end", ctx["session"]))],
                },
            )
        ),
    )
    prepared: list[Any] = []
    original_prepare = config._prepare_driver

    def _tracking_prepare(driver: Any) -> Any:
        prepared.append(driver)
        return original_prepare(driver)

    config._prepare_driver = _tracking_prepare  # type: ignore[method-assign]

    class _ConnCM:
        def __init__(self, conn: Any) -> None:
            self._conn = conn

        async def __aenter__(self) -> Any:
            return self._conn

        async def __aexit__(self, *_args: object) -> None:
            return None

    conn = AsyncMock()
    config.provide_connection = MagicMock(return_value=_ConnCM(conn))  # type: ignore[method-assign]
    plugin = SQLSpecPlugin(config)
    request = SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())),
        conn_info=SimpleNamespace(ctx=SimpleNamespace()),
        ctx=SimpleNamespace(),
        headers={},
        path="/items",
    )
    response = SimpleNamespace(status=200, headers={})

    await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
    session = await plugin.get_session_async(request)
    assert len(prepared) == 1
    assert events == [("start", session)]

    await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
    assert events == [("start", session)]
    assert session.connection is conn

    await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]
    assert events == [("start", session), ("end", session)]
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = session.connection.cursor


@pytest.mark.parametrize("use_deferred", [False, True])
async def test_sanic_async_lifecycle_hooks_on_async_and_deferred_sessions(
    use_deferred: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Sanic awaits async on_session_start and on_session_end hooks on async and deferred sessions."""
    events: list[str] = []

    async def _on_start(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        events.append("start")

    async def _on_end(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        events.append("end")

    config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"sanic": {"commit_mode": "manual"}},
        observability_config=ObservabilityConfig(
            lifecycle=cast("LifecycleConfig", {"on_session_start": [_on_start], "on_session_end": [_on_end]})
        ),
    )

    class _ConnCM:
        def __init__(self, conn: Any) -> None:
            self._conn = conn

        async def __aenter__(self) -> Any:
            return self._conn

        async def __aexit__(self, *_args: object) -> None:
            return None

    conn = AsyncMock()
    config.provide_connection = MagicMock(return_value=_ConnCM(conn))  # type: ignore[method-assign]
    plugin = SQLSpecPlugin(config)
    request = SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace(db_pool=object())),
        conn_info=SimpleNamespace(ctx=SimpleNamespace()),
        ctx=SimpleNamespace(),
        headers={},
        path="/items",
    )
    response = SimpleNamespace(status=200, headers={})

    with caplog.at_level("WARNING"):
        await plugin._on_request(request)  # pyright: ignore[reportPrivateUsage]
        if use_deferred:
            deferred = plugin.get_session(request)
            await deferred.resolve()
        else:
            await plugin.get_session_async(request)
        assert events == ["start"]
        await plugin._on_response(request, response)  # pyright: ignore[reportPrivateUsage]
        await plugin._on_lifecycle_response(request=request, response=response)  # pyright: ignore[reportPrivateUsage]

    assert events == ["start", "end"]
    assert "Async lifecycle hook invoked on sync path" not in caplog.text
