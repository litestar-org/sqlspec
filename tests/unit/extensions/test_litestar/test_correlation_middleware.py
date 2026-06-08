"""Tests for Litestar correlation middleware behavior."""

from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

from sqlspec.extensions.litestar.plugin import CORRELATION_STATE_KEY, CorrelationMiddleware, get_sqlspec_scope_state
from sqlspec.utils.correlation import CorrelationContext

pytestmark = pytest.mark.anyio


async def _noop_receive() -> dict[str, str]:
    return {"type": "http.request"}


async def _noop_send(_message: Any) -> None:
    return None


async def _call_middleware(
    scope: dict[str, Any],
    app: Callable[[Any, Any, Any], Awaitable[None]],
    *,
    headers: tuple[str, ...] = ("x-request-id",),
) -> None:
    middleware = CorrelationMiddleware(app, headers=headers)
    await middleware(cast("Any", scope), cast("Any", _noop_receive), cast("Any", _noop_send))


async def test_litestar_correlation_middleware_restores_previous_correlation_id() -> None:
    CorrelationContext.set("outer")
    seen: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()

    scope = {"type": "http", "headers": [(b"x-request-id", b"inner")]}

    try:
        await _call_middleware(scope, app)
        assert seen["cid"] == "inner"
        assert CorrelationContext.get() == "outer"
    finally:
        CorrelationContext.clear()


def test_correlation_middleware_slots_include_cached_extractor() -> None:
    assert CorrelationMiddleware.__slots__ == ("_app", "_extractor", "_headers")


async def test_litestar_correlation_middleware_sanitizes_header_value() -> None:
    seen: dict[str, Any] = {}
    raw_value = f"  {'x' * 200}  "

    async def app(scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()
        seen["scope_cid"] = get_sqlspec_scope_state(scope, CORRELATION_STATE_KEY)

    scope = {"type": "http", "headers": [(b"x-request-id", raw_value.encode())]}

    await _call_middleware(scope, app)

    assert seen["cid"] == "x" * 128
    assert seen["scope_cid"] == "x" * 128
    assert get_sqlspec_scope_state(cast("Any", scope), CORRELATION_STATE_KEY) is None


async def test_litestar_correlation_middleware_uses_additional_header() -> None:
    seen: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()

    scope = {"type": "http", "headers": [(b"x-correlation-id", b"secondary")]}

    await _call_middleware(scope, app, headers=("x-request-id", "x-correlation-id"))

    assert seen["cid"] == "secondary"


async def test_litestar_correlation_middleware_generates_id_when_headers_missing() -> None:
    seen: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()

    scope = {"type": "http", "headers": []}

    await _call_middleware(scope, app)

    assert isinstance(seen["cid"], str)
    assert seen["cid"]


async def test_litestar_correlation_middleware_skips_non_http_scope() -> None:
    CorrelationContext.set("outer")
    seen: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()

    try:
        await _call_middleware({"type": "websocket", "headers": [(b"x-request-id", b"inner")]}, app)
        assert seen["cid"] == "outer"
        assert CorrelationContext.get() == "outer"
    finally:
        CorrelationContext.clear()
