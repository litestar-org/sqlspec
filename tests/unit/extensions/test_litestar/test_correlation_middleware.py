"""Tests for Litestar correlation middleware behavior."""

from typing import Any, cast

from sqlspec.extensions.litestar.plugin import CorrelationMiddleware
from sqlspec.utils.correlation import CorrelationContext


async def test_litestar_correlation_middleware_restores_previous_correlation_id() -> None:
    CorrelationContext.set("outer")
    seen: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        seen["cid"] = CorrelationContext.get()

    middleware = CorrelationMiddleware(app, headers=("x-request-id",))
    scope = {"type": "http", "headers": [(b"x-request-id", b"inner")]}

    async def receive() -> Any:
        return {"type": "http.request"}

    async def send(_message: Any) -> None:
        return None

    try:
        await middleware(cast("Any", scope), cast("Any", receive), cast("Any", send))
        assert seen["cid"] == "inner"
        assert CorrelationContext.get() == "outer"
    finally:
        CorrelationContext.clear()
