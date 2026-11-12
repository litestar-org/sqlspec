"""Run test code inside the Litestar plugin's tenant request binding."""

from collections.abc import Awaitable, Callable
from typing import Any

from sqlspec.extensions.litestar.plugin import TenantRequestMiddleware

__all__ = ("within_tenant_request",)


async def within_tenant_request(targets: Any, scope: Any, body: Callable[[], Awaitable[Any]]) -> Any:
    """Await ``body`` while the request binding for ``scope`` is active and return its result."""
    result: dict[str, Any] = {}

    async def app(_scope: Any, _receive: Any, _send: Any) -> None:
        result["value"] = await body()

    async def receive() -> Any:
        return {"type": "http.disconnect"}

    async def send(_message: Any) -> None:
        return None

    await TenantRequestMiddleware(app, targets=targets)(scope, receive, send)
    return result.get("value")
