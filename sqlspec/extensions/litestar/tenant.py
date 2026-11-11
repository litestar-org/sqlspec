"""Resolve tenants from verified Litestar authentication and scope receipts."""

import inspect
from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from sqlspec.extensions.litestar._tenant_resolution import (
    TenantResolutionSettings,
    resolve_tenant_from_auth,
    scope_value,
)
from sqlspec.extensions.litestar._utils import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.utils.tenant import TenantContextPayload, coerce_tenant_payload

if TYPE_CHECKING:
    from collections.abc import Awaitable, Generator

    from litestar.connection import ASGIConnection
    from litestar.handlers import BaseRouteHandler
    from litestar.types import Guard, Scope

__all__ = ("TenantResolutionSettings", "require_tenant", "resolve_litestar_tenant", "set_connection_tenant")

_TENANT_SCOPE_KEY = "_sqlspec_tenant_context"


def set_connection_tenant(
    connection_or_scope: Any,
    tenant_id: str | UUID | TenantContextPayload,
    *,
    variables: Mapping[str, str] | None = None,
) -> TenantContextPayload:
    """Store a tenant receipt after trusted application code authorizes access.

    Args:
        connection_or_scope: A Litestar connection or its raw ASGI scope.
        tenant_id: The authorized tenant identity or an existing payload.
        variables: Extra session variables merged with an existing payload.

    Returns:
        The immutable payload stored on the connection scope.
    """
    scope = connection_or_scope if isinstance(connection_or_scope, MutableMapping) else connection_or_scope.scope
    payload = tenant_id if isinstance(tenant_id, TenantContextPayload) else TenantContextPayload(tenant_id)
    if variables is not None:
        payload = TenantContextPayload(
            payload.tenant_id, variables={**payload.variables, **variables}, is_bypass=payload.is_bypass
        )
    set_sqlspec_scope_state(cast("Scope", scope), _TENANT_SCOPE_KEY, payload)
    return payload


def resolve_litestar_tenant(
    scope: MutableMapping[str, Any], settings: TenantResolutionSettings
) -> "Awaitable[TenantContextPayload | None] | TenantContextPayload | None":
    """Resolve an authorized tenant without changing the ambient context.

    Route bypass and explicit trusted receipts precede an application extractor.
    Automatic selection checks path, session and optional header values against
    authenticated memberships; route guard declarations only name selectors.
    """
    route = scope.get("route_handler")
    if scope_value(getattr(route, "opt", None), "tenant_bypass") is True:
        return TenantContextPayload.bypass()
    receipt = get_sqlspec_scope_state(cast("Scope", scope), _TENANT_SCOPE_KEY)
    if receipt is None:
        receipt = scope_value(scope.get("state"), "tenant_context")
    if receipt is not None:
        return coerce_tenant_payload(receipt)
    if settings.tenant_extractor is not None:
        result = settings.tenant_extractor(scope)
        if inspect.isawaitable(result):
            return _TenantPayloadAwaitable(result)
        return coerce_tenant_payload(result)
    return resolve_tenant_from_auth(scope, settings)


class _TenantPayloadAwaitable:
    """Normalize asynchronous extraction while allowing unused coroutines to close."""

    __slots__ = ("_result",)

    def __init__(self, result: "Awaitable[Any]") -> None:
        self._result = result

    def __await__(self) -> "Generator[Any, None, TenantContextPayload | None]":
        return self._resolve().__await__()

    async def _resolve(self) -> TenantContextPayload | None:
        return coerce_tenant_payload(await self._result)

    def close(self) -> None:
        close = getattr(self._result, "close", None)
        if callable(close):
            close()


def require_tenant(
    *,
    path_parameter: str | None = None,
    header: str | None = None,
    session_key: str | None = "tenant_id",
    user_attr: str = "tenant_id",
    memberships_attr: str = "tenant_ids",
    variables_attr: str | None = "tenant_variables",
) -> "Guard":
    """Require authentication and an authorized tenant before route execution.

    Path, session and optional header values select only verified memberships.
    A successful guard stores a receipt consumed by later database acquisition.

    Args:
        path_parameter: Optional route parameter containing a tenant selector.
        header: Optional request header containing a tenant selector.
        session_key: Session key containing a tenant selector.
        user_attr: Verified direct tenant attribute or mapping key.
        memberships_attr: Verified membership attribute or mapping key.
        variables_attr: Extra tenant session variables attribute or mapping key.

    Returns:
        An asynchronous Litestar authorization guard.
    """
    settings = TenantResolutionSettings(
        path_parameter=path_parameter,
        header=header,
        session_key=session_key,
        user_attr=user_attr,
        memberships_attr=memberships_attr,
        variables_attr=variables_attr,
    )

    async def guard(connection: "ASGIConnection[Any, Any, Any, Any]", route_handler: "BaseRouteHandler") -> None:
        from litestar.exceptions import NotAuthorizedException, PermissionDeniedException, WebSocketException

        scope = cast("MutableMapping[str, Any]", connection.scope)
        user = scope.get("user")
        authenticated = (user is not None or scope.get("auth") is not None) and bool(
            getattr(user, "is_authenticated", True)
        )
        if not authenticated:
            if scope.get("type") == "websocket":
                raise WebSocketException(code=4001, detail="Authentication required")
            raise NotAuthorizedException(detail="Authentication required")
        scope.setdefault("route_handler", route_handler)
        payload = resolve_litestar_tenant(scope, settings)
        if inspect.isawaitable(payload):
            payload = await payload
        if payload is None:
            if scope.get("type") == "websocket":
                raise WebSocketException(code=4003, detail="An authorized tenant is required")
            raise PermissionDeniedException(detail="An authorized tenant is required")
        set_connection_tenant(connection, payload)

    return guard
