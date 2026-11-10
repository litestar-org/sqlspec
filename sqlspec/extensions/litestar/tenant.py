"""Resolve tenants from verified Litestar authentication and scope receipts."""

import inspect
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from sqlspec.extensions.litestar._utils import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.utils.tenant import TenantContextPayload, coerce_tenant_payload

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from litestar.connection import ASGIConnection
    from litestar.handlers import BaseRouteHandler
    from litestar.types import Guard, Scope

__all__ = ("TenantResolutionSettings", "require_tenant", "resolve_litestar_tenant", "set_connection_tenant")

_TENANT_SCOPE_KEY = "_sqlspec_tenant_context"


@dataclass(frozen=True, slots=True)
class TenantResolutionSettings:
    """Names used to resolve verified identities and authorize tenant selectors."""

    tenant_extractor: Callable[[MutableMapping[str, Any]], Any] | None = None
    user_attr: str = "tenant_id"
    memberships_attr: str = "tenant_ids"
    variables_attr: str | None = "tenant_variables"
    jwt_claim: str = "tenant_id"
    path_parameter: str | None = "tenant_id"
    session_key: str | None = "tenant_id"
    header: str | None = None


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


def _value(source: Any, key: str) -> Any:
    return source.get(key) if isinstance(source, Mapping) else getattr(source, key, None)


def _identity(value: Any) -> str | None:
    if isinstance(value, UUID):
        return str(value)
    return value if isinstance(value, str) and value else None


def _memberships(value: Any) -> set[str]:
    if isinstance(value, (str, UUID)):
        identity = _identity(value)
        return {identity} if identity is not None else set()
    if not isinstance(value, Iterable):
        return set()
    return {identity for item in value if (identity := _identity(item)) is not None}


def _guard_parameters(route_handler: Any) -> list[str]:
    resolve_guards = getattr(route_handler, "resolve_guards", None)
    pending = list(cast("Iterable[Any]", resolve_guards() or ())) if callable(resolve_guards) else []
    parameters: list[str] = []
    seen: set[int] = set()
    while pending:
        guard = pending.pop(0)
        if id(guard) in seen:
            continue
        seen.add(id(guard))
        name = getattr(guard, "tenant_parameter", None)
        if isinstance(name, str) and name not in parameters:
            parameters.append(name)
        children = getattr(guard, "children", ())
        if isinstance(children, (tuple, list)):
            pending.extend(children)
    return parameters


def _resolve_from_litestar_auth(
    scope: MutableMapping[str, Any], settings: TenantResolutionSettings
) -> TenantContextPayload | None:
    raw_user = scope.get("user")
    if not getattr(raw_user, "is_authenticated", True):
        return None
    user = getattr(raw_user, "user", raw_user)
    users = (user, raw_user) if user is not raw_user else (user,)
    auth = scope.get("auth")
    snapshot = getattr(auth, "authorization", None)
    attributes = getattr(snapshot, "attributes", None) or {}
    extras = getattr(auth, "extras", None) or {}
    allowed = _memberships(getattr(snapshot, "tenant_ids", None))
    allowed.update(_memberships(getattr(snapshot, "tenant_roles", None)))
    for source in (*users, extras):
        allowed.update(_memberships(_value(source, settings.memberships_attr)))
    direct = next(
        (
            identity
            for value in (
                *(_value(source, settings.user_attr) for source in users),
                _value(attributes, settings.user_attr),
                _value(extras, settings.jwt_claim),
                _value(auth, settings.jwt_claim),
            )
            if (identity := _identity(value)) is not None
        ),
        None,
    )
    if direct is not None:
        allowed.add(direct)
    parameters = _guard_parameters(scope.get("route_handler"))
    if settings.path_parameter is not None and settings.path_parameter not in parameters:
        parameters.append(settings.path_parameter)
    path = scope.get("path_params") or {}
    selected: str | None = None
    for name in parameters:
        if name in path:
            selected = _identity(path[name])
            if selected not in allowed:
                return None
            break
    if selected is None:
        session = scope.get("session")
        if settings.session_key is not None and isinstance(session, Mapping) and settings.session_key in session:
            selected = _identity(session[settings.session_key])
            if selected not in allowed:
                return None
        elif settings.header is not None:
            header = settings.header.lower().encode("latin-1")
            for name, value in scope.get("headers", ()):
                if name.lower() == header:
                    selected = value.decode("latin-1")
                    if selected not in allowed:
                        return None
                    break
    selected = selected or direct or (next(iter(allowed)) if len(allowed) == 1 else None)
    if selected is None:
        return None
    variables = None
    if settings.variables_attr is not None:
        variables = next(
            (
                value
                for source in (*users, attributes)
                if (value := _value(source, settings.variables_attr)) is not None
            ),
            None,
        )
    return TenantContextPayload(selected, variables=variables)


def resolve_litestar_tenant(
    scope: MutableMapping[str, Any], settings: TenantResolutionSettings
) -> "Awaitable[TenantContextPayload | None] | TenantContextPayload | None":
    """Resolve an authorized tenant without changing the ambient context.

    Route bypass and explicit trusted receipts precede an application extractor.
    Automatic selection checks path, session and optional header values against
    authenticated memberships; route guard declarations only name selectors.
    """
    route = scope.get("route_handler")
    if _value(getattr(route, "opt", None), "tenant_bypass") is True:
        return TenantContextPayload.bypass()
    receipt = get_sqlspec_scope_state(cast("Scope", scope), _TENANT_SCOPE_KEY)
    if receipt is None:
        receipt = _value(scope.get("state"), "tenant_context")
    if receipt is not None:
        return coerce_tenant_payload(receipt)
    if settings.tenant_extractor is not None:
        result = settings.tenant_extractor(scope)
        if inspect.isawaitable(result):

            async def resolve_result() -> TenantContextPayload | None:
                return coerce_tenant_payload(await result)

            return resolve_result()
        return coerce_tenant_payload(result)
    return _resolve_from_litestar_auth(scope, settings)


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
