"""Resolve tenants from verified Litestar authentication and scope receipts."""

import inspect
from collections.abc import Mapping, MutableMapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Final, NoReturn, cast
from uuid import UUID

from litestar.exceptions import NotAuthorizedException, PermissionDeniedException, WebSocketException

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._tenant_resolution import (
    TenantResolutionSettings,
    is_scope_authenticated,
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

__all__ = (
    "TenantResolutionSettings",
    "is_scope_authenticated",
    "raise_missing_tenant_http_exception",
    "require_tenant",
    "resolve_litestar_tenant",
    "set_connection_tenant",
)

_TENANT_SCOPE_KEY: Final = "_sqlspec_tenant_context"
_UNSET: Final[Any] = object()


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


def raise_missing_tenant_http_exception(scope: Mapping[str, Any]) -> NoReturn:
    """Raise a 401/4001 or 403/4003 Litestar exception when tenant resolution yields None."""
    is_websocket = str(scope.get("type")) == "websocket"
    if not is_scope_authenticated(scope):
        if is_websocket:
            raise WebSocketException(code=4001, detail="Authentication required")
        raise NotAuthorizedException(detail="Authentication required")
    if is_websocket:
        raise WebSocketException(code=4003, detail="An authorized tenant is required")
    raise PermissionDeniedException(detail="An authorized tenant is required")


def require_tenant(
    *,
    path_parameter: str | None = _UNSET,
    header: str | None = _UNSET,
    session_key: str | None = _UNSET,
    user_attr: str = _UNSET,
    memberships_attr: str = _UNSET,
    variables_attr: str | None = _UNSET,
    jwt_claim: str = _UNSET,
    config_key: Any = None,
) -> "Guard":
    """Require authentication and an authorized tenant before route execution.

    Defaults inherit from the active ``SQLSpecPlugin`` configuration on
    ``app.state.sqlspec``, falling back to ``TenantResolutionSettings()`` when
    no plugin is registered. Explicit keyword arguments override inherited fields.

    Args:
        path_parameter: Optional route parameter containing a tenant selector.
        header: Optional request header containing a tenant selector.
        session_key: Session key containing a tenant selector.
        user_attr: Verified direct tenant attribute or mapping key.
        memberships_attr: Verified membership attribute or mapping key.
        variables_attr: Extra tenant session variables attribute or mapping key.
        jwt_claim: Verified token claim containing a direct tenant identity.
        config_key: Optional database config key, type, or instance used to
            select base settings when multiple tenant databases are registered.

    Returns:
        An asynchronous Litestar authorization guard.
    """
    overrides: dict[str, Any] = {}
    if path_parameter is not _UNSET:
        overrides["path_parameter"] = path_parameter
    if header is not _UNSET:
        overrides["header"] = header
    if session_key is not _UNSET:
        overrides["session_key"] = session_key
    if user_attr is not _UNSET:
        overrides["user_attr"] = user_attr
    if memberships_attr is not _UNSET:
        overrides["memberships_attr"] = memberships_attr
    if variables_attr is not _UNSET:
        overrides["variables_attr"] = variables_attr
    if jwt_claim is not _UNSET:
        overrides["jwt_claim"] = jwt_claim

    async def guard(connection: "ASGIConnection[Any, Any, Any, Any]", route_handler: "BaseRouteHandler") -> None:
        if scope_value(getattr(route_handler, "opt", None), "tenant_bypass") is True:
            set_connection_tenant(connection, TenantContextPayload.bypass())
            return
        scope = cast("MutableMapping[str, Any]", connection.scope)
        scope.setdefault("route_handler", route_handler)
        settings = _resolve_guard_settings(connection, overrides, config_key)
        payload = resolve_litestar_tenant(scope, settings)
        if inspect.isawaitable(payload):
            payload = await payload
        if payload is None:
            raise_missing_tenant_http_exception(scope)
        set_connection_tenant(connection, payload)

    if isinstance(path_parameter, str):
        guard.tenant_parameter = path_parameter  # type: ignore[attr-defined]
    return guard


class _TenantPayloadAwaitable:
    """Normalize asynchronous extraction while allowing unused coroutines to close."""

    __slots__ = ("_result",)

    def __init__(self, result: "Awaitable[Any]") -> None:
        self._result = result

    def __await__(self) -> "Generator[Any, None, TenantContextPayload | None]":
        return self._resolve().__await__()

    def close(self) -> None:
        close = getattr(self._result, "close", None)
        if callable(close):
            close()

    async def _resolve(self) -> TenantContextPayload | None:
        return coerce_tenant_payload(await self._result)


def _resolve_guard_settings(
    connection: "ASGIConnection[Any, Any, Any, Any]", overrides: dict[str, Any], config_key: Any
) -> TenantResolutionSettings:
    base = _plugin_tenant_settings(connection, config_key)
    return replace(base, **overrides) if overrides else base


def _plugin_tenant_settings(
    connection: "ASGIConnection[Any, Any, Any, Any]", config_key: Any
) -> TenantResolutionSettings:
    app = getattr(connection, "app", None) or connection.scope.get("app")
    state = getattr(app, "state", None) if app is not None else None
    if state is None:
        return TenantResolutionSettings()
    plugins: tuple[Any, ...] = getattr(state, "_sqlspec_plugins", ())
    if not plugins:
        single = getattr(state, "sqlspec", None)
        plugins = (single,) if single is not None else ()
    if not plugins:
        return TenantResolutionSettings()
    if config_key is not None:
        for plugin in plugins:
            resolver = getattr(plugin, "resolve_tenant_settings", None)
            if not callable(resolver):
                continue
            try:
                return cast("TenantResolutionSettings", resolver(config_key))
            except KeyError:
                continue
        raise KeyError(config_key)
    if len(plugins) == 1:
        resolver = getattr(plugins[0], "resolve_tenant_settings", None)
        if callable(resolver):
            return cast("TenantResolutionSettings", resolver(None))
        return TenantResolutionSettings()

    tenant_settings: list[TenantResolutionSettings] = []
    fallback_settings: list[TenantResolutionSettings] = []
    for plugin in plugins:
        for cfg_state in getattr(plugin, "_plugin_configs", ()):
            cfg = getattr(cfg_state, "config", None)
            features = getattr(cfg, "driver_features", None)
            settings = getattr(cfg_state, "tenant_settings", None)
            if not isinstance(settings, TenantResolutionSettings):
                continue
            if isinstance(features, Mapping) and features.get("tenant_strategy") not in (None, False):
                tenant_settings.append(settings)
            else:
                fallback_settings.append(settings)
    candidates = tenant_settings or fallback_settings
    if not candidates:
        return TenantResolutionSettings()
    first = candidates[0]
    if tenant_settings and any(item != first for item in tenant_settings[1:]):
        msg = (
            "Multiple tenant-scoped Litestar configurations have distinct tenant resolution settings; "
            "pass config_key=... to require_tenant()."
        )
        raise ImproperConfigurationError(msg)
    return first
