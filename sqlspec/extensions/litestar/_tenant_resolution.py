"""Pure tenant resolution over verified authentication values in an ASGI scope."""

from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Final, cast
from uuid import UUID
from weakref import WeakKeyDictionary

from sqlspec.utils.tenant._context import TenantContextPayload, is_tenant_bypass_route

__all__ = (
    "RouteTenantPlan",
    "TenantResolutionSettings",
    "is_scope_authenticated",
    "resolve_tenant_from_auth",
    "route_tenant_plan",
    "scope_value",
)

_MISSING: Final[Any] = object()
_EXTENSION_CONFIG_FIELDS: Final[dict[str, str]] = {
    "tenant_extractor": "tenant_extractor",
    "tenant_user_attr": "user_attr",
    "tenant_memberships_attr": "memberships_attr",
    "tenant_variables_attr": "variables_attr",
    "tenant_jwt_claim": "jwt_claim",
    "tenant_path_parameter": "path_parameter",
    "tenant_session_key": "session_key",
    "tenant_header": "header",
}


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

    @classmethod
    def from_extension_config(cls, config: Mapping[str, Any]) -> "TenantResolutionSettings":
        """Build settings from ``tenant_*`` extension keys, keeping defaults for absent keys."""
        return cls(**{field: config[key] for key, field in _EXTENSION_CONFIG_FIELDS.items() if key in config})


@dataclass(frozen=True, slots=True)
class RouteTenantPlan:
    """Per-route-handler resolution inputs that never change between requests."""

    bypass: bool
    guard_parameters: tuple[str, ...]


_NO_ROUTE_PLAN: Final = RouteTenantPlan(bypass=False, guard_parameters=())
_ROUTE_PLANS: "WeakKeyDictionary[Any, RouteTenantPlan]" = WeakKeyDictionary()


def route_tenant_plan(route_handler: Any) -> RouteTenantPlan:
    """Return the bypass marker and guard selector names of a route handler, computed once.

    Plans are cached per handler object and released with it.
    """
    if route_handler is None:
        return _NO_ROUTE_PLAN
    try:
        cached = _ROUTE_PLANS.get(route_handler)
    except TypeError:
        cached = None
    if cached is not None:
        return cached
    plan = RouteTenantPlan(
        bypass=scope_value(getattr(route_handler, "opt", None), "tenant_bypass") is True
        or is_tenant_bypass_route(route_handler),
        guard_parameters=tuple(_guard_parameters(route_handler)),
    )
    try:
        _ROUTE_PLANS[route_handler] = plan
    except TypeError:
        return plan
    return plan


def scope_value(source: Any, key: str) -> Any:
    """Read a mapping key or attribute, returning None when absent."""
    return source.get(key) if isinstance(source, Mapping) else getattr(source, key, None)


def is_scope_authenticated(scope: Mapping[str, Any]) -> bool:
    """Return whether an ASGI scope carries verified caller authentication."""
    raw_user = _present_scope_entry(scope, "user")
    if raw_user is not None:
        return bool(getattr(raw_user, "is_authenticated", True))
    auth = _present_scope_entry(scope, "auth")
    if auth is None:
        return False
    if hasattr(auth, "evidence") and hasattr(auth, "session") and hasattr(auth, "authorization"):
        return bool(getattr(auth, "evidence", ())) or _is_snapshot_populated(getattr(auth, "authorization", None))
    return bool(getattr(auth, "is_authenticated", True))


def resolve_tenant_from_auth(
    scope: MutableMapping[str, Any],
    settings: TenantResolutionSettings,
    *,
    guard_parameters: Sequence[str] | None = None,
) -> TenantContextPayload | None:
    """Select a tenant the authenticated identity is authorized to use.

    A requested selector must name a verified membership after credential
    restrictions are applied. Without a selector the direct tenant applies when
    it remains authorized, then a sole membership.

    Args:
        scope: The ASGI connection scope.
        settings: Tenant resolution settings.
        guard_parameters: Path selector names declared by the route's guards; read from the
            route handler when omitted.

    Returns:
        The authorized payload, or None.
    """
    if not is_scope_authenticated(scope):
        return None
    raw_user = _present_scope_entry(scope, "user")
    users = _extract_users(raw_user)
    principal_claims = getattr(raw_user, "claims", None) if raw_user is not None else None
    principal_raw_claims = getattr(principal_claims, "raw", principal_claims)
    principal_extras = getattr(raw_user, "extras", None) if raw_user is not None else None

    auth = _present_scope_entry(scope, "auth")
    snapshot = getattr(auth, "authorization", None)
    attributes = getattr(snapshot, "attributes", None) or {}
    extras = getattr(auth, "extras", None) or {}
    auth_raw = getattr(auth, "raw", None) or {}

    allowed = _memberships(getattr(snapshot, "tenant_ids", None))
    allowed.update(_tenant_role_memberships(getattr(snapshot, "tenant_roles", None)))
    for source in (*users, attributes, principal_extras, principal_raw_claims, extras, auth_raw):
        if source:
            allowed.update(_memberships(scope_value(source, settings.memberships_attr)))

    direct: str | None = None
    for candidate in (
        *[scope_value(source, settings.user_attr) for source in users],
        scope_value(attributes, settings.user_attr),
        scope_value(principal_extras, settings.jwt_claim) if principal_extras else None,
        scope_value(principal_raw_claims, settings.jwt_claim) if principal_raw_claims else None,
        scope_value(extras, settings.jwt_claim),
        scope_value(auth_raw, settings.jwt_claim),
        scope_value(auth, settings.jwt_claim),
    ):
        direct = _identity(candidate)
        if direct is not None:
            break

    if direct is not None:
        if not allowed:
            allowed.add(direct)
        elif direct not in allowed:
            direct = None

    for restriction in getattr(auth, "restrictions", None) or ():
        restriction_tenants = getattr(restriction, "tenant_ids", None)
        if restriction_tenants is not None:
            allowed.intersection_update(_memberships(restriction_tenants))
    if direct is not None and direct not in allowed:
        direct = None

    requested, value = _requested_tenant(scope, settings, auth, guard_parameters)
    if requested and value not in allowed:
        return None
    selected = value or direct or (next(iter(allowed)) if len(allowed) == 1 else None)
    if selected is None:
        return None

    variables = None
    if settings.variables_attr is not None:
        for source in (*users, attributes, principal_extras, principal_raw_claims, extras, auth_raw):
            if not source:
                continue
            variables = scope_value(source, settings.variables_attr)
            if variables is not None:
                break
    return TenantContextPayload(selected, variables=variables)


def _present_scope_entry(scope: Mapping[str, Any], key: str) -> Any:
    value = scope.get(key)
    if value is None or type(value).__name__ in {"EmptyType", "_EmptyEnum"}:
        return None
    return value


def _is_snapshot_populated(snapshot: Any) -> bool:
    if snapshot is None:
        return False
    for field_name in ("roles", "permissions", "scopes", "capabilities", "resources", "tenant_ids", "attributes"):
        if getattr(snapshot, field_name, None):
            return True
    tenant_roles = getattr(snapshot, "tenant_roles", None)
    if isinstance(tenant_roles, Mapping):
        return any(bool(roles) for roles in tenant_roles.values())
    return False


def _extract_users(raw_user: Any) -> tuple[Any, ...]:
    if raw_user is None:
        return ()
    inner_user = None if isinstance(raw_user, Mapping) else getattr(raw_user, "user", _MISSING)
    if inner_user is _MISSING or inner_user is None or inner_user is raw_user:
        return (raw_user,)
    return (inner_user, raw_user)


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


def _tenant_role_memberships(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return {identity for key, roles in value.items() if roles and (identity := _identity(key)) is not None}
    return _memberships(value)


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
        wrapped = getattr(guard, "func", None)
        if callable(wrapped):
            pending.append(wrapped)
        name = getattr(guard, "tenant_parameter", None)
        if isinstance(name, str) and name not in parameters:
            parameters.append(name)
        for child_attr in ("children", "predicates"):
            children = getattr(guard, child_attr, ())
            if isinstance(children, (tuple, list)):
                pending.extend(children)
        nested = getattr(guard, "predicate", None)
        if nested is not None:
            pending.append(nested)
    return parameters


@lru_cache(maxsize=64)
def _header_key(header: str) -> bytes:
    return header.lower().encode("latin-1")


def _requested_tenant(
    scope: MutableMapping[str, Any],
    settings: TenantResolutionSettings,
    auth: Any,
    guard_parameters: Sequence[str] | None,
) -> tuple[bool, str | None]:
    """Return whether the request names a tenant selector, and the value it names.

    Path parameters precede the session key, which precedes the optional header.
    """
    parameters = _guard_parameters(scope.get("route_handler")) if guard_parameters is None else list(guard_parameters)
    if settings.path_parameter is not None and settings.path_parameter not in parameters:
        parameters.append(settings.path_parameter)
    path = scope.get("path_params") or {}
    for name in parameters:
        if name in path:
            return True, _identity(path[name])
    if settings.session_key is not None:
        session_found, session_value = _read_session_selector(scope, auth, settings.session_key)
        if session_found:
            return True, session_value
    if settings.header is not None:
        header = _header_key(settings.header)
        for header_name, header_value in scope.get("headers", ()):
            if header_name.lower() == header:
                return True, _identity(header_value.decode("latin-1"))
    return False, None


def _read_session_selector(scope: Mapping[str, Any], auth: Any, session_key: str) -> tuple[bool, str | None]:
    session = _present_scope_entry(scope, "session")
    if isinstance(session, Mapping) and session_key in session:
        return True, _identity(session[session_key])
    for handle in (session, getattr(auth, "session", None)):
        if handle is not None and getattr(handle, "is_available", False) is True:
            getter = getattr(handle, "get", None)
            if callable(getter):
                stored = getter(session_key, _MISSING)
                if stored is not _MISSING:
                    return True, _identity(stored)
    return False, None
