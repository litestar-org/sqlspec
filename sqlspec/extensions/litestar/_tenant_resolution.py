"""Pure tenant resolution over verified authentication values in an ASGI scope."""

from collections.abc import Callable, Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from typing import Any, Final, cast
from uuid import UUID

from sqlspec.utils.tenant._context import TenantContextPayload

__all__ = ("TenantResolutionSettings", "resolve_tenant_from_auth", "scope_value")

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


def scope_value(source: Any, key: str) -> Any:
    """Read a mapping key or attribute, returning None when absent."""
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
        wrapped = getattr(guard, "func", None)
        if callable(wrapped):
            pending.append(wrapped)
        name = getattr(guard, "tenant_parameter", None)
        if isinstance(name, str) and name not in parameters:
            parameters.append(name)
        children = getattr(guard, "children", ())
        if isinstance(children, (tuple, list)):
            pending.extend(children)
    return parameters


def _requested_tenant(scope: MutableMapping[str, Any], settings: TenantResolutionSettings) -> tuple[bool, str | None]:
    """Return whether the request names a tenant selector, and the value it names.

    Path parameters precede the session key, which precedes the optional header.
    """
    parameters = _guard_parameters(scope.get("route_handler"))
    if settings.path_parameter is not None and settings.path_parameter not in parameters:
        parameters.append(settings.path_parameter)
    path = scope.get("path_params") or {}
    for name in parameters:
        if name in path:
            return True, _identity(path[name])
    session = scope.get("session")
    if settings.session_key is not None and isinstance(session, Mapping) and settings.session_key in session:
        return True, _identity(session[settings.session_key])
    if settings.header is not None:
        header = settings.header.lower().encode("latin-1")
        for header_name, header_value in scope.get("headers", ()):
            if header_name.lower() == header:
                return True, header_value.decode("latin-1")
    return False, None


def resolve_tenant_from_auth(
    scope: MutableMapping[str, Any], settings: TenantResolutionSettings
) -> TenantContextPayload | None:
    """Select a tenant the authenticated identity is authorized to use.

    A requested selector must name a verified membership. Without a selector the
    direct tenant applies, then a sole membership.
    """
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
        allowed.update(_memberships(scope_value(source, settings.memberships_attr)))
    direct: str | None = None
    for candidate in (
        *[scope_value(source, settings.user_attr) for source in users],
        scope_value(attributes, settings.user_attr),
        scope_value(extras, settings.jwt_claim),
        scope_value(auth, settings.jwt_claim),
    ):
        direct = _identity(candidate)
        if direct is not None:
            allowed.add(direct)
            break
    requested, value = _requested_tenant(scope, settings)
    if requested and value not in allowed:
        return None
    selected = value or direct or (next(iter(allowed)) if len(allowed) == 1 else None)
    if selected is None:
        return None
    variables = None
    if settings.variables_attr is not None:
        for source in (*users, attributes):
            variables = scope_value(source, settings.variables_attr)
            if variables is not None:
                break
    return TenantContextPayload(selected, variables=variables)
