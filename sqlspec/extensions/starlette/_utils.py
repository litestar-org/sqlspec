"""Request state helpers for the Starlette SQLSpec extension."""

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = (
    "get_connection_from_request",
    "get_or_create_session",
    "get_request_tenant",
    "get_state_value",
    "has_state_value",
    "pop_state_value",
    "set_request_tenant",
    "set_state_value",
)

_MISSING = object()
_TENANT_STATE_KEY: Final = "_sqlspec_tenant_context"


def get_state_value(state: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Starlette state object."""
    data = _state_dict(state)
    if default is _MISSING:
        try:
            return data[key]
        except KeyError as exc:
            msg = f"'{state.__class__.__name__}' object has no attribute '{key}'"
            raise AttributeError(msg) from exc
    return data.get(key, default)


def set_state_value(state: Any, key: str, value: Any) -> None:
    """Set a value on a Starlette state object."""
    _state_dict(state)[key] = value


def pop_state_value(state: Any, key: str) -> Any | None:
    """Remove a value from a Starlette state object."""
    return _state_dict(state).pop(key, None)


def has_state_value(state: Any, key: str) -> bool:
    """Check if a Starlette state object has a stored value."""
    return key in _state_dict(state)


def get_connection_from_request(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get database connection from request state.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.
    """
    return get_state_value(request.state, config_state.connection_key)


def get_or_create_session(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get or create database session for request.

    Sessions are cached per request to ensure the same session instance
    is returned for multiple calls within the same request.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session (driver instance).
    """
    session_instance_key = f"{config_state.session_key}_instance"

    existing_session = get_state_value(request.state, session_instance_key, None)
    if existing_session is not None:
        return existing_session
    session = config_state.config.driver_type(
        connection=get_connection_from_request(request, config_state),
        statement_config=config_state.config.statement_config,
        driver_features=config_state.config.driver_features,
    )
    set_state_value(request.state, session_instance_key, session)
    return session


def set_request_tenant(
    request_or_scope: Any,
    tenant_id: str | UUID | TenantContextPayload,
    *,
    variables: Mapping[str, str] | None = None,
    key: str | None = None,
) -> TenantContextPayload:
    """Store an authorized tenant receipt on a Starlette or FastAPI request.

    Args:
        request_or_scope: A Starlette/FastAPI ``Request`` or raw ASGI scope.
        tenant_id: Verified tenant identifier or immutable payload.
        variables: Extra session variables merged into the stored payload.
        key: Optional configuration ``session_key``, ``connection_key``, or
            ``bind_key`` scoping the receipt to one database configuration.

    Returns:
        The immutable ``TenantContextPayload`` stored on the request state.
    """
    payload = tenant_id if isinstance(tenant_id, TenantContextPayload) else TenantContextPayload(tenant_id)
    if variables is not None:
        payload = TenantContextPayload(
            payload.tenant_id, variables={**payload.variables, **variables}, is_bypass=payload.is_bypass
        )
    target_key = _tenant_state_key(key)
    if isinstance(request_or_scope, MutableMapping):
        state_map = request_or_scope.setdefault("state", {})
        if isinstance(state_map, MutableMapping):
            state_map[target_key] = payload
        else:
            set_state_value(state_map, target_key, payload)
        return payload
    set_state_value(request_or_scope.state, target_key, payload)
    return payload


def get_request_tenant(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Return any explicit tenant receipt stored for ``config_state`` on ``request``."""
    candidate_keys = [_tenant_state_key(config_state.session_key), _tenant_state_key(config_state.connection_key)]
    bind_key = getattr(config_state.config, "bind_key", None)
    if isinstance(bind_key, str) and bind_key:
        candidate_keys.append(_tenant_state_key(bind_key))
    candidate_keys.append(_TENANT_STATE_KEY)
    candidate_keys.append("tenant_context")

    raw_scope = getattr(request, "scope", None)
    scope_state = raw_scope.get("state") if isinstance(raw_scope, Mapping) else None
    for state_key in candidate_keys:
        stored = get_state_value(request.state, state_key, None)
        if stored is not None:
            return stored
        if isinstance(scope_state, Mapping) and scope_state.get(state_key) is not None:
            return scope_state[state_key]
    return None


def _tenant_state_key(key: str | None) -> str:
    return _TENANT_STATE_KEY if key is None else f"{_TENANT_STATE_KEY}_{key}"


def _state_dict(state: Any) -> dict[str, Any]:
    """Return the underlying state dictionary."""
    try:
        return cast("dict[str, Any]", object.__getattribute__(state, "_state"))
    except AttributeError:
        return cast("dict[str, Any]", state.__dict__)
