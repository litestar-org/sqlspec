"""Request state helpers for the Starlette SQLSpec extension."""

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from sqlspec.extensions.starlette._state import tenant_receipt_key
from sqlspec.utils.adapters import resolve_adapter_name
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = (
    "deferred_session_key",
    "get_connection_from_request",
    "get_or_create_driver_session",
    "get_or_create_driver_session_async",
    "get_or_create_session",
    "get_request_tenant",
    "get_scope_tenant",
    "get_state_value",
    "has_state_value",
    "pop_state_value",
    "set_request_tenant",
    "set_state_value",
)

_MISSING = object()
_LAZY_BEGIN_ADAPTERS: Final[frozenset[str]] = frozenset({
    "adbc",
    "arrow_odbc",
    "asyncpg",
    "cockroach_asyncpg",
    "duckdb",
    "psqlpy",
    "teradata",
})


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
    """Get the request connection, checking it out on first access.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.

    Raises:
        ImproperConfigurationError: The configuration is asynchronous and its connection is not
            open; use the plugin's asynchronous connection accessor.
    """
    if has_state_value(request.state, config_state.connection_key):
        return get_state_value(request.state, config_state.connection_key)
    from sqlspec.extensions.starlette.middleware import acquire_pending_connection_sync

    return acquire_pending_connection_sync(request, config_state)


def get_or_create_session(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get or create database session for request.

    Sessions are cached per request to ensure the same session instance
    is returned for multiple calls within the same request. For an asynchronous
    configuration whose request connection is not open yet, the result is a
    deferred session that opens the connection on its first awaited call and
    forwards to the driver session afterwards.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session (driver instance).
    """
    deferred_key = deferred_session_key(config_state)
    deferred = get_state_value(request.state, deferred_key, None)
    if deferred is not None:
        return deferred
    if config_state.config.is_async and not has_state_value(request.state, config_state.connection_key):
        from sqlspec.extensions.starlette.middleware import deferred_request_session

        deferred = deferred_request_session(request, config_state)
        set_state_value(request.state, deferred_key, deferred)
        return deferred
    return get_or_create_driver_session(request, config_state)


def get_or_create_driver_session(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get or create the driver session bound to the request connection.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session (driver instance).
    """
    session, created = _resolve_or_build_driver_session(request, config_state)
    if created:
        runtime = session.observability
        if not runtime.is_idle:
            runtime.emit_session_start_sync(session)
    return session


async def get_or_create_driver_session_async(request: "Request", config_state: "SQLSpecConfigState") -> Any:
    """Get or create the driver session asynchronously, awaiting session-start hooks.

    Args:
        request: Starlette request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session (driver instance).
    """
    session, created = _resolve_or_build_driver_session(request, config_state)
    if created:
        runtime = session.observability
        if not runtime.is_idle:
            await runtime.emit_session_start_async(session)
    return session


def deferred_session_key(config_state: "SQLSpecConfigState") -> str:
    """Return the request-state key caching a configuration's deferred session."""
    return f"{config_state.session_key}_deferred"


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
    target_key = tenant_receipt_key(key)
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
    candidate_keys = config_state.tenant_receipt_keys

    raw_scope = getattr(request, "scope", None)
    scope_state = raw_scope.get("state") if isinstance(raw_scope, Mapping) else None
    for state_key in candidate_keys:
        stored = get_state_value(request.state, state_key, None)
        if stored is not None:
            return stored
        if isinstance(scope_state, Mapping) and scope_state.get(state_key) is not None:
            return scope_state[state_key]
    return None


def get_scope_tenant(scope: "Mapping[str, Any]", config_state: "SQLSpecConfigState") -> Any:
    """Return any explicit tenant receipt stored for ``config_state`` in the request scope state."""
    scope_state = scope.get("state")
    if not isinstance(scope_state, Mapping):
        return None
    for state_key in config_state.tenant_receipt_keys:
        stored = scope_state.get(state_key)
        if stored is not None:
            return stored
    return None


def _resolve_or_build_driver_session(request: "Request", config_state: "SQLSpecConfigState") -> "tuple[Any, bool]":
    """Return the cached driver session or build and cache a fresh one."""
    session_instance_key = f"{config_state.session_key}_instance"
    existing_session = get_state_value(request.state, session_instance_key, None)
    if existing_session is not None:
        return existing_session, False
    config = config_state.config
    session = config._prepare_driver(
        config.driver_type(
            connection=get_connection_from_request(request, config_state),
            statement_config=config.statement_config,
            driver_features=config.driver_features,
        )
    )
    if (
        config_state.commit_mode in {"autocommit", "autocommit_include_redirect"}
        and resolve_adapter_name(config) in _LAZY_BEGIN_ADAPTERS
    ):
        session._auto_begin_on_write = True
    set_state_value(request.state, session_instance_key, session)
    return session, True


def _state_dict(state: Any) -> dict[str, Any]:
    """Return the underlying state dictionary."""
    try:
        return cast("dict[str, Any]", object.__getattribute__(state, "_state"))
    except AttributeError:
        return cast("dict[str, Any]", state.__dict__)
