"""Request context state utilities for the Sanic SQLSpec extension."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic._state import tenant_receipt_key
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.extensions.sanic._state import SanicConfigState

__all__ = (
    "get_connection_from_request",
    "get_context_value",
    "get_or_create_session",
    "get_request_tenant",
    "has_context_value",
    "pop_context_value",
    "set_context_value",
    "set_request_tenant",
)

_MISSING = object()


def get_context_value(context: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to retrieve.
        default: Optional value returned when the key is missing.

    Returns:
        Stored context value.
    """
    if default is _MISSING:
        return getattr(context, key)
    return getattr(context, key, default)


def set_context_value(context: Any, key: str, value: Any) -> None:
    """Set a value on a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to set.
        value: Value to store.
    """
    setattr(context, key, value)


def pop_context_value(context: Any, key: str) -> Any | None:
    """Remove a value from a Sanic ``ctx`` object.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to remove.

    Returns:
        Removed value if present, otherwise ``None``.
    """
    if not hasattr(context, key):
        return None
    value = getattr(context, key)
    delattr(context, key)
    return value


def has_context_value(context: Any, key: str) -> bool:
    """Check if a Sanic ``ctx`` object has a stored value.

    Args:
        context: Sanic ``app.ctx`` or ``request.ctx`` object.
        key: Attribute name to check.

    Returns:
        ``True`` when the key is present.
    """
    return hasattr(context, key)


def get_connection_from_request(request: Any, config_state: "SanicConfigState") -> Any:
    """Get database connection from request context.

    Args:
        request: Sanic request instance.
        config_state: Configuration state for the database.

    Returns:
        Database connection object.

    Raises:
        ImproperConfigurationError: If SQLSpec request middleware has not stored a connection.
    """
    try:
        return get_context_value(request.ctx, config_state.connection_key)
    except AttributeError as exc:
        msg = (
            f"Sanic request context does not contain SQLSpec connection '{config_state.connection_key}'. "
            "Ensure SQLSpecPlugin is initialized and its request middleware runs before accessing sessions."
        )
        raise ImproperConfigurationError(msg) from exc


def get_or_create_session(request: Any, config_state: "SanicConfigState") -> Any:
    """Get or create database session for request.

    Sessions are cached per request to return the same session instance across
    multiple calls in one request.

    Args:
        request: Sanic request instance.
        config_state: Configuration state for the database.

    Returns:
        Database session driver instance.
    """
    session_instance_key = f"{config_state.session_key}_instance"

    existing_session = get_context_value(request.ctx, session_instance_key, None)
    if existing_session is not None:
        return existing_session
    session = config_state.config.driver_type(
        connection=get_connection_from_request(request, config_state),
        statement_config=config_state.config.statement_config,
        driver_features=config_state.config.driver_features,
    )
    set_context_value(request.ctx, session_instance_key, session)
    return session


def set_request_tenant(
    request: Any,
    tenant_id: str | UUID | TenantContextPayload,
    *,
    variables: Mapping[str, str] | None = None,
    key: str | None = None,
) -> TenantContextPayload:
    """Store an authorized tenant receipt on a Sanic ``request.ctx``.

    Args:
        request: Sanic request instance (or ``request.ctx`` directly).
        tenant_id: Verified tenant identifier or immutable payload.
        variables: Extra session variables merged into the stored payload.
        key: Optional configuration ``session_key``, ``connection_key``, or
            ``bind_key`` scoping the receipt to one database configuration.

    Returns:
        The immutable ``TenantContextPayload`` stored on ``request.ctx``.
    """
    ctx = getattr(request, "ctx", request)
    payload = tenant_id if isinstance(tenant_id, TenantContextPayload) else TenantContextPayload(tenant_id)
    if variables is not None:
        payload = TenantContextPayload(
            payload.tenant_id, variables={**payload.variables, **variables}, is_bypass=payload.is_bypass
        )
    set_context_value(ctx, tenant_receipt_key(key), payload)
    return payload


def get_request_tenant(request: Any, config_state: "SanicConfigState") -> Any:
    """Return any explicit tenant receipt stored for ``config_state`` on ``request.ctx``."""
    ctx = getattr(request, "ctx", request)
    candidate_keys = config_state.tenant_receipt_keys
    for receipt_key in candidate_keys:
        stored = get_context_value(ctx, receipt_key, None)
        if stored is not None:
            return stored
    return None
