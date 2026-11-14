"""Helper utilities for Flask extension."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from sqlspec.extensions._deferred_session import PortalSession
from sqlspec.extensions.flask._state import tenant_receipt_key
from sqlspec.utils.adapters import resolve_adapter_name
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.extensions.flask._state import FlaskConfigState
    from sqlspec.protocols import DictProtocol
    from sqlspec.utils.portal import Portal

__all__ = (
    "get_context_value",
    "get_or_create_session",
    "get_request_tenant",
    "has_context_value",
    "pop_context_value",
    "set_context_value",
    "set_request_tenant",
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


def get_context_value(target: Any, key: str, default: Any = _MISSING) -> Any:
    """Get a value from a Flask context object."""
    data = _context_dict(target)
    if default is _MISSING:
        return data[key]
    return data.get(key, default)


def set_context_value(target: Any, key: str, value: Any) -> None:
    """Set a value on a Flask context object."""
    _context_dict(target)[key] = value


def pop_context_value(target: Any, key: str) -> Any | None:
    """Remove a value from a Flask context object."""
    return _context_dict(target).pop(key, None)


def has_context_value(target: Any, key: str) -> bool:
    """Check if a Flask context object has a stored value."""
    return key in _context_dict(target)


def get_or_create_session(config_state: "FlaskConfigState", portal: "Portal | None") -> Any:
    """Get or create database session for current request.

    Sessions are cached per request in Flask g object to ensure
    the same session is reused throughout the request lifecycle. For an
    asynchronous configuration with a portal, the result is a blocking session
    that runs each call of the asynchronous driver on the portal's event loop.

    Args:
        config_state: Configuration state for this database.
        portal: Portal for async operations (None for sync).

    Returns:
        Database session (driver instance).
    """
    from flask import g

    cache_key = f"sqlspec_session_cache_{config_state.session_key}"
    blocking_key = f"sqlspec_portal_session_{config_state.session_key}"
    if not config_state.is_async:
        portal = None
    if portal is not None:
        existing_view = get_context_value(g, blocking_key, None)
        if existing_view is not None:
            return existing_view
    session = get_context_value(g, cache_key, None)
    if session is None:
        config = config_state.config
        session = config._prepare_driver(
            config.driver_type(
                connection=get_context_value(g, config_state.connection_key),
                statement_config=config.statement_config,
                driver_features=config.driver_features,
            )
        )
        if (
            config_state.commit_mode in {"autocommit", "autocommit_include_redirect"}
            and resolve_adapter_name(config) in _LAZY_BEGIN_ADAPTERS
        ):
            session._auto_begin_on_write = True
        runtime = session.observability
        if not runtime.is_idle:
            runtime.emit_session_start_sync(session)
        set_context_value(g, cache_key, session)
    if portal is None:
        return session
    view = PortalSession(session, portal)
    set_context_value(g, blocking_key, view)
    return view


def set_request_tenant(
    tenant_id: str | UUID | TenantContextPayload, *, variables: Mapping[str, str] | None = None, key: str | None = None
) -> TenantContextPayload:
    """Store an authorized tenant receipt on Flask ``g`` for the active request.

    Args:
        tenant_id: Verified tenant identifier or immutable payload.
        variables: Extra session variables merged into the stored payload.
        key: Optional configuration ``session_key``, ``connection_key``, or
            ``bind_key`` scoping the receipt to one database configuration.

    Returns:
        The immutable ``TenantContextPayload`` stored on ``flask.g``.
    """
    from flask import g

    payload = tenant_id if isinstance(tenant_id, TenantContextPayload) else TenantContextPayload(tenant_id)
    if variables is not None:
        payload = TenantContextPayload(
            payload.tenant_id, variables={**payload.variables, **variables}, is_bypass=payload.is_bypass
        )
    set_context_value(g, tenant_receipt_key(key), payload)
    return payload


def get_request_tenant(config_state: "FlaskConfigState") -> Any:
    """Return any explicit tenant receipt stored for ``config_state`` on ``flask.g``."""
    from flask import g

    candidate_keys = config_state.tenant_receipt_keys
    for receipt_key in candidate_keys:
        stored = get_context_value(g, receipt_key, None)
        if stored is not None:
            return stored
    return None


def _context_dict(target: Any) -> dict[str, Any]:
    """Return the underlying context dictionary."""
    return cast("DictProtocol", target).__dict__
