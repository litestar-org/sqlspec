"""Starlette extension for SQLSpec.

Provides middleware-based session management, automatic transaction handling,
and connection pooling lifecycle management for Starlette applications.
"""

from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette._utils import get_connection_from_request, get_or_create_session, set_request_tenant
from sqlspec.extensions.starlette.extension import SQLSpecPlugin
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from sqlspec.utils.tenant import SQLSpecTenantMiddleware, tenant_bypass

__all__ = (
    "SQLSpecAsyncService",
    "SQLSpecAutocommitMiddleware",
    "SQLSpecConfigState",
    "SQLSpecManualMiddleware",
    "SQLSpecPlugin",
    "SQLSpecSyncService",
    "SQLSpecTenantMiddleware",
    "get_connection_from_request",
    "get_or_create_session",
    "set_request_tenant",
    "tenant_bypass",
)
