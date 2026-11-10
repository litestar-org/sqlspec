"""Litestar framework integration for SQLSpec."""

from sqlspec.extensions.litestar._utils import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.extensions.litestar.channels import SQLSpecChannelsBackend
from sqlspec.extensions.litestar.cli import database_group
from sqlspec.extensions.litestar.config import LitestarConfig
from sqlspec.extensions.litestar.plugin import (
    DEFAULT_COMMIT_MODE,
    DEFAULT_CONNECTION_KEY,
    DEFAULT_POOL_KEY,
    DEFAULT_SESSION_KEY,
    TRACE_CONTEXT_FALLBACK_HEADERS,
    CommitMode,
    CorrelationMiddleware,
    SQLSpecPlugin,
)
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.extensions.litestar.tenant import (
    TenantResolutionSettings,
    require_tenant,
    resolve_litestar_tenant,
    set_connection_tenant,
)
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService
from sqlspec.utils.tenant import SQLSpecTenantMiddleware

__all__ = (
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "TRACE_CONTEXT_FALLBACK_HEADERS",
    "BaseSQLSpecStore",
    "CommitMode",
    "CorrelationMiddleware",
    "LitestarConfig",
    "SQLSpecAsyncService",
    "SQLSpecChannelsBackend",
    "SQLSpecPlugin",
    "SQLSpecSyncService",
    "SQLSpecTenantMiddleware",
    "TenantResolutionSettings",
    "database_group",
    "get_sqlspec_scope_state",
    "require_tenant",
    "resolve_litestar_tenant",
    "set_connection_tenant",
    "set_sqlspec_scope_state",
)
