"""Sanic extension for SQLSpec.

Provides Sanic-native app and request context helpers plus plugin wiring for
connection lifecycle and request-scoped sessions.
"""

from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.sanic._utils import get_connection_from_request, get_or_create_session
from sqlspec.extensions.sanic.extension import SQLSpecPlugin
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

__all__ = (
    "SQLSpecAsyncService",
    "SQLSpecPlugin",
    "SQLSpecSyncService",
    "SanicConfigState",
    "get_connection_from_request",
    "get_or_create_session",
)
