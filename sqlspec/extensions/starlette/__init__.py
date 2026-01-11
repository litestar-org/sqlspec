"""Starlette extension for SQLSpec.

Provides middleware-based session management, automatic transaction handling,
and connection pooling lifecycle management for Starlette applications.
"""

from sqlspec.extensions.starlette._state import SQLSpecConfigState
from sqlspec.extensions.starlette._utils import get_connection_from_request, get_or_create_session
from sqlspec.extensions.starlette.extension import SQLSpecPlugin
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware

__all__ = (
    "SQLSpecAutocommitMiddleware",
    "SQLSpecConfigState",
    "SQLSpecManualMiddleware",
    "SQLSpecPlugin",
    "get_connection_from_request",
    "get_or_create_session",
)
