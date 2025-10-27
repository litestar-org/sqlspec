"""Starlette extension for SQLSpec.

Provides middleware-based session management, automatic transaction handling,
and connection pooling lifecycle management for Starlette applications.
"""

from sqlspec.extensions.starlette.extension import SQLSpecPlugin
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware

__all__ = ("SQLSpecAutocommitMiddleware", "SQLSpecManualMiddleware", "SQLSpecPlugin")
