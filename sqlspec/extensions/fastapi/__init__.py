"""FastAPI extension for SQLSpec.

Extends Starlette integration with dependency injection helpers for FastAPI's
Depends() system, including filter dependency builders.
"""

from sqlspec.extensions.fastapi.extension import SQLSpecPlugin
from sqlspec.extensions.fastapi.providers import DependencyDefaults, FieldNameType, FilterConfig, provide_filters
from sqlspec.extensions.starlette.middleware import SQLSpecAutocommitMiddleware, SQLSpecManualMiddleware

__all__ = (
    "DependencyDefaults",
    "FieldNameType",
    "FilterConfig",
    "SQLSpecAutocommitMiddleware",
    "SQLSpecManualMiddleware",
    "SQLSpecPlugin",
    "provide_filters",
)
