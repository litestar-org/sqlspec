"""Flask extension for SQLSpec.

Provides request-scoped session management, automatic transaction handling,
and async adapter support via portal pattern.
"""

from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.flask.extension import SQLSpecPlugin
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

__all__ = ("FlaskConfigState", "SQLSpecAsyncService", "SQLSpecPlugin", "SQLSpecSyncService")
