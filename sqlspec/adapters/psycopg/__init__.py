"""Psycopg synchronous and asynchronous PostgreSQL database adapter."""

from sqlspec.adapters.psycopg._typing import (
    PsycopgAsyncConnection,
    PsycopgAsyncCursor,
    PsycopgSyncConnection,
    PsycopgSyncCursor,
)
from sqlspec.adapters.psycopg.config import (
    PsycopgAsyncConfig,
    PsycopgConnectionParams,
    PsycopgDriverFeatures,
    PsycopgPoolParams,
    PsycopgSyncConfig,
    build_connection_config,
)
from sqlspec.adapters.psycopg.core import default_statement_config
from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncDriver,
    PsycopgAsyncExceptionHandler,
    PsycopgSyncDriver,
    PsycopgSyncExceptionHandler,
)
from sqlspec.adapters.psycopg.tenant import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy

__all__ = (
    "PsycopgAsyncConfig",
    "PsycopgAsyncConnection",
    "PsycopgAsyncCursor",
    "PsycopgAsyncDriver",
    "PsycopgAsyncExceptionHandler",
    "PsycopgAsyncRLSStrategy",
    "PsycopgConnectionParams",
    "PsycopgDriverFeatures",
    "PsycopgPoolParams",
    "PsycopgSyncConfig",
    "PsycopgSyncConnection",
    "PsycopgSyncCursor",
    "PsycopgSyncDriver",
    "PsycopgSyncExceptionHandler",
    "PsycopgSyncRLSStrategy",
    "build_connection_config",
    "default_statement_config",
)
