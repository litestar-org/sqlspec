"""AsyncPG adapter for SQLSpec."""

from sqlspec.adapters.asyncpg._typing import AsyncpgConnection, AsyncpgCursor, AsyncpgPool, AsyncpgPreparedStatement
from sqlspec.adapters.asyncpg.config import (
    AsyncpgConfig,
    AsyncpgConnectionConfig,
    AsyncpgDriverFeatures,
    AsyncpgPoolConfig,
)
from sqlspec.adapters.asyncpg.core import build_connection_config, default_statement_config
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver, AsyncpgExceptionHandler
from sqlspec.adapters.asyncpg.tenant import AsyncpgRLSStrategy

__all__ = (
    "AsyncpgConfig",
    "AsyncpgConnection",
    "AsyncpgConnectionConfig",
    "AsyncpgCursor",
    "AsyncpgDriver",
    "AsyncpgDriverFeatures",
    "AsyncpgExceptionHandler",
    "AsyncpgPool",
    "AsyncpgPoolConfig",
    "AsyncpgPreparedStatement",
    "AsyncpgRLSStrategy",
    "build_connection_config",
    "default_statement_config",
)
