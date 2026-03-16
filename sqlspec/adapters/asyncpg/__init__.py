"""AsyncPG adapter for SQLSpec."""

from sqlspec.adapters.asyncpg._typing import AsyncpgConnection, AsyncpgCursor, AsyncpgPool, AsyncpgPreparedStatement
from sqlspec.adapters.asyncpg.config import AsyncpgConfig, AsyncpgConnectionConfig, AsyncpgPoolConfig
from sqlspec.adapters.asyncpg.core import default_statement_config
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver, AsyncpgExceptionHandler
from sqlspec.dialects import postgres  # noqa: F401

__all__ = (
    "AsyncpgConfig",
    "AsyncpgConnection",
    "AsyncpgConnectionConfig",
    "AsyncpgCursor",
    "AsyncpgDriver",
    "AsyncpgExceptionHandler",
    "AsyncpgPool",
    "AsyncpgPoolConfig",
    "AsyncpgPreparedStatement",
    "default_statement_config",
)
