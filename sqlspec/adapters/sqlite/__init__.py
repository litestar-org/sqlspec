"""SQLite adapter for SQLSpec."""

from sqlspec.adapters.sqlite._typing import SqliteConnection, SqliteCursor
from sqlspec.adapters.sqlite.config import (
    SqliteAggregateConfig,
    SqliteCollationConfig,
    SqliteConfig,
    SqliteConnectionParams,
    SqliteDriverFeatures,
    SqliteFunctionConfig,
)
from sqlspec.adapters.sqlite.core import default_statement_config
from sqlspec.adapters.sqlite.driver import SqliteDriver, SqliteExceptionHandler
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

__all__ = (
    "SqliteAggregateConfig",
    "SqliteCollationConfig",
    "SqliteConfig",
    "SqliteConnection",
    "SqliteConnectionParams",
    "SqliteConnectionPool",
    "SqliteCursor",
    "SqliteDriver",
    "SqliteDriverFeatures",
    "SqliteExceptionHandler",
    "SqliteFunctionConfig",
    "default_statement_config",
)
