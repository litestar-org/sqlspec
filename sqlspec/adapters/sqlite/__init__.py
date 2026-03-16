"""SQLite adapter for SQLSpec."""

from sqlspec.adapters.sqlite._typing import SqliteConnection, SqliteCursor
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionParams, SqliteDriverFeatures
from sqlspec.adapters.sqlite.core import default_statement_config
from sqlspec.adapters.sqlite.driver import SqliteDriver, SqliteExceptionHandler
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

__all__ = (
    "SqliteConfig",
    "SqliteConnection",
    "SqliteConnectionParams",
    "SqliteConnectionPool",
    "SqliteCursor",
    "SqliteDriver",
    "SqliteDriverFeatures",
    "SqliteExceptionHandler",
    "default_statement_config",
)
