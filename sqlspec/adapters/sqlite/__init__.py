"""SQLite adapter for SQLSpec."""

from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionParams, SqliteDriverFeatures
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, SqliteExceptionHandler, sqlite_statement_config
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
    "sqlite_statement_config",
)
