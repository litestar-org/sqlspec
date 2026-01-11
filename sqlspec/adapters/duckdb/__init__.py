"""DuckDB adapter for SQLSpec."""

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.core import default_statement_config
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, DuckDBExceptionHandler
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

__all__ = (
    "DuckDBConfig",
    "DuckDBConnection",
    "DuckDBConnectionParams",
    "DuckDBConnectionPool",
    "DuckDBCursor",
    "DuckDBDriver",
    "DuckDBExceptionHandler",
    "DuckDBExtensionConfig",
    "DuckDBSecretConfig",
    "default_statement_config",
)
