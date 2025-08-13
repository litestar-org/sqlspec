"""DuckDB adapter for SQLSpec."""

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, DuckDBExceptionHandler, duckdb_statement_config
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
    "duckdb_statement_config",
)
