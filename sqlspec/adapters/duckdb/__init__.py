"""DuckDB adapter for SQLSpec."""

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, DuckDBExceptionHandler, duckdb_statement_config

__all__ = (
    "DuckDBConfig",
    "DuckDBConnection",
    "DuckDBConnectionParams",
    "DuckDBCursor",
    "DuckDBDriver",
    "DuckDBExceptionHandler",
    "DuckDBExtensionConfig",
    "DuckDBSecretConfig",
    "duckdb_statement_config",
)
