"""DuckDB adapter for SQLSpec."""

from sqlspec.adapters.duckdb._typing import DuckDBConnection, DuckDBCursor
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBDriverFeatures,
    DuckDBExtensionConfig,
    DuckDBPoolParams,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.core import default_statement_config
from sqlspec.adapters.duckdb.driver import DuckDBDriver, DuckDBExceptionHandler
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

__all__ = (
    "DuckDBConfig",
    "DuckDBConnection",
    "DuckDBConnectionParams",
    "DuckDBConnectionPool",
    "DuckDBCursor",
    "DuckDBDriver",
    "DuckDBDriverFeatures",
    "DuckDBExceptionHandler",
    "DuckDBExtensionConfig",
    "DuckDBPoolParams",
    "DuckDBSecretConfig",
    "default_statement_config",
)
