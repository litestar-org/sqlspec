"""DuckDB adapter for SQLSpec."""

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.driver import DuckDBDriver

__all__ = (
    "DuckDBConfig",
    "DuckDBConnection",
    "DuckDBConnectionParams",
    "DuckDBDriver",
    "DuckDBExtensionConfig",
    "DuckDBSecretConfig",
)
