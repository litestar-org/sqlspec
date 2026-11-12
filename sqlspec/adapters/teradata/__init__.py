"""Teradata Vantage adapter for SQLSpec."""

from sqlspec.adapters.teradata._typing import (
    TeradataConnection,
    TeradataCursor,
    TeradataRawCursor,
    TeradataSessionContext,
)
from sqlspec.adapters.teradata.config import (
    TeradataConfig,
    TeradataConnectionContext,
    TeradataConnectionParams,
    TeradataDriverFeatures,
    TeradataLogMech,
    TeradataPoolParams,
    TeradataSSLMode,
    TeradataTransactionMode,
)
from sqlspec.adapters.teradata.core import (
    DEFAULT_TERADATA_PORT,
    apply_driver_features,
    build_connection_config,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_query_band,
)
from sqlspec.adapters.teradata.data_dictionary import TeradataDataDictionary, TeradataSyncDataDictionary
from sqlspec.adapters.teradata.driver import TeradataDriver, TeradataExceptionHandler
from sqlspec.adapters.teradata.migrations import TeradataSyncMigrationTracker
from sqlspec.adapters.teradata.pool import TeradataConnectionPool

__all__ = (
    "DEFAULT_TERADATA_PORT",
    "TeradataConfig",
    "TeradataConnection",
    "TeradataConnectionContext",
    "TeradataConnectionParams",
    "TeradataConnectionPool",
    "TeradataCursor",
    "TeradataDataDictionary",
    "TeradataDriver",
    "TeradataDriverFeatures",
    "TeradataExceptionHandler",
    "TeradataLogMech",
    "TeradataPoolParams",
    "TeradataRawCursor",
    "TeradataSSLMode",
    "TeradataSessionContext",
    "TeradataSyncDataDictionary",
    "TeradataSyncMigrationTracker",
    "TeradataTransactionMode",
    "apply_driver_features",
    "build_connection_config",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "format_query_band",
)
