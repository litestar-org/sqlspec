"""Snowflake connector adapter for SQLSpec."""

from sqlspec.adapters.snowflake._typing import SnowflakeConnection, SnowflakeCursor, SnowflakeSessionContext
from sqlspec.adapters.snowflake.config import (
    SnowflakeConfig,
    SnowflakeConnectionParams,
    SnowflakeDriverFeatures,
    SnowflakePoolConnectionContext,
    SnowflakePoolParams,
    SnowflakePoolSessionFactory,
)
from sqlspec.adapters.snowflake.core import (
    apply_driver_features,
    build_statement_config,
    default_statement_config,
    driver_profile,
    normalize_connection_params,
)
from sqlspec.adapters.snowflake.data_dictionary import SnowflakeSyncDataDictionary
from sqlspec.adapters.snowflake.driver import SnowflakeDriver, SnowflakeExceptionHandler
from sqlspec.adapters.snowflake.migrations import SnowflakeSyncMigrationTracker
from sqlspec.adapters.snowflake.pool import SnowflakeConnectionPool
from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter

__all__ = (
    "SnowflakeConfig",
    "SnowflakeConnection",
    "SnowflakeConnectionParams",
    "SnowflakeConnectionPool",
    "SnowflakeCursor",
    "SnowflakeDriver",
    "SnowflakeDriverFeatures",
    "SnowflakeExceptionHandler",
    "SnowflakeOutputConverter",
    "SnowflakePoolConnectionContext",
    "SnowflakePoolParams",
    "SnowflakePoolSessionFactory",
    "SnowflakeSessionContext",
    "SnowflakeSyncDataDictionary",
    "SnowflakeSyncMigrationTracker",
    "apply_driver_features",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
    "normalize_connection_params",
)
