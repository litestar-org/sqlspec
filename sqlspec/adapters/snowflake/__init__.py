"""Snowflake connector adapter for SQLSpec."""

from sqlspec.adapters.snowflake._typing import SnowflakeConnection, SnowflakeCursor, SnowflakeSessionContext
from sqlspec.adapters.snowflake.config import (
    SnowflakeConfig,
    SnowflakeConnectionParams,
    SnowflakeDriverFeatures,
    SnowflakePoolParams,
)
from sqlspec.adapters.snowflake.core import default_statement_config, driver_profile, normalize_connection_params
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
    "SnowflakePoolParams",
    "SnowflakeSessionContext",
    "SnowflakeSyncDataDictionary",
    "SnowflakeSyncMigrationTracker",
    "default_statement_config",
    "driver_profile",
    "normalize_connection_params",
)
