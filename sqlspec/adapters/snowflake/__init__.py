"""Snowflake connector adapter for SQLSpec."""

from typing import TYPE_CHECKING, Any

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
from sqlspec.adapters.snowflake.pool import SnowflakeConnectionPool
from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter
from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from sqlspec.adapters.snowflake.data_dictionary import SnowflakeSyncDataDictionary
    from sqlspec.adapters.snowflake.driver import SnowflakeDriver, SnowflakeExceptionHandler
    from sqlspec.adapters.snowflake.migrations import SnowflakeSyncMigrationTracker

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


def __getattr__(name: str) -> Any:
    modules = {
        "SnowflakeDriver": "sqlspec.adapters.snowflake.driver",
        "SnowflakeExceptionHandler": "sqlspec.adapters.snowflake.driver",
        "SnowflakeSyncDataDictionary": "sqlspec.adapters.snowflake.data_dictionary",
        "SnowflakeSyncMigrationTracker": "sqlspec.adapters.snowflake.migrations",
    }
    if name in modules:
        resolved = import_optional_attr(modules[name], name)
        if resolved is not None:
            globals()[name] = resolved
            return resolved
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
