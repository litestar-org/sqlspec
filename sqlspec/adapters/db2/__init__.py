"""IBM Db2 adapter for SQLSpec."""

from sqlspec.adapters.db2.config import Db2ConnectionParams, Db2DriverFeatures, Db2PoolParams, Db2SyncConfig
from sqlspec.adapters.db2.core import build_connection_config, default_statement_config
from sqlspec.adapters.db2.data_dictionary import Db2SyncDataDictionary, Db2VersionInfo
from sqlspec.adapters.db2.driver import Db2SyncDriver, Db2SyncExceptionHandler

__all__ = (
    "Db2ConnectionParams",
    "Db2DriverFeatures",
    "Db2PoolParams",
    "Db2SyncConfig",
    "Db2SyncDataDictionary",
    "Db2SyncDriver",
    "Db2SyncExceptionHandler",
    "Db2VersionInfo",
    "build_connection_config",
    "default_statement_config",
)
