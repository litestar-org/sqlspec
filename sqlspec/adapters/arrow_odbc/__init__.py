"""SQLSpec adapter for arrow-odbc."""

from sqlspec.adapters.arrow_odbc.config import ArrowOdbcConfig, ArrowOdbcConnectionParams, ArrowOdbcDriverFeatures
from sqlspec.adapters.arrow_odbc.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.data_dictionary import ArrowOdbcDataDictionary
from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver, ArrowOdbcExceptionHandler
from sqlspec.adapters.arrow_odbc.type_converter import odbc_type_to_arrow

__all__ = (
    "ArrowOdbcConfig",
    "ArrowOdbcConnectionParams",
    "ArrowOdbcDataDictionary",
    "ArrowOdbcDriver",
    "ArrowOdbcDriverFeatures",
    "ArrowOdbcExceptionHandler",
    "apply_driver_features",
    "build_connection_config",
    "build_statement_config",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "odbc_type_to_arrow",
    "resolve_dialect_from_dbms_name",
)
