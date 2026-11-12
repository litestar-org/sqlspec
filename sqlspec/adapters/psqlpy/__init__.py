"""Psqlpy adapter for SQLSpec."""

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection, PsqlpyCursor
from sqlspec.adapters.psqlpy.config import PsqlpyConfig, PsqlpyConnectionParams, PsqlpyDriverFeatures, PsqlpyPoolParams
from sqlspec.adapters.psqlpy.core import build_connection_config, default_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver, PsqlpyExceptionHandler
from sqlspec.adapters.psqlpy.tenant import PsqlpyRLSStrategy

__all__ = (
    "PsqlpyConfig",
    "PsqlpyConnection",
    "PsqlpyConnectionParams",
    "PsqlpyCursor",
    "PsqlpyDriver",
    "PsqlpyDriverFeatures",
    "PsqlpyExceptionHandler",
    "PsqlpyPoolParams",
    "PsqlpyRLSStrategy",
    "build_connection_config",
    "default_statement_config",
)
