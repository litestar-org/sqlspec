"""Psqlpy adapter for SQLSpec."""

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection, PsqlpyCursor
from sqlspec.adapters.psqlpy.config import PsqlpyConfig, PsqlpyConnectionParams, PsqlpyPoolParams
from sqlspec.adapters.psqlpy.core import default_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver, PsqlpyExceptionHandler
from sqlspec.dialects import postgres  # noqa: F401

__all__ = (
    "PsqlpyConfig",
    "PsqlpyConnection",
    "PsqlpyConnectionParams",
    "PsqlpyCursor",
    "PsqlpyDriver",
    "PsqlpyExceptionHandler",
    "PsqlpyPoolParams",
    "default_statement_config",
)
