"""CockroachDB psycopg adapter for SQLSpec."""

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachAsyncConnection,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgSyncSessionContext,
    CockroachSyncConnection,
)
from sqlspec.adapters.cockroach_psycopg.config import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgConnectionConfig,
    CockroachPsycopgDriverFeatures,
    CockroachPsycopgPoolConfig,
    CockroachPsycopgSyncConfig,
    build_connection_config,
)
from sqlspec.adapters.cockroach_psycopg.core import CockroachPsycopgRetryConfig, build_statement_config, driver_profile
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgAsyncExceptionHandler,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncExceptionHandler,
)
from sqlspec.adapters.cockroach_psycopg.tenant import CockroachPsycopgAsyncRLSStrategy, CockroachPsycopgSyncRLSStrategy

__all__ = (
    "CockroachAsyncConnection",
    "CockroachPsycopgAsyncConfig",
    "CockroachPsycopgAsyncDriver",
    "CockroachPsycopgAsyncExceptionHandler",
    "CockroachPsycopgAsyncRLSStrategy",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgConnectionConfig",
    "CockroachPsycopgDriverFeatures",
    "CockroachPsycopgPoolConfig",
    "CockroachPsycopgRetryConfig",
    "CockroachPsycopgSyncConfig",
    "CockroachPsycopgSyncDriver",
    "CockroachPsycopgSyncExceptionHandler",
    "CockroachPsycopgSyncRLSStrategy",
    "CockroachPsycopgSyncSessionContext",
    "CockroachSyncConnection",
    "build_connection_config",
    "build_statement_config",
    "driver_profile",
)
