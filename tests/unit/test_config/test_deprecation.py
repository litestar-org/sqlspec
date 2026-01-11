"""Tests for rejected legacy config parameters."""

import pytest

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize(
    "adapter_class",
    [
        AsyncpgConfig,
        AsyncmyConfig,
        PsycopgSyncConfig,
        PsycopgAsyncConfig,
        OracleSyncConfig,
        OracleAsyncConfig,
        PsqlpyConfig,
        SpannerSyncConfig,
    ],
)
def test_pool_config_rejected(adapter_class: type) -> None:
    """Legacy pool_config aliases should be rejected."""
    with pytest.raises(ImproperConfigurationError):
        adapter_class(pool_config={})


@pytest.mark.parametrize(
    "adapter_class",
    [
        AsyncpgConfig,
        AsyncmyConfig,
        PsycopgSyncConfig,
        PsycopgAsyncConfig,
        OracleSyncConfig,
        OracleAsyncConfig,
        PsqlpyConfig,
        SpannerSyncConfig,
    ],
)
def test_pool_instance_rejected(adapter_class: type) -> None:
    """Legacy pool_instance aliases should be rejected."""
    with pytest.raises(ImproperConfigurationError):
        adapter_class(pool_instance=None)
