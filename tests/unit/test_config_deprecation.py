"""Tests for config parameter deprecation (pool_config → connection_config, pool_instance → connection_instance).

Only adapters that previously supported pooling are tested here:
- asyncpg (async pooled)
- psycopg (sync/async pooled)
- psqlpy (async pooled)
- asyncmy (async pooled)
- oracledb (sync/async pooled)
- spanner (sync pooled)

Non-pooled adapters (sqlite, duckdb, aiosqlite, adbc, bigquery) never had pool_config/pool_instance.
"""

import warnings
from typing import Any

import pytest

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.spanner import SpannerSyncConfig


def test_pool_config_deprecated_psycopg_sync() -> None:
    """Test pool_config parameter triggers deprecation warning (sync pooled adapter)."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(pool_config={"conninfo": "postgresql://localhost/test"})

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "pool_config" in str(w[0].message)
        assert "connection_config" in str(w[0].message)
        assert "0.34.0" in str(w[0].message)
        assert config.connection_config["conninfo"] == "postgresql://localhost/test"


def test_pool_config_deprecated_asyncpg() -> None:
    """Test pool_config parameter triggers deprecation warning (async pooled adapter)."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/test"})

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "pool_config" in str(w[0].message)
        assert "connection_config" in str(w[0].message)
        assert config.connection_config["dsn"] == "postgresql://localhost/test"


def test_pool_config_deprecated_oracledb() -> None:
    """Test pool_config parameter triggers deprecation warning (Oracle pooled adapter)."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = OracleSyncConfig(pool_config={"user": "test", "password": "test"})

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "pool_config" in str(w[0].message)
        assert config.connection_config["user"] == "test"


def test_pool_instance_deprecated() -> None:
    """Test pool_instance parameter triggers deprecation warning."""
    mock_pool: Any = object()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(pool_instance=mock_pool)

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "pool_instance" in str(w[0].message)
        assert "connection_instance" in str(w[0].message)
        assert config.connection_instance is mock_pool


def test_new_parameter_takes_precedence() -> None:
    """Test new parameter wins when both old and new provided."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(
            pool_config={"conninfo": "postgresql://localhost/old"},
            connection_config={"conninfo": "postgresql://localhost/new"},
        )

        # Should get warning for pool_config but still use connection_config
        assert len(w) == 1
        assert "pool_config" in str(w[0].message)
        assert config.connection_config["conninfo"] == "postgresql://localhost/new"


def test_no_warning_when_using_new_params() -> None:
    """Test no warning when only new parameters used."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})

        assert len(w) == 0


def test_both_deprecated_params() -> None:
    """Test both deprecated parameters together."""
    mock_pool: Any = object()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(pool_config={"conninfo": "postgresql://localhost/test"}, pool_instance=mock_pool)

        assert len(w) == 2
        warning_messages = [str(warning.message) for warning in w]
        assert any("pool_config" in msg for msg in warning_messages)
        assert any("pool_instance" in msg for msg in warning_messages)
        assert config.connection_instance is mock_pool


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
def test_all_pooled_adapters_handle_deprecated_params(adapter_class: type) -> None:
    """Parametrized test ensuring all pooled adapters support deprecated parameter names.

    Only adapters that previously supported pool_config/pool_instance are tested:
    - asyncpg, asyncmy, psycopg (sync/async), oracledb (sync/async), psqlpy, spanner
    """
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        adapter_class(pool_config={})

        assert len(w) >= 1
        assert any("pool_config" in str(warning.message) for warning in w)
        assert all(issubclass(warning.category, DeprecationWarning) for warning in w)


def test_deprecation_message_format() -> None:
    """Test deprecation warning has correct format with all required information."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        PsycopgSyncConfig(pool_config={"conninfo": "postgresql://localhost/test"})

        assert len(w) == 1
        message = str(w[0].message)

        # Check all required elements are present
        assert "pool_config" in message  # Old parameter name
        assert "connection_config" in message  # New parameter name
        assert "0.33.0" in message  # Deprecated in version
        assert "0.34.0" in message  # Removal version
        assert "consistency" in message.lower()  # Rationale info


def test_connection_instance_precedence() -> None:
    """Test connection_instance takes precedence over pool_instance."""
    old_instance: Any = object()
    new_instance: Any = object()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(pool_instance=old_instance, connection_instance=new_instance)

        # Should get warning for pool_instance but use connection_instance
        assert len(w) == 1
        assert "pool_instance" in str(w[0].message)
        assert config.connection_instance is new_instance
        assert config.connection_instance is not old_instance


def test_empty_pool_config_deprecated() -> None:
    """Test empty pool_config dict triggers deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        PsycopgSyncConfig(pool_config={})

        assert len(w) == 1
        assert "pool_config" in str(w[0].message)


def test_none_pool_instance_deprecated() -> None:
    """Test explicitly passing None for pool_instance triggers warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(pool_instance=None)

        assert len(w) == 1
        assert "pool_instance" in str(w[0].message)
        assert config.connection_instance is None


def test_mixed_old_new_both_params() -> None:
    """Test when one old param and one new param provided together."""
    mock_pool: Any = object()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = PsycopgSyncConfig(
            pool_config={"conninfo": "postgresql://localhost/test"}, connection_instance=mock_pool
        )

        # Should only warn about pool_config
        assert len(w) == 1
        assert "pool_config" in str(w[0].message)
        assert config.connection_instance is mock_pool


def test_warning_stack_level() -> None:
    """Test deprecation warnings are DeprecationWarning category."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        # Create config with deprecated param
        PsycopgSyncConfig(pool_config={"conninfo": "postgresql://localhost/test"})

        assert len(w) == 1
        # Warning should be DeprecationWarning type
        assert issubclass(w[0].category, DeprecationWarning)
        # Warning is raised from config.py (stacklevel=2 in warn_deprecation)
        assert "config.py" in w[0].filename
