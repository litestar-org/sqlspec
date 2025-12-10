"""DuckDB configuration tests for security/extension flag promotion."""

import pytest

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")

from sqlspec.adapters.duckdb import DuckDBConfig


def test_duckdb_config_promotes_security_flags() -> None:
    """Extension flags should move from connection_config to driver_features."""

    config = DuckDBConfig(
        connection_config={
            "database": ":memory:",
            "allow_community_extensions": True,
            "allow_unsigned_extensions": False,
            "enable_external_access": True,
        }
    )

    flags = config.driver_features.get("extension_flags")
    assert flags == {
        "allow_community_extensions": True,
        "allow_unsigned_extensions": False,
        "enable_external_access": True,
    }
    assert "allow_community_extensions" not in config.connection_config
    assert "allow_unsigned_extensions" not in config.connection_config
    assert "enable_external_access" not in config.connection_config


def test_duckdb_config_merges_existing_extension_flags() -> None:
    """Existing driver feature flags should merge with promoted ones."""

    config = DuckDBConfig(
        connection_config={"database": ":memory:", "allow_community_extensions": True},
        driver_features={"extension_flags": {"custom": "value"}},
    )

    flags = config.driver_features.get("extension_flags")
    assert flags == {"custom": "value", "allow_community_extensions": True}
