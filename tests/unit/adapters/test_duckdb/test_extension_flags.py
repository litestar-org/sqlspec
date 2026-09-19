"""DuckDB configuration tests for security/extension flag promotion."""

from pathlib import Path

import pytest

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")

from sqlspec.adapters.duckdb import DuckDBConfig


def test_duckdb_config_keeps_security_flags_in_the_startup_config() -> None:
    """Extension flags must reach duckdb.connect, which is the only time they can be set."""

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
    assert config.connection_config["allow_community_extensions"] is True
    assert config.connection_config["allow_unsigned_extensions"] is False
    assert config.connection_config["enable_external_access"] is True


def test_duckdb_config_merges_existing_extension_flags() -> None:
    """Flags supplied as a driver feature must also reach the startup config."""

    config = DuckDBConfig(
        connection_config={"database": ":memory:", "allow_community_extensions": True},
        driver_features={"extension_flags": {"custom": "value"}},
    )

    flags = config.driver_features.get("extension_flags")
    assert flags == {"custom": "value", "allow_community_extensions": True}
    assert config.connection_config["custom"] == "value"
    assert config.connection_config["allow_community_extensions"] is True


@pytest.mark.parametrize(
    ("flag", "value", "setting"),
    [
        ("allow_community_extensions", False, "allow_community_extensions"),
        ("allow_unsigned_extensions", True, "allow_unsigned_extensions"),
        ("enable_external_access", False, "enable_external_access"),
    ],
)
def test_extension_flags_take_effect_on_the_connection(flag: str, value: bool, setting: str, tmp_path: Path) -> None:
    """DuckDB rejects these settings at runtime, so each must be applied at startup."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / f"{flag}.duckdb"), flag: value})
    try:
        with config.provide_connection() as connection:
            observed = connection.execute(f"SELECT current_setting('{setting}')").fetchone()
        assert observed is not None
        assert bool(observed[0]) is value
    finally:
        config.close_pool()
