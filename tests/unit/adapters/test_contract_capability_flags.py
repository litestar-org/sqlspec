"""Unit tests for the additive contract capability flags on DriverCase."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCase,
    get_driver_case,
    sync_driver_params_with,
    sync_driver_params_without,
)

NEW_CAPABILITY_FLAGS = (
    "supports_lob",
    "supports_native_array_codec",
    "supports_json_native",
    "supports_merge",
    "supports_copy",
    "supports_pooling",
    "supports_multi_schema_migrations",
    "supports_data_dictionary",
    "supports_native_metadata",
    "supports_native_statistics",
    "supports_arrow_streaming",
    "supports_native_arrow",
    "arrow_reader_honors_batch_size",
)


@pytest.fixture
def bare_case() -> DriverCase:
    return DriverCase(id="bare", fixture_name="f", adapter="a", dialect="sqlite", mode="sync")


@pytest.mark.parametrize("flag", NEW_CAPABILITY_FLAGS)
def test_new_capability_flag_defaults_false(bare_case: DriverCase, flag: str) -> None:
    """Every new capability flag is additive and opt-in (defaults False), so opting into nothing is a no-op."""
    assert getattr(bare_case, flag) is False


def test_existing_vector_flag_still_present(bare_case: DriverCase) -> None:
    """The pre-existing supports_vector flag is retained alongside the new flags."""
    assert bare_case.supports_vector is False


def test_new_capability_flags_opt_in_independently() -> None:
    """Each new capability can be explicitly enabled on a case."""
    case = DriverCase(
        id="opt",
        fixture_name="f",
        adapter="a",
        dialect="sqlite",
        mode="sync",
        supports_lob=True,
        supports_native_array_codec=True,
        supports_json_native=True,
        supports_merge=True,
        supports_copy=True,
        supports_pooling=True,
        supports_multi_schema_migrations=True,
        supports_data_dictionary=True,
        supports_native_metadata=True,
        supports_native_statistics=True,
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        arrow_reader_honors_batch_size=True,
    )
    for flag in NEW_CAPABILITY_FLAGS:
        assert getattr(case, flag) is True


def test_snowflake_capabilities_route_storage_and_savepoint_contracts() -> None:
    """Registered capabilities select native Arrow contracts and exclude savepoint contracts."""
    pytest.importorskip("snowflake.connector")
    pytest.importorskip("pyarrow")
    from sqlspec.adapters.snowflake import SnowflakeConfig

    case = get_driver_case("snowflake-sync")
    arrow_cases = [parameter.values[0] for parameter in sync_driver_params_with("supports_native_arrow")]
    savepoint_cases = [parameter.values[0] for parameter in sync_driver_params_with("supports_savepoints")]
    no_savepoint_cases = [parameter.values[0] for parameter in sync_driver_params_without("supports_savepoints")]
    assert case in arrow_cases
    assert case not in savepoint_cases
    assert case in no_savepoint_cases
    config = SnowflakeConfig(connection_config={"account": "org-account", "user": "service"})
    capabilities = config.storage_capabilities()
    assert capabilities["arrow_export_enabled"] is True
    assert capabilities["arrow_import_enabled"] is True
    assert capabilities["parquet_export_enabled"] is True
    assert capabilities["parquet_import_enabled"] is True
    capabilities["arrow_export_enabled"] = False
    assert config.storage_capabilities()["arrow_export_enabled"] is True
