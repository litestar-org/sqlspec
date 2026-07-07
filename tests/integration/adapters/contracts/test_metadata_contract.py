"""Shared adapter native metadata and statistics contracts."""

from typing import Any, cast

import pytest

from sqlspec.data_dictionary import MetadataSupport, SystemMetadataRequest, SystemMetadataResult
from tests.integration.adapters.contracts._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts.behaviors import (
    assert_async_data_dictionary_contract,
    assert_async_data_dictionary_schema_contract,
    assert_async_data_dictionary_topology_contract,
    assert_async_native_metadata_contract,
    assert_sync_data_dictionary_contract,
    assert_sync_data_dictionary_schema_contract,
    assert_sync_data_dictionary_topology_contract,
    assert_sync_native_metadata_contract,
    assert_sync_native_statistics_contract,
)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_native_metadata"), indirect=True
)
def test_sync_native_metadata_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers with native metadata list contract tables and columns."""
    assert_sync_native_metadata_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_native_metadata"), indirect=True
)
async def test_async_native_metadata_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers with native metadata list contract tables and columns."""
    await assert_async_native_metadata_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_native_statistics"), indirect=True
)
def test_sync_native_statistics_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers surface native statistics or fail clearly."""
    assert_sync_native_statistics_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_data_dictionary"), indirect=True
)
def test_sync_data_dictionary_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync data dictionaries expose portable version, feature, type, table, and column metadata."""
    assert_sync_data_dictionary_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_data_dictionary"), indirect=True
)
def test_sync_data_dictionary_capability_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync data dictionaries expose truthful replacement metadata capability tiers."""
    case = sync_capability_driver_case.case

    assert isinstance(case.supports_data_dictionary_core, bool)
    assert isinstance(case.supports_data_dictionary_constraints, bool)
    assert isinstance(case.supports_data_dictionary_ddl, bool)
    assert isinstance(case.supports_data_dictionary_dependencies, bool)
    assert isinstance(case.supports_data_dictionary_system, bool)
    assert isinstance(case.supports_data_dictionary_transport_metadata, bool)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_data_dictionary"), indirect=True
)
async def test_async_data_dictionary_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async data dictionaries expose portable version, feature, type, table, and column metadata."""
    await assert_async_data_dictionary_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_data_dictionary"), indirect=True
)
async def test_async_data_dictionary_capability_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async data dictionaries expose truthful replacement metadata capability tiers."""
    case = async_capability_driver_case.case

    assert isinstance(case.supports_data_dictionary_core, bool)
    assert isinstance(case.supports_data_dictionary_constraints, bool)
    assert isinstance(case.supports_data_dictionary_ddl, bool)
    assert isinstance(case.supports_data_dictionary_dependencies, bool)
    assert isinstance(case.supports_data_dictionary_system, bool)
    assert isinstance(case.supports_data_dictionary_transport_metadata, bool)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_data_dictionary"), indirect=True
)
def test_sync_data_dictionary_system_metadata_disabled_by_default(
    sync_capability_driver_case: DriverCaseContext,
) -> None:
    """Sync system metadata contract fails closed without explicit opt-in."""
    data_dictionary = cast("Any", sync_capability_driver_case.driver).data_dictionary

    result = data_dictionary.get_system_metadata(
        sync_capability_driver_case.driver, SystemMetadataRequest(domain="sessions")
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.GATED
    assert result.rows == ()


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_data_dictionary"), indirect=True
)
async def test_async_data_dictionary_system_metadata_disabled_by_default(
    async_capability_driver_case: DriverCaseContext,
) -> None:
    """Async system metadata contract fails closed without explicit opt-in."""
    data_dictionary = cast("Any", async_capability_driver_case.driver).data_dictionary

    result = await data_dictionary.get_system_metadata(
        async_capability_driver_case.driver, SystemMetadataRequest(domain="sessions")
    )

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.GATED
    assert result.rows == ()


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_schema_qualified_data_dictionary"), indirect=True
)
def test_sync_data_dictionary_schema_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync data dictionaries support schema-qualified column discovery."""
    assert_sync_data_dictionary_schema_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_schema_qualified_data_dictionary"), indirect=True
)
async def test_async_data_dictionary_schema_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async data dictionaries support schema-qualified column discovery."""
    await assert_async_data_dictionary_schema_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_data_dictionary_topology"), indirect=True
)
def test_sync_data_dictionary_topology_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync data dictionaries sort table dependencies and surface FK/index metadata."""
    assert_sync_data_dictionary_topology_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_data_dictionary_topology"), indirect=True
)
async def test_async_data_dictionary_topology_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async data dictionaries sort table dependencies and surface FK/index metadata."""
    await assert_async_data_dictionary_topology_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
