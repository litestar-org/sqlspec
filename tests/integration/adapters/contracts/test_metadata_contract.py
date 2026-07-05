"""Shared adapter native metadata and statistics contracts."""

import pytest

from tests.integration.adapters.contracts._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts.behaviors import (
    assert_async_native_metadata_contract,
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
