"""Shared adapter native metadata and statistics contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_native_metadata_contract,
    assert_sync_native_metadata_contract,
    assert_sync_native_statistics_contract,
)


def test_sync_native_metadata_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers with native metadata list contract tables and columns."""
    assert_sync_native_metadata_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_native_metadata_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers with native metadata list contract tables and columns."""
    await assert_async_native_metadata_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_native_statistics_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers surface native statistics or fail clearly."""
    assert_sync_native_statistics_contract(sync_driver_case.driver, sync_driver_case.case)
