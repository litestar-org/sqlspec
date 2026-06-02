"""Shared result materialization contracts for adapter integration tests."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import assert_async_result_contract, assert_sync_result_contract


def test_sync_result_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers materialize rows, scalar values, and empty results consistently."""
    assert_sync_result_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_result_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers materialize rows, scalar values, and empty results consistently."""
    await assert_async_result_contract(async_driver_case.driver, async_driver_case.case)
