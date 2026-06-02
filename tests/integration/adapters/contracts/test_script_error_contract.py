"""Shared script execution and error mapping contracts for adapter integration tests."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_script_error_contract,
    assert_sync_script_error_contract,
)


def test_sync_script_error_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers execute scripts and map generic SQL errors consistently."""
    assert_sync_script_error_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_script_error_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers execute scripts and map generic SQL errors consistently."""
    await assert_async_script_error_contract(async_driver_case.driver, async_driver_case.case)
