"""Shared nested transaction block contracts."""

from tests.integration.adapters._shared._cases import DriverCaseContext
from tests.integration.adapters._shared.transaction_behaviors import (
    assert_async_nested_transaction_contract,
    assert_sync_nested_transaction_contract,
)


def test_sync_nested_transaction_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync services and drivers nest transaction blocks through savepoints or refuse clearly."""
    assert_sync_nested_transaction_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_nested_transaction_contract(async_driver_case: DriverCaseContext) -> None:
    """Async services and drivers nest transaction blocks through savepoints or refuse clearly."""
    await assert_async_nested_transaction_contract(async_driver_case.driver, async_driver_case.case)
