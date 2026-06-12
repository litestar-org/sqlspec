"""Shared adapter row-streaming contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_streaming_contract,
    assert_async_streaming_unsupported_contract,
    assert_sync_streaming_contract,
    assert_sync_streaming_unsupported_contract,
)


def test_sync_streaming_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers stream dict rows in bounded chunks with cleanup guarantees."""
    assert_sync_streaming_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_streaming_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers stream dict rows in bounded chunks with cleanup guarantees."""
    await assert_async_streaming_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_streaming_unsupported_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers without native streaming raise unless eager fallback is requested."""
    assert_sync_streaming_unsupported_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_streaming_unsupported_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers without native streaming raise unless eager fallback is requested."""
    await assert_async_streaming_unsupported_contract(async_driver_case.driver, async_driver_case.case)
