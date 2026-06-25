"""Shared adapter native bulk-ingest contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_load_from_records_contract,
    assert_async_native_bulk_ingest_contract,
    assert_sync_load_from_records_contract,
    assert_sync_native_bulk_ingest_contract,
)


def test_sync_native_bulk_ingest_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers honor row-count fidelity, overwrite/append, and error surfacing."""
    assert_sync_native_bulk_ingest_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_native_bulk_ingest_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers honor row-count fidelity, overwrite/append, and error surfacing."""
    await assert_async_native_bulk_ingest_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_load_from_records_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers ingest dict + positional records and validate input."""
    assert_sync_load_from_records_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_load_from_records_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers ingest dict + positional records and validate input."""
    await assert_async_load_from_records_contract(async_driver_case.driver, async_driver_case.case)
