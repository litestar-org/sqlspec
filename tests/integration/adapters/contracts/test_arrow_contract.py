"""Shared adapter Arrow contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_arrow_contract,
    assert_async_arrow_extras_contract,
    assert_async_arrow_polars_contract,
    assert_sync_arrow_contract,
    assert_sync_arrow_extras_contract,
    assert_sync_arrow_polars_contract,
)


def test_sync_arrow_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers return Arrow tables, batches, filtered, and empty results."""
    assert_sync_arrow_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_arrow_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers return Arrow tables, batches, filtered, and empty results."""
    await assert_async_arrow_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_arrow_extras_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync Arrow output preserves NULLs and scales to a large result set."""
    assert_sync_arrow_extras_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_arrow_extras_contract(async_driver_case: DriverCaseContext) -> None:
    """Async Arrow output preserves NULLs and scales to a large result set."""
    await assert_async_arrow_extras_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_arrow_polars_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync Arrow results convert to a Polars DataFrame."""
    assert_sync_arrow_polars_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_arrow_polars_contract(async_driver_case: DriverCaseContext) -> None:
    """Async Arrow results convert to a Polars DataFrame."""
    await assert_async_arrow_polars_contract(async_driver_case.driver, async_driver_case.case)
