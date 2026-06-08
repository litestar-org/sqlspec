"""Shared adapter complex-query and statement-filter contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_complex_query_contract,
    assert_async_filter_contract,
    assert_sync_complex_query_contract,
    assert_sync_filter_contract,
)


def test_sync_filter_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers apply OrderBy/LimitOffset, InCollection, and Search filters."""
    assert_sync_filter_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_filter_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers apply OrderBy/LimitOffset, InCollection, and Search filters."""
    await assert_async_filter_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_complex_query_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run grouped aggregation and correlated subquery selects."""
    assert_sync_complex_query_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_complex_query_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run grouped aggregation and correlated subquery selects."""
    await assert_async_complex_query_contract(async_driver_case.driver, async_driver_case.case)
