"""Shared adapter complex-query and statement-filter contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
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


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_grouped_subquery"), indirect=True
)
def test_sync_complex_query_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers run grouped aggregation and correlated subquery selects."""
    assert_sync_complex_query_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_grouped_subquery"), indirect=True
)
async def test_async_complex_query_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers run grouped aggregation and correlated subquery selects."""
    await assert_async_complex_query_contract(async_capability_driver_case.driver, async_capability_driver_case.case)
