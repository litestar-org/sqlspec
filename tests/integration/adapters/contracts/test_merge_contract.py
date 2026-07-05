"""Shared MERGE builder runtime contracts."""

import pytest

from tests.integration.adapters.contracts._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts.behaviors import (
    assert_async_merge_bulk_contract,
    assert_async_merge_contract,
    assert_sync_merge_bulk_contract,
    assert_sync_merge_contract,
)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_merge"), indirect=True)
def test_sync_merge_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers that opt into MERGE cover update, insert, expression, NULL, and table-source paths."""
    assert_sync_merge_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_merge"), indirect=True)
async def test_async_merge_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers that opt into MERGE cover update, insert, expression, NULL, and table-source paths."""
    await assert_async_merge_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_merge_bulk"), indirect=True)
def test_sync_merge_bulk_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers that opt into bulk MERGE cover bulk source expansion and mixed upsert paths."""
    assert_sync_merge_bulk_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_merge_bulk"), indirect=True)
async def test_async_merge_bulk_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers that opt into bulk MERGE cover bulk source expansion and mixed upsert paths."""
    await assert_async_merge_bulk_contract(async_capability_driver_case.driver, async_capability_driver_case.case)
