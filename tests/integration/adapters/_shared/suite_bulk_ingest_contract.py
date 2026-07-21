"""Shared adapter native bulk-ingest contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
    assert_async_load_from_records_contract,
    assert_async_native_bulk_ingest_contract,
    assert_sync_load_from_records_contract,
    assert_sync_native_bulk_ingest_contract,
)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_native_bulk_ingest"), indirect=True
)
def test_sync_native_bulk_ingest_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers honor row-count fidelity, overwrite/append, and error surfacing."""
    assert_sync_native_bulk_ingest_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_native_bulk_ingest"), indirect=True
)
async def test_async_native_bulk_ingest_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers honor row-count fidelity, overwrite/append, and error surfacing."""
    await assert_async_native_bulk_ingest_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_load_from_records"), indirect=True
)
def test_sync_load_from_records_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers ingest dict + positional records and validate input."""
    assert_sync_load_from_records_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_load_from_records"), indirect=True
)
async def test_async_load_from_records_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers ingest dict + positional records and validate input."""
    await assert_async_load_from_records_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
