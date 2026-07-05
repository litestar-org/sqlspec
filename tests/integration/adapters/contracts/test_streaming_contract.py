"""Shared adapter row-streaming contracts."""

import pytest

from tests.integration.adapters.contracts._cases import (
    DriverCaseContext,
    async_driver_params_with,
    async_driver_params_without,
    sync_driver_params_with,
    sync_driver_params_without,
)
from tests.integration.adapters.contracts.behaviors import (
    assert_async_streaming_contract,
    assert_async_streaming_unsupported_contract,
    assert_sync_streaming_contract,
    assert_sync_streaming_unsupported_contract,
)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_native_row_streaming"), indirect=True
)
def test_sync_streaming_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers stream dict rows in bounded chunks with cleanup guarantees."""
    assert_sync_streaming_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_native_row_streaming"), indirect=True
)
async def test_async_streaming_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers stream dict rows in bounded chunks with cleanup guarantees."""
    await assert_async_streaming_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_without("supports_native_row_streaming"), indirect=True
)
def test_sync_streaming_unsupported_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers without native streaming raise unless eager fallback is requested."""
    assert_sync_streaming_unsupported_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_without("supports_native_row_streaming"), indirect=True
)
async def test_async_streaming_unsupported_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers without native streaming raise unless eager fallback is requested."""
    await assert_async_streaming_unsupported_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
