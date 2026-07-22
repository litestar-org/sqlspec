"""Shared adapter storage-bridge contracts (local filesystem)."""

from pathlib import Path

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
    assert_async_storage_bridge_local_contract,
    assert_sync_storage_bridge_local_contract,
)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_storage_bridge"), indirect=True
)
def test_sync_storage_bridge_local_contract(sync_capability_driver_case: DriverCaseContext, tmp_path: Path) -> None:
    """Sync drivers round-trip Arrow and local parquet through the storage bridge."""
    assert_sync_storage_bridge_local_contract(
        sync_capability_driver_case.driver, sync_capability_driver_case.case, tmp_path
    )


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_storage_bridge"), indirect=True
)
async def test_async_storage_bridge_local_contract(
    async_capability_driver_case: DriverCaseContext, tmp_path: Path
) -> None:
    """Async drivers round-trip Arrow and local parquet through the storage bridge."""
    await assert_async_storage_bridge_local_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case, tmp_path
    )
