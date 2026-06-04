"""Shared adapter storage-bridge contracts (local filesystem)."""

from pathlib import Path

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_storage_bridge_local_contract,
    assert_sync_storage_bridge_local_contract,
)


def test_sync_storage_bridge_local_contract(sync_driver_case: DriverCaseContext, tmp_path: Path) -> None:
    """Sync drivers round-trip Arrow and local parquet through the storage bridge."""
    assert_sync_storage_bridge_local_contract(sync_driver_case.driver, sync_driver_case.case, tmp_path)


async def test_async_storage_bridge_local_contract(async_driver_case: DriverCaseContext, tmp_path: Path) -> None:
    """Async drivers round-trip Arrow and local parquet through the storage bridge."""
    await assert_async_storage_bridge_local_contract(async_driver_case.driver, async_driver_case.case, tmp_path)
