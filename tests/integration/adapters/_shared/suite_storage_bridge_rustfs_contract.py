"""Shared adapter storage-bridge contracts (cloud / RustFS object storage)."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
    assert_async_storage_bridge_rustfs_contract,
    assert_sync_storage_bridge_rustfs_contract,
)

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService

pytestmark = [
    pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_storage_bridge"), indirect=True
)
def test_sync_storage_bridge_rustfs_contract(
    sync_capability_driver_case: DriverCaseContext, rustfs_service: "RustfsService", rustfs_bucket_name: str
) -> None:
    """Sync drivers round-trip a SELECT through RustFS object storage."""
    assert_sync_storage_bridge_rustfs_contract(
        sync_capability_driver_case.driver, sync_capability_driver_case.case, rustfs_service, rustfs_bucket_name
    )


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_storage_bridge"), indirect=True
)
async def test_async_storage_bridge_rustfs_contract(
    async_capability_driver_case: DriverCaseContext, rustfs_service: "RustfsService", rustfs_bucket_name: str
) -> None:
    """Async drivers round-trip a SELECT through RustFS object storage."""
    await assert_async_storage_bridge_rustfs_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case, rustfs_service, rustfs_bucket_name
    )
