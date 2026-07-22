"""Shared vector-distance execution contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import assert_async_vector_contract, assert_sync_vector_contract


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_vector"), indirect=True)
def test_sync_vector_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync vector-capable drivers execute vector distance and similarity builders."""
    assert_sync_vector_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_vector"), indirect=True)
async def test_async_vector_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async vector-capable drivers execute vector distance and similarity builders."""
    await assert_async_vector_contract(async_capability_driver_case.driver, async_capability_driver_case.case)
