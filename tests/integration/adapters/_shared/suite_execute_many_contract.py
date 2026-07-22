"""Shared adapter execute_many contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
    assert_async_execute_many_empty_contract,
    assert_async_execute_many_input_contract,
    assert_async_execute_many_mutation_contract,
    assert_async_execute_many_specifics_contract,
    assert_sync_execute_many_empty_contract,
    assert_sync_execute_many_input_contract,
    assert_sync_execute_many_mutation_contract,
    assert_sync_execute_many_specifics_contract,
)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
def test_sync_execute_many_empty_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers treat an empty batch as a no-op with a zero row count."""
    assert_sync_execute_many_empty_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
async def test_async_execute_many_empty_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers treat an empty batch as a no-op with a zero row count."""
    await assert_async_execute_many_empty_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
def test_sync_execute_many_mutation_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers batch insert, update, and delete with accurate row counts."""
    assert_sync_execute_many_mutation_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
async def test_async_execute_many_mutation_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers batch insert, update, and delete with accurate row counts."""
    await assert_async_execute_many_mutation_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
def test_sync_execute_many_input_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers batch a large sequence and an is_many SQL object."""
    assert_sync_execute_many_input_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
async def test_async_execute_many_input_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers batch a large sequence and an is_many SQL object."""
    await assert_async_execute_many_input_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
def test_sync_execute_many_specifics_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers run folded driver-specific execute_many proofs (arrays/JSON/edge cases)."""
    assert_sync_execute_many_specifics_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
async def test_async_execute_many_specifics_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers run folded driver-specific execute_many proofs (arrays/JSON/edge cases)."""
    await assert_async_execute_many_specifics_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
