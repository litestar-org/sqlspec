"""Shared adapter execute_many contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_execute_many_input_contract,
    assert_async_execute_many_mutation_contract,
    assert_async_execute_many_specifics_contract,
    assert_sync_execute_many_input_contract,
    assert_sync_execute_many_mutation_contract,
    assert_sync_execute_many_specifics_contract,
)


def test_sync_execute_many_mutation_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers batch insert, update, and delete with accurate row counts."""
    assert_sync_execute_many_mutation_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_execute_many_mutation_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers batch insert, update, and delete with accurate row counts."""
    await assert_async_execute_many_mutation_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_execute_many_input_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers batch a large sequence and an is_many SQL object."""
    assert_sync_execute_many_input_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_execute_many_input_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers batch a large sequence and an is_many SQL object."""
    await assert_async_execute_many_input_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_execute_many_specifics_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run folded driver-specific execute_many proofs (arrays/JSON/edge cases)."""
    assert_sync_execute_many_specifics_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_execute_many_specifics_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run folded driver-specific execute_many proofs (arrays/JSON/edge cases)."""
    await assert_async_execute_many_specifics_contract(async_driver_case.driver, async_driver_case.case)
