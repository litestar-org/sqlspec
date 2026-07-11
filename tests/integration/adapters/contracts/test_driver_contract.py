"""Shared driver behavior contracts."""

import pytest

from tests.integration.adapters.contracts._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts.behaviors import (
    assert_async_driver_basics_contract,
    assert_async_execute_many_contract,
    assert_async_for_update_contract,
    assert_async_savepoint_round_trip_contract,
    assert_async_savepoint_unsafe_name_contract,
    assert_async_statement_stack_contract,
    assert_async_transaction_semantics_contract,
    assert_sync_driver_basics_contract,
    assert_sync_execute_many_contract,
    assert_sync_for_update_contract,
    assert_sync_savepoint_round_trip_contract,
    assert_sync_savepoint_unsafe_name_contract,
    assert_sync_statement_stack_contract,
    assert_sync_transaction_semantics_contract,
)


def test_sync_driver_basics_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run the CRUD lifecycle and expose result column metadata."""
    assert_sync_driver_basics_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_driver_basics_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run the CRUD lifecycle and expose result column metadata."""
    await assert_async_driver_basics_contract(async_driver_case.driver, async_driver_case.case)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
def test_sync_driver_execute_many_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers insert batches and return ordered rows consistently."""
    assert_sync_execute_many_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
async def test_async_driver_execute_many_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers insert batches and return ordered rows consistently."""
    await assert_async_execute_many_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


def test_sync_statement_stack_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers execute a StatementStack sequentially and return per-operation results."""
    assert_sync_statement_stack_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_statement_stack_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers execute a StatementStack sequentially and return per-operation results."""
    await assert_async_statement_stack_contract(async_driver_case.driver, async_driver_case.case)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_transactions"), indirect=True)
def test_sync_transaction_semantics_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers roll back caller work, commit durable work, and preserve outer stack ownership."""
    assert_sync_transaction_semantics_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_transactions"), indirect=True
)
async def test_async_transaction_semantics_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers roll back caller work, commit durable work, and preserve outer stack ownership."""
    await assert_async_transaction_semantics_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_savepoints"), indirect=True)
def test_sync_savepoint_unsafe_name_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers reject unsafe savepoint names before sending SQL to the database."""
    assert_sync_savepoint_unsafe_name_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_savepoints"), indirect=True)
async def test_async_savepoint_unsafe_name_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers reject unsafe savepoint names before sending SQL to the database."""
    await assert_async_savepoint_unsafe_name_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_savepoints"), indirect=True)
def test_sync_savepoint_round_trip_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers roll back only work after a savepoint and commit later work."""
    assert_sync_savepoint_round_trip_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_savepoints"), indirect=True)
async def test_async_savepoint_round_trip_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers roll back only work after a savepoint and commit later work."""
    await assert_async_savepoint_round_trip_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_for_update"), indirect=True)
def test_sync_for_update_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers that support row locking honor FOR UPDATE / SKIP LOCKED / FOR SHARE."""
    assert_sync_for_update_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_for_update"), indirect=True)
async def test_async_for_update_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers that support row locking honor FOR UPDATE / SKIP LOCKED / FOR SHARE."""
    await assert_async_for_update_contract(async_capability_driver_case.driver, async_capability_driver_case.case)


def test_driver_case_metadata_resolves_fixture(driver_case: DriverCaseContext) -> None:
    """Every driver case resolves by fixture name and carries required metadata."""
    assert driver_case.driver is not None
    assert driver_case.case.adapter
    assert driver_case.case.dialect
    assert driver_case.case.fixture_name
    assert driver_case.case.supports_execute_many or driver_case.case.supports_native_bulk_ingest
