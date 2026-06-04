"""Shared driver behavior contracts."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_driver_basics_contract,
    assert_async_execute_many_contract,
    assert_async_for_update_contract,
    assert_async_statement_stack_contract,
    assert_sync_driver_basics_contract,
    assert_sync_execute_many_contract,
    assert_sync_for_update_contract,
    assert_sync_statement_stack_contract,
)


def test_sync_driver_basics_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run the CRUD lifecycle and expose result column metadata."""
    assert_sync_driver_basics_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_driver_basics_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run the CRUD lifecycle and expose result column metadata."""
    await assert_async_driver_basics_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_driver_execute_many_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers insert batches and return ordered rows consistently."""
    assert_sync_execute_many_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_driver_execute_many_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers insert batches and return ordered rows consistently."""
    await assert_async_execute_many_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_statement_stack_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers execute a StatementStack sequentially and return per-operation results."""
    assert_sync_statement_stack_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_statement_stack_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers execute a StatementStack sequentially and return per-operation results."""
    await assert_async_statement_stack_contract(async_driver_case.driver, async_driver_case.case)


def test_sync_for_update_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers that support row locking honor FOR UPDATE / SKIP LOCKED / FOR SHARE."""
    assert_sync_for_update_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_for_update_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers that support row locking honor FOR UPDATE / SKIP LOCKED / FOR SHARE."""
    await assert_async_for_update_contract(async_driver_case.driver, async_driver_case.case)


def test_driver_case_metadata_resolves_fixture(driver_case: DriverCaseContext) -> None:
    """Every driver case resolves by fixture name and carries required metadata."""
    assert driver_case.driver is not None
    assert driver_case.case.adapter
    assert driver_case.case.dialect
    assert driver_case.case.fixture_name
    assert isinstance(driver_case.case.supports_execute_many, bool)
