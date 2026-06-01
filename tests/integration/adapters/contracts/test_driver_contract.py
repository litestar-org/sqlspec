"""Shared driver behavior contracts."""

from typing import cast

from sqlspec import SQLResult
from sqlspec.adapters.aiosqlite import AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteDriver
from tests.integration.adapters.contracts._cases import DriverCaseContext


def test_sync_driver_execute_many_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers insert batches and return ordered rows consistently."""
    driver = cast("SqliteDriver", sync_driver_case.driver)

    result = driver.execute_many(
        "INSERT INTO contract_items (name, value) VALUES (?, ?)", [("alpha", 10), ("beta", 20), ("gamma", 30)]
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    rows_result = driver.execute("SELECT name, value FROM contract_items ORDER BY value")
    assert isinstance(rows_result, SQLResult)
    assert rows_result.get_data() == [
        {"name": "alpha", "value": 10},
        {"name": "beta", "value": 20},
        {"name": "gamma", "value": 30},
    ]


async def test_async_driver_execute_many_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers insert batches and return ordered rows consistently."""
    driver = cast("AiosqliteDriver", async_driver_case.driver)

    result = await driver.execute_many(
        "INSERT INTO contract_items (name, value) VALUES (?, ?)", [("alpha", 10), ("beta", 20), ("gamma", 30)]
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    rows_result = await driver.execute("SELECT name, value FROM contract_items ORDER BY value")
    assert isinstance(rows_result, SQLResult)
    assert rows_result.get_data() == [
        {"name": "alpha", "value": 10},
        {"name": "beta", "value": 20},
        {"name": "gamma", "value": 30},
    ]


def test_driver_case_metadata_resolves_fixture(driver_case: DriverCaseContext) -> None:
    """Every driver case resolves by fixture name and carries required metadata."""
    assert driver_case.driver is not None
    assert driver_case.case.adapter
    assert driver_case.case.dialect
    assert driver_case.case.fixture_name
    assert driver_case.case.supports_execute_many
