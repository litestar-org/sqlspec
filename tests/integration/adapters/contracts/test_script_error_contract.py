"""Shared script execution and error mapping contracts for adapter integration tests."""

from typing import Any

import pytest

from sqlspec import SQL, SQLResult
from tests.integration.adapters.contracts._cases import (
    DriverCase,
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts._schema import build_bigquery_contract_table
from tests.integration.adapters.contracts.behaviors import (
    assert_async_script_error_contract,
    assert_async_script_parameter_embedding_contract,
    assert_sync_script_error_contract,
    assert_sync_script_parameter_embedding_contract,
)


class _ScriptRecordingDriver:
    __slots__ = ("scripts",)

    def __init__(self) -> None:
        self.scripts: list[str] = []

    def execute_script(self, script: str) -> SQLResult:
        self.scripts.append(script)
        return SQLResult(statement=SQL(script), operation_type="SCRIPT")

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> SQLResult:
        del args, kwargs
        return SQLResult(
            statement=SQL(statement),
            data=[{"name": "script1", "value": 30}, {"name": "script2", "value": 20}],
            operation_type="SELECT",
        )


def test_sync_script_error_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers execute scripts and map generic SQL errors consistently."""
    assert_sync_script_error_contract(sync_driver_case.driver, sync_driver_case.case)


def test_bigquery_sync_script_contract_matches_emulator_safe_main_shape() -> None:
    """BigQuery script contract mirrors the adapter-local script proof from main."""
    driver = _ScriptRecordingDriver()
    case = DriverCase(
        id="bigquery-sync",
        fixture_name="contract_bigquery_driver",
        adapter="bigquery",
        dialect="bigquery",
        mode="sync",
        table=build_bigquery_contract_table("contract_items"),
        invalid_sql_error_policy="emulator_retries",
    )

    assert_sync_script_error_contract(driver, case)

    script = driver.scripts[0]
    assert "INSERT INTO contract_items (name, value)" in script
    assert "note" not in script
    assert "NULL" not in script


async def test_async_script_error_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers execute scripts and map generic SQL errors consistently."""
    await assert_async_script_error_contract(async_driver_case.driver, async_driver_case.case)


@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_execute_script"), indirect=True
)
def test_sync_script_parameter_embedding_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync script-capable drivers embed one flat payload across all statements."""
    assert_sync_script_parameter_embedding_contract(
        sync_capability_driver_case.driver, sync_capability_driver_case.case
    )


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_script"), indirect=True
)
async def test_async_script_parameter_embedding_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async script-capable drivers embed one flat payload across all statements."""
    await assert_async_script_parameter_embedding_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
