"""Shared Oracle LOB fetch-path contracts."""

import pytest
from _pytest.mark.structures import ParameterSet

from tests.integration.adapters.contracts._cases import DriverCaseContext, get_driver_case
from tests.integration.adapters.contracts.behaviors import (
    assert_async_oracle_lob_fetch_contract,
    assert_sync_oracle_lob_fetch_contract,
)


def _case_param(case_id: str) -> ParameterSet:
    case = get_driver_case(case_id)
    return pytest.param(case, id=case.id, marks=case.marks)


@pytest.mark.parametrize("sync_driver_case", (_case_param("oracledb-sync"),), indirect=True)
def test_sync_oracle_lob_fetch_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync Oracle proves metadata-driven LOB/JSON fetches through the shared matrix."""
    assert_sync_oracle_lob_fetch_contract(sync_driver_case.driver, sync_driver_case.case)


@pytest.mark.parametrize("async_driver_case", (_case_param("oracledb-async"),), indirect=True)
async def test_async_oracle_lob_fetch_contract(async_driver_case: DriverCaseContext) -> None:
    """Async Oracle proves metadata-driven LOB/JSON fetches through the shared matrix."""
    await assert_async_oracle_lob_fetch_contract(async_driver_case.driver, async_driver_case.case)
