"""Shared statement input contracts for adapter integration tests."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import STATEMENT_INPUT_PARAMS, StatementInputCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_statement_input_contract,
    assert_sync_statement_input_contract,
)


@pytest.mark.parametrize("statement_input_case", STATEMENT_INPUT_PARAMS)
def test_sync_statement_input_contract(
    sync_driver_case: DriverCaseContext, statement_input_case: StatementInputCase
) -> None:
    """Sync drivers execute every supported statement input shape consistently."""
    assert_sync_statement_input_contract(sync_driver_case.driver, sync_driver_case.case, statement_input_case)


@pytest.mark.parametrize("statement_input_case", STATEMENT_INPUT_PARAMS)
async def test_async_statement_input_contract(
    async_driver_case: DriverCaseContext, statement_input_case: StatementInputCase
) -> None:
    """Async drivers execute every supported statement input shape consistently."""
    await assert_async_statement_input_contract(async_driver_case.driver, async_driver_case.case, statement_input_case)
