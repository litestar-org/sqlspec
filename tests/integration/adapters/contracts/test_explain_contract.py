"""Shared adapter EXPLAIN contracts."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import EXPLAIN_PARAMS, ExplainCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_explain_contract,
    assert_async_explain_modifiers_contract,
    assert_sync_explain_contract,
    assert_sync_explain_modifiers_contract,
)


@pytest.mark.parametrize("explain_case", EXPLAIN_PARAMS)
def test_sync_explain_contract(sync_driver_case: DriverCaseContext, explain_case: ExplainCase) -> None:
    """Sync drivers execute EXPLAIN artifacts and return plan rows."""
    assert_sync_explain_contract(sync_driver_case.driver, sync_driver_case.case, explain_case)


@pytest.mark.parametrize("explain_case", EXPLAIN_PARAMS)
async def test_async_explain_contract(async_driver_case: DriverCaseContext, explain_case: ExplainCase) -> None:
    """Async drivers execute EXPLAIN artifacts and return plan rows."""
    await assert_async_explain_contract(async_driver_case.driver, async_driver_case.case, explain_case)


def test_sync_explain_modifiers_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run their folded dialect-specific EXPLAIN modifier proofs (analyze/format/verbose)."""
    assert_sync_explain_modifiers_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_explain_modifiers_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run their folded dialect-specific EXPLAIN modifier proofs (analyze/format/verbose)."""
    await assert_async_explain_modifiers_contract(async_driver_case.driver, async_driver_case.case)
