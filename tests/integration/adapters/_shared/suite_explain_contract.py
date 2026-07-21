"""Shared adapter EXPLAIN contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared._inputs import EXPLAIN_PARAMS, ExplainCase
from tests.integration.adapters._shared.behaviors import (
    assert_async_explain_contract,
    assert_async_explain_modifiers_contract,
    assert_sync_explain_contract,
    assert_sync_explain_modifiers_contract,
)


@pytest.mark.parametrize("explain_case", EXPLAIN_PARAMS)
@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_explain"), indirect=True)
def test_sync_explain_contract(sync_capability_driver_case: DriverCaseContext, explain_case: ExplainCase) -> None:
    """Sync drivers execute EXPLAIN artifacts and return plan rows."""
    assert_sync_explain_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case, explain_case)


@pytest.mark.parametrize("explain_case", EXPLAIN_PARAMS)
@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_explain"), indirect=True)
async def test_async_explain_contract(
    async_capability_driver_case: DriverCaseContext, explain_case: ExplainCase
) -> None:
    """Async drivers execute EXPLAIN artifacts and return plan rows."""
    await assert_async_explain_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case, explain_case
    )


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_explain"), indirect=True)
def test_sync_explain_modifiers_contract(sync_capability_driver_case: DriverCaseContext) -> None:
    """Sync drivers run their folded dialect-specific EXPLAIN modifier proofs (analyze/format/verbose)."""
    assert_sync_explain_modifiers_contract(sync_capability_driver_case.driver, sync_capability_driver_case.case)


@pytest.mark.parametrize("async_capability_driver_case", async_driver_params_with("supports_explain"), indirect=True)
async def test_async_explain_modifiers_contract(async_capability_driver_case: DriverCaseContext) -> None:
    """Async drivers run their folded dialect-specific EXPLAIN modifier proofs (analyze/format/verbose)."""
    await assert_async_explain_modifiers_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case
    )
