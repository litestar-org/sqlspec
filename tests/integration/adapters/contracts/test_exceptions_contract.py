"""Shared adapter exception-mapping contracts."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import EXCEPTION_VIOLATION_PARAMS, ExceptionViolationCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_exception_contract,
    assert_sync_exception_contract,
)


@pytest.mark.parametrize("violation", EXCEPTION_VIOLATION_PARAMS)
def test_sync_exception_contract(sync_driver_case: DriverCaseContext, violation: ExceptionViolationCase) -> None:
    """Sync drivers normalize constraint violations to shared sqlspec exceptions."""
    if not sync_driver_case.case.supports_exception_translation:
        pytest.skip(f"{sync_driver_case.case.adapter} does not surface structured constraint violations")
    assert_sync_exception_contract(sync_driver_case.driver, violation)


@pytest.mark.parametrize("violation", EXCEPTION_VIOLATION_PARAMS)
async def test_async_exception_contract(
    async_driver_case: DriverCaseContext, violation: ExceptionViolationCase
) -> None:
    """Async drivers normalize constraint violations to shared sqlspec exceptions."""
    if not async_driver_case.case.supports_exception_translation:
        pytest.skip(f"{async_driver_case.case.adapter} does not surface structured constraint violations")
    await assert_async_exception_contract(async_driver_case.driver, violation)
