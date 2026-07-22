"""Shared adapter exception-mapping contracts."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared._inputs import EXCEPTION_VIOLATION_PARAMS, ExceptionViolationCase
from tests.integration.adapters._shared.behaviors import assert_async_exception_contract, assert_sync_exception_contract


@pytest.mark.parametrize("violation", EXCEPTION_VIOLATION_PARAMS)
@pytest.mark.parametrize(
    "sync_capability_driver_case", sync_driver_params_with("supports_exception_translation"), indirect=True
)
def test_sync_exception_contract(
    sync_capability_driver_case: DriverCaseContext, violation: ExceptionViolationCase
) -> None:
    """Sync drivers normalize constraint violations to shared sqlspec exceptions."""
    assert_sync_exception_contract(sync_capability_driver_case.driver, violation)


@pytest.mark.parametrize("violation", EXCEPTION_VIOLATION_PARAMS)
@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_exception_translation"), indirect=True
)
async def test_async_exception_contract(
    async_capability_driver_case: DriverCaseContext, violation: ExceptionViolationCase
) -> None:
    """Async drivers normalize constraint violations to shared sqlspec exceptions."""
    await assert_async_exception_contract(async_capability_driver_case.driver, violation)
