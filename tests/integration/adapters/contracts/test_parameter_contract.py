"""Shared parameter handling contracts for adapter integration tests."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import PARAMETER_PROFILE_PARAMS, ParameterProfileCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_parameter_contract,
    assert_sync_parameter_contract,
)


@pytest.mark.parametrize("parameter_case", PARAMETER_PROFILE_PARAMS)
def test_sync_parameter_contract(sync_driver_case: DriverCaseContext, parameter_case: ParameterProfileCase) -> None:
    """Sync drivers bind supported parameter styles consistently."""
    assert_sync_parameter_contract(sync_driver_case.driver, sync_driver_case.case, parameter_case)


@pytest.mark.parametrize("parameter_case", PARAMETER_PROFILE_PARAMS)
async def test_async_parameter_contract(
    async_driver_case: DriverCaseContext, parameter_case: ParameterProfileCase
) -> None:
    """Async drivers bind supported parameter styles consistently."""
    await assert_async_parameter_contract(async_driver_case.driver, async_driver_case.case, parameter_case)
