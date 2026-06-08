"""Shared parameter style contracts for adapter integration tests."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import PARAMETER_STYLE_PARAMS, ParameterStyleCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_parameter_style_contract,
    assert_sync_parameter_style_contract,
)


@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_PARAMS)
def test_sync_parameter_style_contract(
    sync_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Sync drivers bind common parameter styles consistently."""
    assert_sync_parameter_style_contract(sync_driver_case.driver, sync_driver_case.case, parameter_style_case)


@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_PARAMS)
async def test_async_parameter_style_contract(
    async_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Async drivers bind common parameter styles consistently."""
    await assert_async_parameter_style_contract(async_driver_case.driver, async_driver_case.case, parameter_style_case)
