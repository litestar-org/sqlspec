"""Shared parameter style contracts for adapter integration tests."""

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared._inputs import (
    PARAMETER_STYLE_EXECUTE_MANY_PARAMS,
    PARAMETER_STYLE_EXECUTE_PARAMS,
    ParameterStyleCase,
)
from tests.integration.adapters._shared.behaviors import (
    assert_async_parameter_style_contract,
    assert_sync_parameter_style_contract,
)


@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_EXECUTE_PARAMS)
def test_sync_parameter_style_execute_contract(
    sync_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Sync drivers bind common execute parameter styles consistently."""
    assert_sync_parameter_style_contract(sync_driver_case.driver, sync_driver_case.case, parameter_style_case)


@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_EXECUTE_PARAMS)
async def test_async_parameter_style_execute_contract(
    async_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Async drivers bind common execute parameter styles consistently."""
    await assert_async_parameter_style_contract(async_driver_case.driver, async_driver_case.case, parameter_style_case)


@pytest.mark.parametrize("sync_capability_driver_case", sync_driver_params_with("supports_execute_many"), indirect=True)
@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_EXECUTE_MANY_PARAMS)
def test_sync_parameter_style_execute_many_contract(
    sync_capability_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Sync drivers bind common execute_many parameter styles consistently."""
    assert_sync_parameter_style_contract(
        sync_capability_driver_case.driver, sync_capability_driver_case.case, parameter_style_case
    )


@pytest.mark.parametrize(
    "async_capability_driver_case", async_driver_params_with("supports_execute_many"), indirect=True
)
@pytest.mark.parametrize("parameter_style_case", PARAMETER_STYLE_EXECUTE_MANY_PARAMS)
async def test_async_parameter_style_execute_many_contract(
    async_capability_driver_case: DriverCaseContext, parameter_style_case: ParameterStyleCase
) -> None:
    """Async drivers bind common execute_many parameter styles consistently."""
    await assert_async_parameter_style_contract(
        async_capability_driver_case.driver, async_capability_driver_case.case, parameter_style_case
    )
