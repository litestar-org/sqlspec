"""Shared parameter handling contracts for adapter integration tests."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts._inputs import PARAMETER_PROFILE_PARAMS, ParameterProfileCase
from tests.integration.adapters.contracts.behaviors import (
    assert_async_param_codecs_contract,
    assert_async_parameter_contract,
    assert_sync_param_codecs_contract,
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


def test_sync_param_codecs_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run folded driver-specific parameter-codec proofs (arrays/JSON/type fidelity)."""
    assert_sync_param_codecs_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_param_codecs_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run folded driver-specific parameter-codec proofs (arrays/JSON/type fidelity)."""
    await assert_async_param_codecs_contract(async_driver_case.driver, async_driver_case.case)
