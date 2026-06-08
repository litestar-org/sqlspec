"""Shared driver-feature contracts: folded driver-specific behaviors (COPY, SET-variable, native types)."""

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    assert_async_driver_features_contract,
    assert_sync_driver_features_contract,
)


def test_sync_driver_features_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run folded driver-feature proofs opted in via extra_assertions."""
    assert_sync_driver_features_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_driver_features_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run folded driver-feature proofs opted in via extra_assertions."""
    await assert_async_driver_features_contract(async_driver_case.driver, async_driver_case.case)
