"""Shared driver lifecycle contracts: connection pooling and on_connection_create hooks."""

from typing import cast

import pytest

from tests.integration.adapters.contracts._cases import DriverCaseContext
from tests.integration.adapters.contracts.behaviors import (
    AsyncConfigFactory,
    SyncConfigFactory,
    assert_async_connection_hook_contract,
    assert_async_pooling_contract,
    assert_sync_connection_hook_contract,
    assert_sync_pooling_contract,
)


def _sync_factory(context: DriverCaseContext) -> SyncConfigFactory:
    assert context.make_config is not None, (
        f"{context.case.adapter} declares lifecycle support without a config factory"
    )
    return cast("SyncConfigFactory", context.make_config)


def _async_factory(context: DriverCaseContext) -> AsyncConfigFactory:
    assert context.make_config is not None, (
        f"{context.case.adapter} declares lifecycle support without a config factory"
    )
    return cast("AsyncConfigFactory", context.make_config)


def test_sync_pooling_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync pooled configs share data across sessions drawn from the same pool."""
    if not sync_driver_case.case.supports_pooling:
        pytest.skip(f"{sync_driver_case.case.adapter} has no verified pooling support")
    assert_sync_pooling_contract(_sync_factory(sync_driver_case), sync_driver_case.case)


async def test_async_pooling_contract(async_driver_case: DriverCaseContext) -> None:
    """Async pooled configs share data across sessions drawn from the same pool."""
    if not async_driver_case.case.supports_pooling:
        pytest.skip(f"{async_driver_case.case.adapter} has no verified pooling support")
    await assert_async_pooling_contract(_async_factory(async_driver_case), async_driver_case.case)


def test_sync_connection_hook_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync adapters invoke the on_connection_create driver-feature hook."""
    if not sync_driver_case.case.supports_connection_hook:
        pytest.skip(f"{sync_driver_case.case.adapter} has no verified connection-hook support")
    assert_sync_connection_hook_contract(_sync_factory(sync_driver_case), sync_driver_case.case)


async def test_async_connection_hook_contract(async_driver_case: DriverCaseContext) -> None:
    """Async adapters invoke the on_connection_create driver-feature hook."""
    if not async_driver_case.case.supports_connection_hook:
        pytest.skip(f"{async_driver_case.case.adapter} has no verified connection-hook support")
    await assert_async_connection_hook_contract(_async_factory(async_driver_case), async_driver_case.case)
