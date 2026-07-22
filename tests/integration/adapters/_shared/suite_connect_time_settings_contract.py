"""SQLite-family connect-time settings contracts."""

from typing import cast

import pytest

from tests.integration.adapters._shared._cases import DriverCaseContext, get_driver_case
from tests.integration.adapters._shared.behaviors import (
    AsyncConfigFactory,
    SyncConfigFactory,
    assert_async_connect_time_settings_contract,
    assert_sync_connect_time_settings_contract,
)

SQLITE_SYNC_CASE = get_driver_case("sqlite-sync")
AIOSQLITE_ASYNC_CASE = get_driver_case("aiosqlite-async")


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case",
    (pytest.param(SQLITE_SYNC_CASE, id=SQLITE_SYNC_CASE.id, marks=SQLITE_SYNC_CASE.marks),),
    indirect=True,
)
def test_sync_connect_time_settings_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """SQLite exposes its connect-time settings without fixture overrides."""
    assert sync_lifecycle_driver_case.make_config is not None
    assert_sync_connect_time_settings_contract(
        cast("SyncConfigFactory", sync_lifecycle_driver_case.make_config), sync_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "async_lifecycle_driver_case",
    (pytest.param(AIOSQLITE_ASYNC_CASE, id=AIOSQLITE_ASYNC_CASE.id, marks=AIOSQLITE_ASYNC_CASE.marks),),
    indirect=True,
)
async def test_async_connect_time_settings_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Aiosqlite exposes its connect-time settings without fixture overrides."""
    assert async_lifecycle_driver_case.make_config is not None
    await assert_async_connect_time_settings_contract(
        cast("AsyncConfigFactory", async_lifecycle_driver_case.make_config), async_lifecycle_driver_case.case
    )
