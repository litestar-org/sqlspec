# pyright: reportPrivateUsage=false
"""Unit tests for execute_script behavior in driver base classes."""

import pytest

from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("driver")


@requires_interpreted
def test_sync_execute_script_tracks_all_successful_statements(sqlite_sync_driver) -> None:
    """Sync execute_script should report all statements as successful."""
    result = sqlite_sync_driver.execute_script("SELECT * FROM users; SELECT * FROM users; SELECT * FROM users;")
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


@requires_interpreted
async def test_async_execute_script_tracks_all_successful_statements(aiosqlite_async_driver) -> None:
    """Async execute_script should report all statements as successful."""
    result = await aiosqlite_async_driver.execute_script(
        "SELECT * FROM users; SELECT * FROM users; SELECT * FROM users;"
    )
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True
