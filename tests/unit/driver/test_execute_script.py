"""Unit tests for execute_script behavior in driver base classes."""

from typing import Any

from sqlspec.core import SQL
from tests.conftest import requires_interpreted


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


def test_dispatch_script_no_double_prepare_dispatch_execute_script_prepares_parameters_once(
    sqlite_sync_driver: Any, monkeypatch: Any
) -> None:
    calls = 0
    original_prepare = sqlite_sync_driver.prepare_driver_parameters

    def counted_prepare_driver_parameters(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(sqlite_sync_driver, "prepare_driver_parameters", counted_prepare_driver_parameters)
    result = sqlite_sync_driver.execute_script("SELECT 1; SELECT 2; SELECT 3;")
    assert calls == 1
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


def test_dispatch_script_no_double_prepare_build_direct_sub_statement_returns_processed_cache_direct_sql(
    sqlite_sync_driver: Any,
) -> None:
    sub_statement = sqlite_sync_driver._build_direct_sub_statement("SELECT ?", (1,))
    assert isinstance(sub_statement, SQL)
    assert sub_statement.is_processed is True
    assert getattr(sub_statement, "_is_cache_direct") is True
    assert sub_statement.get_processed_state().execution_parameters == (1,)
