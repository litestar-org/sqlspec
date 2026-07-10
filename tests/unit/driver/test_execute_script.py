"""Unit tests for execute_script behavior in driver base classes."""

from typing import Any

from sqlspec.adapters.sqlite.core import default_statement_config
from sqlspec.core import SQL
from tests.conftest import requires_interpreted


def test_as_script_embeds_parameters_statically() -> None:
    """as_script() should force static parameter embedding regardless of the adapter default."""
    assert default_statement_config.parameter_config.needs_static_script_compilation is False

    sql = SQL(
        "INSERT INTO t (a) VALUES (:a); INSERT INTO t (a) VALUES (:a)", a=5, statement_config=default_statement_config
    ).as_script()
    compiled_sql, params = sql.compile()

    assert params is None
    assert ":a" not in compiled_sql
    assert "5" in compiled_sql


def test_as_script_embeds_distinct_qmark_values_across_statements() -> None:
    """A flat script-wide payload embeds each qmark occurrence and leaves no driver parameters."""
    statement = SQL(
        "INSERT INTO t (name, value) VALUES (?, ?); "
        "INSERT INTO t (name, value) VALUES (?, ?); "
        "UPDATE t SET value = ? WHERE name = ?",
        ("embed-one", 10, "embed-two", 20, 99, "embed-two"),
        statement_config=default_statement_config,
    ).as_script()

    compiled_sql, parameters = statement.compile()

    assert parameters is None
    assert "?" not in compiled_sql
    assert "VALUES ('embed-one', 10)" in compiled_sql
    assert "VALUES ('embed-two', 20)" in compiled_sql
    assert "SET value = 99 WHERE name = 'embed-two'" in compiled_sql


@requires_interpreted
def test_sync_execute_script_tracks_all_successful_statements(sqlite_sync_driver) -> None:
    """Sync execute_script should report all statements as successful."""
    result = sqlite_sync_driver.execute_script("SELECT * FROM users; SELECT * FROM users; SELECT * FROM users;")
    assert result.total_statements == 3
    assert result.successful_statements == 3
    assert result.is_success() is True


@requires_interpreted
def test_sync_execute_script_with_parameters_runs_every_statement(sqlite_sync_driver) -> None:
    """A parameterized multi-statement script embeds its params and lands every row."""
    result = sqlite_sync_driver.execute_script(
        "INSERT INTO users (name) VALUES (:n); INSERT INTO users (name) VALUES (:n)", n="scripted"
    )
    assert result.successful_statements == 2

    inserted = sqlite_sync_driver.execute("SELECT COUNT(*) FROM users WHERE name = :n", n="scripted").data
    assert inserted[0][0] == 2


@requires_interpreted
async def test_async_execute_script_with_parameters_runs_every_statement(aiosqlite_async_driver) -> None:
    """A parameterized multi-statement script embeds its params and lands every row on async drivers."""
    result = await aiosqlite_async_driver.execute_script(
        "INSERT INTO users (name) VALUES (:n); INSERT INTO users (name) VALUES (:n)", n="scripted"
    )
    assert result.successful_statements == 2

    inserted = (await aiosqlite_async_driver.execute("SELECT COUNT(*) FROM users WHERE name = :n", n="scripted")).data
    assert inserted[0][0] == 2


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


def test_dispatch_script_no_double_prepare_sub_statement_returns_processed_cache_direct_sql(
    sqlite_sync_driver: Any,
) -> None:
    sub_statement = sqlite_sync_driver._sub_statement("SELECT ?", (1,))
    assert isinstance(sub_statement, SQL)
    assert sub_statement.is_processed is True
    assert getattr(sub_statement, "_is_cache_direct") is True
    assert sub_statement.get_processed_state().execution_parameters == (1,)
