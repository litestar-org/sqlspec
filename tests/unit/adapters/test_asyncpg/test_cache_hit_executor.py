"""Unit tests for the asyncpg cache-hit executor."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.core import default_statement_config, execute_cache_hit
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import DatabaseConnectionError, QueryTimeoutError, UniqueViolationError


def _cached(compiled_sql: str, operation_type: Any, *, returns_rows: bool, param_count: int = 0) -> CachedQuery:
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
        param_count=param_count,
        processed_state=ProcessedState(
            compiled_sql=compiled_sql, execution_parameters=[], operation_type=operation_type
        ),
    )


async def test_execute_cache_hit_returns_records_and_column_names() -> None:
    """A row-returning statement yields the fetched records, their column names, and the row count."""
    connection = MagicMock()
    connection.fetch = AsyncMock(return_value=[{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}])
    sql = "SELECT id, name FROM items WHERE id > $1"
    cached = _cached(sql, "SELECT", returns_rows=True, param_count=1)

    result = await execute_cache_hit(connection, sql, (0,), cached, default_statement_config, None)

    assert result.get_data() == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 2
    connection.fetch.assert_awaited_once_with(sql, 0)


async def test_execute_cache_hit_returns_empty_result_for_no_records() -> None:
    """A row-returning statement with no records yields an empty result without column names."""
    connection = MagicMock()
    connection.fetch = AsyncMock(return_value=[])
    sql = "SELECT id FROM items"
    cached = _cached(sql, "SELECT", returns_rows=True)

    result = await execute_cache_hit(connection, sql, (), cached, default_statement_config, None)

    assert result.get_data() == []
    assert result.column_names == []
    assert result.rows_affected == 0


async def test_execute_cache_hit_parses_status_for_dml() -> None:
    """A statement without rows yields the affected-row count parsed from the command status."""
    connection = MagicMock()
    connection.execute = AsyncMock(return_value="UPDATE 3")
    sql = "UPDATE items SET active = $1"
    cached = _cached(sql, "UPDATE", returns_rows=False, param_count=1)

    result = await execute_cache_hit(connection, sql, [False], cached, default_statement_config, None)

    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    connection.execute.assert_awaited_once_with(sql, False)


@pytest.mark.parametrize(
    ("execution_args", "expected_timeout"),
    [({"timeout": 5}, 5.0), ({"command_timeout": "2.5"}, 2.5)],
    ids=["timeout", "command_timeout"],
)
async def test_execute_cache_hit_passes_configured_timeout(
    execution_args: "dict[str, Any]", expected_timeout: float
) -> None:
    """A timeout configured in the statement execution arguments is passed to the connection call."""
    connection = MagicMock()
    connection.fetch = AsyncMock(return_value=[{"value": 1}])
    connection.execute = AsyncMock(return_value="DELETE 1")
    config = default_statement_config.replace(execution_args=execution_args)

    await execute_cache_hit(connection, "SELECT 1", (), _cached("SELECT 1", "SELECT", returns_rows=True), config, None)
    await execute_cache_hit(
        connection, "DELETE FROM t", (), _cached("DELETE FROM t", "DELETE", returns_rows=False), config, None
    )

    connection.fetch.assert_awaited_once_with("SELECT 1", timeout=expected_timeout)
    connection.execute.assert_awaited_once_with("DELETE FROM t", timeout=expected_timeout)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (asyncpg.exceptions.UniqueViolationError("duplicate key"), UniqueViolationError),
        (TimeoutError("timed out"), QueryTimeoutError),
        (ConnectionResetError("reset"), DatabaseConnectionError),
        (asyncpg.InterfaceError("closed"), DatabaseConnectionError),
    ],
    ids=["postgres", "timeout", "transport", "interface"],
)
async def test_execute_cache_hit_maps_driver_errors(error: Exception, expected: "type[Exception]") -> None:
    """Errors the asyncpg exception handler maps are raised as their SQLSpec exception with the cause kept."""
    connection = MagicMock()
    connection.execute = AsyncMock(side_effect=error)
    sql = "INSERT INTO items (id) VALUES ($1)"
    cached = _cached(sql, "INSERT", returns_rows=False, param_count=1)

    with pytest.raises(expected) as exc_info:
        await execute_cache_hit(connection, sql, (1,), cached, default_statement_config, None)

    assert exc_info.value.__cause__ is error


async def test_execute_cache_hit_propagates_unmapped_errors() -> None:
    """An error outside the mapped set propagates unchanged."""
    connection = MagicMock()
    error = ValueError("bad argument")
    connection.fetch = AsyncMock(side_effect=error)
    cached = _cached("SELECT 1", "SELECT", returns_rows=True)

    with pytest.raises(ValueError, match="bad argument") as exc_info:
        await execute_cache_hit(connection, "SELECT 1", (), cached, default_statement_config, None)

    assert exc_info.value is error
