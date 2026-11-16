"""Unit tests for the CockroachDB asyncpg cache-hit executor."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from sqlspec.adapters.cockroach_asyncpg.core import default_statement_config, execute_cache_hit
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import DatabaseConnectionError, QueryTimeoutError, SerializationConflictError


def _cached(compiled_sql: str, operation_type: Any, *, returns_rows: bool, param_count: int = 0) -> CachedQuery:
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type=operation_type,
        operation_profile=OperationProfile(
            returns_rows=returns_rows, modifies_rows=not returns_rows, is_read_only=returns_rows
        ),
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

    result = await execute_cache_hit(connection, sql, (0,), cached, default_statement_config, {})

    assert result is not None
    assert result.get_data() == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    assert result.column_names == ["id", "name"]
    assert result.rows_affected == 2
    connection.fetch.assert_awaited_once_with(sql, 0)


@pytest.mark.parametrize(
    ("status", "expected_rows"),
    [("UPDATE 3", 3), ("INSERT 0 2", 2), ("CREATE TABLE", 0), (None, 0)],
    ids=["update", "insert", "ddl", "missing"],
)
async def test_execute_cache_hit_parses_status_for_dml(status: "str | None", expected_rows: int) -> None:
    """A statement without rows yields the affected-row count parsed from the command status."""
    connection = MagicMock()
    connection.execute = AsyncMock(return_value=status)
    sql = "UPDATE items SET active = $1"
    cached = _cached(sql, "UPDATE", returns_rows=False, param_count=1)

    result = await execute_cache_hit(connection, sql, [False], cached, default_statement_config, {})

    assert result is not None
    assert result.rows_affected == expected_rows
    assert result.operation_type == "UPDATE"
    connection.execute.assert_awaited_once_with(sql, False)


async def test_execute_cache_hit_passes_configured_timeout() -> None:
    """A timeout configured in the statement execution arguments is passed to the connection call."""
    connection = MagicMock()
    connection.fetch = AsyncMock(return_value=[{"value": 1}])
    config = default_statement_config.replace(execution_args={"command_timeout": 4})

    await execute_cache_hit(connection, "SELECT 1", (), _cached("SELECT 1", "SELECT", returns_rows=True), config, {})

    connection.fetch.assert_awaited_once_with("SELECT 1", timeout=4.0)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (asyncpg.exceptions.SerializationError("restart transaction"), SerializationConflictError),
        (TimeoutError("timed out"), QueryTimeoutError),
        (ConnectionResetError("reset"), DatabaseConnectionError),
        (asyncpg.InterfaceError("closed"), DatabaseConnectionError),
    ],
    ids=["postgres", "timeout", "transport", "interface"],
)
async def test_execute_cache_hit_maps_driver_errors(error: Exception, expected: "type[Exception]") -> None:
    """Errors the CockroachDB asyncpg exception handler maps are raised as their SQLSpec exception."""
    connection = MagicMock()
    connection.execute = AsyncMock(side_effect=error)
    sql = "INSERT INTO items (id) VALUES ($1)"
    cached = _cached(sql, "INSERT", returns_rows=False, param_count=1)

    with pytest.raises(expected) as exc_info:
        await execute_cache_hit(connection, sql, (1,), cached, default_statement_config, {})

    assert exc_info.value.__cause__ is error


async def test_execute_cache_hit_propagates_unmapped_errors() -> None:
    """An error outside the mapped set propagates unchanged."""
    connection = MagicMock()
    error = ValueError("bad argument")
    connection.fetch = AsyncMock(side_effect=error)
    cached = _cached("SELECT 1", "SELECT", returns_rows=True)

    with pytest.raises(ValueError, match="bad argument") as exc_info:
        await execute_cache_hit(connection, "SELECT 1", (), cached, default_statement_config, {})

    assert exc_info.value is error


@pytest.mark.parametrize(
    ("driver_features", "returns_rows"),
    [
        ({"read_only_session": True}, True),
        ({"read_only_session": True}, False),
        ({"enable_follower_reads": True}, True),
    ],
    ids=["read-only-session-read", "read-only-session-write", "follower-read"],
)
async def test_execute_cache_hit_declines_follower_and_read_only_sessions(
    driver_features: "dict[str, Any]", returns_rows: bool
) -> None:
    """Statements that need a follower-read transaction or a read-only check are declined untouched."""
    connection = MagicMock()
    connection.fetch = AsyncMock()
    connection.execute = AsyncMock()
    cached = _cached("SELECT 1", "SELECT" if returns_rows else "UPDATE", returns_rows=returns_rows)

    result = await execute_cache_hit(connection, "SELECT 1", (), cached, default_statement_config, driver_features)

    assert result is None
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()


async def test_execute_cache_hit_runs_writes_on_follower_read_sessions() -> None:
    """A modifying statement on a follower-read session executes directly."""
    connection = MagicMock()
    connection.execute = AsyncMock(return_value="DELETE 4")
    cached = _cached("DELETE FROM items", "DELETE", returns_rows=False)

    result = await execute_cache_hit(
        connection, "DELETE FROM items", (), cached, default_statement_config, {"enable_follower_reads": True}
    )

    assert result is not None
    assert result.rows_affected == 4
