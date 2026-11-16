"""Unit tests for the psqlpy cache-hit executor."""

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from psqlpy.exceptions import ConnectionExecuteError

from sqlspec.adapters.psqlpy import core as psqlpy_core
from sqlspec.adapters.psqlpy.core import default_statement_config, execute_cache_hit
from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
from sqlspec.core.cache import CachedQuery
from sqlspec.exceptions import SQLSpecError, UniqueViolationError

STATE: "tuple[dict[str, Any], bool]" = ({}, False)


def _cached(
    compiled_sql: str, operation_type: Any, *, returns_rows: bool, parameter_casts: "dict[int, str] | None" = None
) -> CachedQuery:
    return CachedQuery(
        compiled_sql=compiled_sql,
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts=parameter_casts or {},
        operation_type=operation_type,
        operation_profile=OperationProfile(returns_rows=returns_rows, modifies_rows=not returns_rows),
        param_count=0,
        processed_state=ProcessedState(
            compiled_sql=compiled_sql, execution_parameters=[], operation_type=operation_type
        ),
    )


def _rows(rows: "list[dict[str, Any]]") -> SimpleNamespace:
    return SimpleNamespace(result=lambda: rows)


async def test_execute_cache_hit_returns_dict_rows_and_column_names() -> None:
    """A row-returning statement yields the fetched dict rows, their column names, and the row count."""
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=_rows([{"id": 1, "score": 1.5}, {"id": 2, "score": 2.5}]))
    )
    sql = "SELECT id, score FROM items WHERE score > $1"
    cached = _cached(sql, "SELECT", returns_rows=True)

    result = await execute_cache_hit(connection, sql, [0.5], cached, default_statement_config, STATE)

    assert result is not None
    assert result.get_data() == [{"id": 1, "score": 1.5}, {"id": 2, "score": 2.5}]
    assert result.column_names == ["id", "score"]
    assert result.rows_affected == 2
    connection.fetch.assert_awaited_once_with(sql, (0.5,))


async def test_execute_cache_hit_counts_dml_rows_with_coerced_parameters() -> None:
    """A DML statement runs through the counting query with float parameters bound as decimals."""
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=_rows([{psqlpy_core._DML_COUNT_COLUMN: 3}])), execute=AsyncMock()
    )
    sql = "UPDATE items SET score = $1"
    cached = _cached(sql, "UPDATE", returns_rows=False)

    result = await execute_cache_hit(connection, sql, (1.5,), cached, default_statement_config, STATE)

    assert result is not None
    assert result.rows_affected == 3
    assert result.operation_type == "UPDATE"
    count_sql, bound = connection.fetch.await_args.args
    assert psqlpy_core._DML_COUNT_COLUMN in count_sql
    assert bound == (Decimal("1.5"),)
    connection.execute.assert_not_awaited()


async def test_execute_cache_hit_rejects_invalid_dml_count() -> None:
    """A counting query that returns anything but one count row raises without being remapped."""
    connection = SimpleNamespace(fetch=AsyncMock(return_value=_rows([{"other": 1}])), execute=AsyncMock())
    sql = "DELETE FROM items"

    with pytest.raises(SQLSpecError, match="invalid result"):
        await execute_cache_hit(
            connection, sql, (), _cached(sql, "DELETE", returns_rows=False), default_statement_config, STATE
        )


async def test_execute_cache_hit_parses_command_tag_for_uncounted_statements() -> None:
    """A statement the counting query cannot wrap executes directly and reports its command tag count."""
    connection = SimpleNamespace(fetch=AsyncMock(), execute=AsyncMock(return_value="CREATE TABLE"))
    sql = "CREATE TABLE items (id INT)"

    result = await execute_cache_hit(
        connection, sql, (), _cached(sql, "DDL", returns_rows=False), default_statement_config, STATE
    )

    assert result is not None
    assert result.rows_affected == 0
    assert result.operation_type == "DDL"
    connection.execute.assert_awaited_once_with(sql, ())
    connection.fetch.assert_not_awaited()


async def test_execute_cache_hit_runs_unprepared_in_pooler_mode() -> None:
    """A session that runs statements unprepared passes ``prepared=False`` to the connection."""
    connection = SimpleNamespace(fetch=AsyncMock(return_value=_rows([{"value": 1}])))
    cached = _cached("SELECT 1 AS value", "SELECT", returns_rows=True)

    result = await execute_cache_hit(connection, "SELECT 1 AS value", (), cached, default_statement_config, ({}, True))

    assert result is not None
    assert result.get_data() == [{"value": 1}]
    connection.fetch.assert_awaited_once_with("SELECT 1 AS value", (), prepared=False)


async def test_execute_cache_hit_maps_driver_errors() -> None:
    """A psqlpy error is raised as its SQLSpec exception with the cause kept."""
    error = ConnectionExecuteError("duplicate key value violates unique constraint items_pk")
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=error))
    cached = _cached("SELECT 1 AS value", "SELECT", returns_rows=True)

    with pytest.raises(UniqueViolationError) as exc_info:
        await execute_cache_hit(connection, "SELECT 1 AS value", (), cached, default_statement_config, STATE)

    assert exc_info.value.__cause__ is error


async def test_execute_cache_hit_propagates_unmapped_errors() -> None:
    """An error outside the psqlpy hierarchy propagates unchanged."""
    error = RuntimeError("boom")
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=error))
    cached = _cached("SELECT 1 AS value", "SELECT", returns_rows=True)

    with pytest.raises(RuntimeError, match="boom") as exc_info:
        await execute_cache_hit(connection, "SELECT 1 AS value", (), cached, default_statement_config, STATE)

    assert exc_info.value is error


@pytest.mark.parametrize(
    ("driver_features", "dialect"),
    [({"enable_cast_detection": False}, "postgres"), ({}, "duckdb")],
    ids=["cast-detection-disabled", "non-postgres-dialect"],
)
async def test_execute_cache_hit_declines_without_cast_aware_preparation(
    driver_features: "dict[str, Any]", dialect: str
) -> None:
    """Sessions that prepare parameters without cast detection are declined untouched."""
    connection = SimpleNamespace(fetch=AsyncMock(), execute=AsyncMock())
    config = default_statement_config.replace(dialect=dialect)
    cached = _cached("SELECT 1 AS value", "SELECT", returns_rows=True)

    result = await execute_cache_hit(connection, "SELECT 1 AS value", (), cached, config, (driver_features, False))

    assert result is None
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()
