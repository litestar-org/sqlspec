"""Unit tests for psqlpy driver dispatch."""

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQL

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection


class _Connection:
    def __init__(self, count: int = 0, returned_rows: "list[dict[str, Any]] | None" = None) -> None:
        self.count = count
        self.returned_rows = returned_rows or []
        self.fetch_calls: list[tuple[str, object]] = []
        self.execute_calls: list[tuple[str, object]] = []

    async def fetch(self, sql: str, parameters: object) -> object:
        self.fetch_calls.append((sql, parameters))
        rows = [{"_sqlspec_rows_affected": self.count}] if "_sqlspec_rows_affected" in sql else self.returned_rows
        return SimpleNamespace(result=lambda: rows)

    async def execute(self, sql: str, parameters: object) -> str:
        self.execute_calls.append((sql, parameters))
        return ""


class _Driver(PsqlpyDriver):
    def __init__(self, connection: _Connection, compiled_sql: str, parameters: object) -> None:
        super().__init__(connection=cast("PsqlpyConnection", connection))
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return self.compiled_sql, self.compiled_parameters


@pytest.mark.anyio
@pytest.mark.parametrize("count", [0, 1, 7])
async def test_dispatch_execute_fetches_exact_dml_count(count: int) -> None:
    """Non-returning DML should fetch one server-side count row."""
    connection = _Connection(count=count)
    parameters = ["done", 1]
    driver = _Driver(connection, "UPDATE events SET state = $1 WHERE id = $2", parameters)

    result = await driver.dispatch_execute(
        cast("PsqlpyConnection", connection), SQL("UPDATE events SET state = ? WHERE id = ?", parameters)
    )

    assert result.rowcount_override == count
    assert len(connection.fetch_calls) == 1
    assert connection.fetch_calls[0][1] is parameters
    assert connection.execute_calls == []


@pytest.mark.anyio
async def test_dispatch_execute_reuses_count_shape_with_current_cached_parameters() -> None:
    """Repeated compiled SQL should execute the count query with each call's parameters."""
    connection = _Connection(count=1)
    driver = _Driver(connection, "DELETE FROM events WHERE id = $1", [1])

    first = await driver.dispatch_execute(
        cast("PsqlpyConnection", connection), SQL("DELETE FROM events WHERE id = ?", 1)
    )
    driver.compiled_parameters = [2]
    second = await driver.dispatch_execute(
        cast("PsqlpyConnection", connection), SQL("DELETE FROM events WHERE id = ?", 2)
    )

    assert first.rowcount_override == 1
    assert second.rowcount_override == 1
    assert [call[1] for call in connection.fetch_calls] == [[1], [2]]


@pytest.mark.anyio
async def test_dispatch_execute_preserves_returning_rows() -> None:
    """DML with RETURNING should retain the existing row-result path."""
    connection = _Connection(returned_rows=[{"id": 1}])
    parameters = ["done", 1]
    driver = _Driver(connection, "UPDATE events SET state = $1 WHERE id = $2 RETURNING id", parameters)

    result = await driver.dispatch_execute(
        cast("PsqlpyConnection", connection), SQL("UPDATE events SET state = ? WHERE id = ? RETURNING id", parameters)
    )

    assert result.selected_data == [{"id": 1}]
    assert result.data_row_count == 1
    assert connection.execute_calls == []
    assert connection.fetch_calls == [("UPDATE events SET state = $1 WHERE id = $2 RETURNING id", parameters)]
