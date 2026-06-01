"""Psqlpy driver regression tests."""

from types import SimpleNamespace
from typing import Any

import pytest

from sqlspec.adapters.psqlpy.core import default_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.exceptions import SQLSpecError


class _Cursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[Any, ...]] = []

    async def execute(self, *args: Any) -> str:
        self.execute_calls.append(args)
        return "OK"


class _Driver(PsqlpyDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=object())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return self.compiled_sql, self.compiled_parameters


@pytest.mark.anyio
async def test_psqlpy_execute_script_rejects_multi_statement_parameters() -> None:
    driver = _Driver("INSERT INTO t VALUES ($1); INSERT INTO t VALUES ($1)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)

    with pytest.raises(SQLSpecError, match="multi-statement"):
        await driver.dispatch_execute_script(_Cursor(), statement)


@pytest.mark.anyio
async def test_psqlpy_execute_script_uses_empty_params_for_each_sub_statement() -> None:
    driver = _Driver("SELECT 1; SELECT 2")
    statement = SimpleNamespace(statement_config=default_statement_config)
    cursor = _Cursor()

    await driver.dispatch_execute_script(cursor, statement)

    assert cursor.execute_calls == [("SELECT 1", []), ("SELECT 2", [])]


@pytest.mark.anyio
async def test_psqlpy_execute_script_allows_single_statement_parameters_with_empty_driver_params() -> None:
    driver = _Driver("INSERT INTO t VALUES ($1)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)
    cursor = _Cursor()

    await driver.dispatch_execute_script(cursor, statement)

    assert cursor.execute_calls == [("INSERT INTO t VALUES ($1)", [])]
