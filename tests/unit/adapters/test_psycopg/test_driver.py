"""Psycopg driver regression tests."""

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from typing_extensions import Self

from sqlspec.adapters.psycopg.core import default_statement_config
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.exceptions import SQLSpecError


class _SyncCopyContext:
    def __init__(self, rows: list[bytes] | None = None) -> None:
        self.rows = rows or []
        self.writes: list[bytes] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def __iter__(self) -> Iterator[bytes]:
        return iter(self.rows)

    def write(self, data: bytes) -> None:
        self.writes.append(data)


class _SyncCursor:
    def __init__(self) -> None:
        self.rowcount = 0
        self.description = None
        self.copy_calls: list[str] = []
        self.execute_calls: list[tuple[Any, ...]] = []
        self.copy_context = _SyncCopyContext([b"exported"])

    def copy(self, sql: str) -> _SyncCopyContext:
        self.copy_calls.append(sql)
        return self.copy_context

    def execute(self, *args: Any) -> None:
        self.execute_calls.append(args)


class _AsyncCopyContext:
    def __init__(self, rows: list[bytes] | None = None) -> None:
        self.rows = rows or []
        self.writes: list[bytes] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def __aiter__(self) -> "_AsyncCopyContext":
        self._iter = iter(self.rows)
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def write(self, data: bytes) -> None:
        self.writes.append(data)


class _AsyncCursor:
    def __init__(self) -> None:
        self.rowcount = 0
        self.description = None
        self.copy_calls: list[str] = []
        self.execute_calls: list[tuple[Any, ...]] = []
        self.copy_context = _AsyncCopyContext([b"exported"])

    def copy(self, sql: str) -> _AsyncCopyContext:
        self.copy_calls.append(sql)
        return self.copy_context

    async def execute(self, *args: Any) -> None:
        self.execute_calls.append(args)


class _SyncDriver(PsycopgSyncDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=object())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return self.compiled_sql, self.compiled_parameters


class _AsyncDriver(PsycopgAsyncDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=object())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return self.compiled_sql, self.compiled_parameters


def test_psycopg_sync_execute_script_rejects_multi_statement_parameters() -> None:
    driver = _SyncDriver("INSERT INTO t VALUES (%s); INSERT INTO t VALUES (%s)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)

    with pytest.raises(SQLSpecError, match="multi-statement"):
        driver.dispatch_execute_script(_SyncCursor(), statement)


@pytest.mark.anyio
async def test_psycopg_async_execute_script_rejects_multi_statement_parameters() -> None:
    driver = _AsyncDriver("INSERT INTO t VALUES (%s); INSERT INTO t VALUES (%s)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)

    with pytest.raises(SQLSpecError, match="multi-statement"):
        await driver.dispatch_execute_script(_AsyncCursor(), statement)


@pytest.mark.anyio
async def test_psycopg_async_copy_from_uses_copy_for_program_variant() -> None:
    driver = _AsyncDriver("COPY users FROM PROGRAM 'cat data.csv'")
    statement = SimpleNamespace(
        operation_type="COPY_FROM", parameters="payload", statement_config=default_statement_config
    )
    cursor = _AsyncCursor()

    await driver.dispatch_special_handling(cursor, statement)

    assert cursor.copy_calls == ["COPY users FROM PROGRAM 'cat data.csv'"]
    assert cursor.execute_calls == []


@pytest.mark.anyio
async def test_psycopg_async_copy_to_uses_copy_for_file_variant() -> None:
    driver = _AsyncDriver("COPY users TO '/tmp/users.csv'")
    statement = SimpleNamespace(operation_type="COPY_TO", parameters=None, statement_config=default_statement_config)
    cursor = _AsyncCursor()

    result = await driver.dispatch_special_handling(cursor, statement)

    assert cursor.copy_calls == ["COPY users TO '/tmp/users.csv'"]
    assert cursor.execute_calls == []
    assert result is not None
    assert result.data == [{"copy_output": "exported"}]
