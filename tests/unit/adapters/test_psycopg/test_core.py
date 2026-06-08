# pyright: reportArgumentType=false
"""Unit tests for psycopg core helpers."""

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from typing_extensions import Self

from sqlspec.adapters.psycopg.core import (
    build_async_pipeline_execution_result,
    build_pipeline_execution_result,
    create_mapped_exception,
    default_statement_config,
    resolve_many_rowcount,
)
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core import SQL
from sqlspec.exceptions import SQLParsingError, SQLSpecError, UniqueViolationError

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection


def test_resolve_many_rowcount_prefers_positive_driver_rowcount() -> None:
    """resolve_many_rowcount should keep positive cursor rowcount values."""
    cursor = SimpleNamespace(rowcount=7)
    affected_rows = resolve_many_rowcount(cursor, [("a",), ("b",), ("c",)])
    assert affected_rows == 7


def test_resolve_many_rowcount_falls_back_to_parameter_count() -> None:
    """resolve_many_rowcount should fallback to parameter count when rowcount is unknown."""
    cursor = SimpleNamespace(rowcount=-1)
    affected_rows = resolve_many_rowcount(cursor, [("a",), ("b",), ("c",)])
    assert affected_rows == 3


def test_resolve_many_rowcount_returns_zero_for_unsized_parameters() -> None:
    """resolve_many_rowcount should return zero when no rowcount or length is available."""
    cursor = SimpleNamespace(rowcount=-1)

    def _parameter_stream() -> "Iterator[tuple[str]]":
        yield ("a",)

    affected_rows = resolve_many_rowcount(cursor, _parameter_stream())
    assert affected_rows == 0


def test_resolve_many_rowcount_prefers_precomputed_fallback_for_unsized_parameters() -> None:
    """resolve_many_rowcount should use a precomputed fallback before probing length."""
    cursor = SimpleNamespace(rowcount=-1)

    def _parameter_stream() -> "Iterator[tuple[str]]":
        yield ("a",)

    affected_rows = resolve_many_rowcount(cursor, _parameter_stream(), fallback_count=4)
    assert affected_rows == 4


def test_create_mapped_exception_dispatches_native_psycopg_error() -> None:
    """Native psycopg error types should resolve through the adapter-local dispatch table."""
    pytest.importorskip("psycopg")
    from psycopg import errors as pg_errors

    error = pg_errors.SyntaxError("syntax error at or near SELECT")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, SQLParsingError)
    assert mapped.__cause__ is error
    assert "[42601]" in str(mapped)


def test_create_mapped_exception_dispatches_native_psycopg_subclass() -> None:
    """Native psycopg subclasses should use cached MRO dispatch without changing mappings."""
    pytest.importorskip("psycopg")
    from psycopg import errors as pg_errors

    class CustomUniqueViolation(pg_errors.UniqueViolation):
        pass

    error = CustomUniqueViolation("duplicate key value violates unique constraint")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, UniqueViolationError)
    assert mapped.__cause__ is error
    assert "[23505]" in str(mapped)


def test_build_pipeline_execution_result_uses_column_resolver_fast_path() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace()]

        def fetchall(self) -> list[tuple[int, str]]:
            return [(1, "alice"), (2, "bob")]

    cursor = _Cursor()
    result = build_pipeline_execution_result(
        cast("Any", _Statement()), cursor, column_name_resolver=lambda description: ["id", "name"]
    )
    assert result.selected_data == [(1, "alice"), (2, "bob")]
    assert result.column_names == ["id", "name"]
    assert result.data_row_count == 2


def test_build_async_pipeline_execution_result_uses_column_resolver_fast_path() -> None:

    class _Statement:
        def returns_rows(self) -> bool:
            return True

    class _Cursor:
        def __init__(self) -> None:
            self.description = [SimpleNamespace()]

        async def fetchall(self) -> list[tuple[int, str]]:
            return [(1, "alice"), (2, "bob")]

    async def _run() -> None:
        cursor = _Cursor()
        result = await build_async_pipeline_execution_result(
            cast("Any", _Statement()), cursor, column_name_resolver=lambda description: ["id", "name"]
        )
        assert result.selected_data == [(1, "alice"), (2, "bob")]
        assert result.column_names == ["id", "name"]
        assert result.data_row_count == 2

    asyncio.run(_run())


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


def _sync_connection() -> "PsycopgSyncConnection":
    return cast("PsycopgSyncConnection", object())


def _async_connection() -> "PsycopgAsyncConnection":
    return cast("PsycopgAsyncConnection", object())


class _SyncDriver(PsycopgSyncDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=_sync_connection())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return (self.compiled_sql, self.compiled_parameters)


class _AsyncDriver(PsycopgAsyncDriver):
    def __init__(self, compiled_sql: str, parameters: object = None) -> None:
        super().__init__(connection=_async_connection())
        self.compiled_sql = compiled_sql
        self.compiled_parameters = parameters

    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return (self.compiled_sql, self.compiled_parameters)


def test_driver_psycopg_sync_execute_script_rejects_multi_statement_parameters() -> None:
    driver = _SyncDriver("INSERT INTO t VALUES (%s); INSERT INTO t VALUES (%s)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)
    with pytest.raises(SQLSpecError, match="multi-statement"):
        driver.dispatch_execute_script(_SyncCursor(), cast("SQL", statement))


@pytest.mark.anyio
async def test_driver_psycopg_async_execute_script_rejects_multi_statement_parameters() -> None:
    driver = _AsyncDriver("INSERT INTO t VALUES (%s); INSERT INTO t VALUES (%s)", [1])
    statement = SimpleNamespace(statement_config=default_statement_config)
    with pytest.raises(SQLSpecError, match="multi-statement"):
        await driver.dispatch_execute_script(_AsyncCursor(), cast("SQL", statement))


@pytest.mark.anyio
async def test_driver_psycopg_async_copy_from_uses_copy_for_program_variant() -> None:
    driver = _AsyncDriver("COPY users FROM PROGRAM 'cat data.csv'")
    statement = SimpleNamespace(
        operation_type="COPY_FROM", parameters="payload", statement_config=default_statement_config
    )
    cursor = _AsyncCursor()
    await driver.dispatch_special_handling(cursor, cast("SQL", statement))
    assert cursor.copy_calls == ["COPY users FROM PROGRAM 'cat data.csv'"]
    assert cursor.execute_calls == []


@pytest.mark.anyio
async def test_driver_psycopg_async_copy_to_uses_copy_for_file_variant() -> None:
    driver = _AsyncDriver("COPY users TO '/tmp/users.csv'")
    statement = SimpleNamespace(operation_type="COPY_TO", parameters=None, statement_config=default_statement_config)
    cursor = _AsyncCursor()
    result = await driver.dispatch_special_handling(cursor, cast("SQL", statement))
    assert cursor.copy_calls == ["COPY users TO '/tmp/users.csv'"]
    assert cursor.execute_calls == []
    assert result is not None
    assert result.data == [{"copy_output": "exported"}]
