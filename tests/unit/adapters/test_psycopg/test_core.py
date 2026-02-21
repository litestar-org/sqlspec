"""Unit tests for psycopg core helpers."""

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, cast

from sqlspec.adapters.psycopg.core import (
    build_async_pipeline_execution_result,
    build_pipeline_execution_result,
    resolve_many_rowcount,
)


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
