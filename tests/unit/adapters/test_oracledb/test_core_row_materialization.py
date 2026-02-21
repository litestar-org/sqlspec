"""Unit tests for Oracle row materialization helpers."""

import pytest

from sqlspec.adapters.oracledb.core import collect_async_rows, collect_sync_rows, resolve_row_metadata

pytestmark = pytest.mark.xdist_group("adapter_unit")


class _TypeCode:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class _ReadableValue:
    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def read(self) -> str:
        return self._value


def test_collect_sync_rows_reuses_original_rows_when_no_lob_columns() -> None:
    """collect_sync_rows should avoid row copy work for non-LOB result sets."""
    rows = [(1, "alpha"), (2, "beta")]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]

    data, column_names = collect_sync_rows(rows, description, {"enable_lowercase_column_names": True})

    assert data is rows
    assert column_names == ["id", "name"]


def test_collect_sync_rows_coerces_lob_values_when_lob_columns_present() -> None:
    """collect_sync_rows should coerce readable LOB values when metadata requires it."""
    rows = [(1, _ReadableValue("plain text"))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]

    data, column_names = collect_sync_rows(rows, description, {"enable_lowercase_column_names": True})

    assert data is not rows
    assert data == [(1, "plain text")]
    assert column_names == ["id", "payload"]


async def test_collect_async_rows_reuses_original_rows_when_no_lob_columns() -> None:
    """collect_async_rows should avoid async coercion for non-LOB result sets."""
    rows = [(1, "alpha"), (2, "beta")]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]

    data, column_names = await collect_async_rows(rows, description, {"enable_lowercase_column_names": True})

    assert data is rows
    assert column_names == ["id", "name"]


async def test_collect_async_rows_coerces_lob_values_when_lob_columns_present() -> None:
    """collect_async_rows should coerce readable LOB values for LOB result sets."""
    rows = [(1, _ReadableValue("plain text"))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]

    data, column_names = await collect_async_rows(rows, description, {"enable_lowercase_column_names": True})

    assert data == [(1, "plain text")]
    assert column_names == ["id", "payload"]


def test_resolve_row_metadata_reuses_cached_description() -> None:
    """resolve_row_metadata should cache normalized names and LOB flags by description identity."""
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]
    cache: dict[int, tuple[object, list[str], bool]] = {}

    first_names, first_requires_lob = resolve_row_metadata(description, {"enable_lowercase_column_names": True}, cache)
    second_names, second_requires_lob = resolve_row_metadata(
        description, {"enable_lowercase_column_names": True}, cache
    )

    assert first_names == ["id", "name"]
    assert second_names is first_names
    assert first_requires_lob is False
    assert second_requires_lob is False
    assert len(cache) == 1
