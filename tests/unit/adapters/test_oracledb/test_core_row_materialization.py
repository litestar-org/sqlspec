# pyright: reportArgumentType=false
"""Unit tests for Oracle row materialization helpers."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.oracledb.core import collect_async_rows, collect_sync_rows, resolve_row_metadata
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection


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
    (data, column_names) = collect_sync_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data is rows
    assert column_names == ["id", "name"]


def test_collect_sync_rows_coerces_lob_values_when_lob_columns_present() -> None:
    """collect_sync_rows should coerce readable LOB values when metadata requires it."""
    rows = [(1, _ReadableValue("plain text"))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]
    (data, column_names) = collect_sync_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data is not rows
    assert data == [(1, "plain text")]
    assert column_names == ["id", "payload"]


def test_collect_sync_rows_leaves_json_text_as_string_without_content_sniffing() -> None:
    """Readable CLOB values should be read as strings, not auto-parsed as JSON."""
    rows = [(1, _ReadableValue('{"key": "value"}'))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]
    (data, _) = collect_sync_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data == [(1, '{"key": "value"}')]
    assert isinstance(data[0][1], str)


async def test_collect_async_rows_reuses_original_rows_when_no_lob_columns() -> None:
    """collect_async_rows should avoid async coercion for non-LOB result sets."""
    rows = [(1, "alpha"), (2, "beta")]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]
    (data, column_names) = await collect_async_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data is rows
    assert column_names == ["id", "name"]


async def test_collect_async_rows_coerces_lob_values_when_lob_columns_present() -> None:
    """collect_async_rows should coerce readable LOB values for LOB result sets."""
    rows = [(1, _ReadableValue("plain text"))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]
    (data, column_names) = await collect_async_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data == [(1, "plain text")]
    assert column_names == ["id", "payload"]


async def test_collect_async_rows_leaves_json_text_as_string_without_content_sniffing() -> None:
    """Readable CLOB values should be read as strings, not auto-parsed as JSON."""
    rows = [(1, _ReadableValue('{"key": "value"}'))]
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]
    (data, _) = await collect_async_rows(rows, description, {"enable_lowercase_column_names": True})
    assert data == [(1, '{"key": "value"}')]
    assert isinstance(data[0][1], str)


def test_resolve_row_metadata_reuses_cached_description() -> None:
    """resolve_row_metadata should cache normalized names and LOB flags by description identity."""
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (first_names, first_requires_lob) = resolve_row_metadata(
        description, {"enable_lowercase_column_names": True}, cache
    )
    (second_names, second_requires_lob) = resolve_row_metadata(
        description, {"enable_lowercase_column_names": True}, cache
    )
    assert first_names == ["id", "name"]
    assert second_names is first_names
    assert first_requires_lob is False
    assert second_requires_lob is False
    assert len(cache) == 1


def test_resolve_row_metadata_single_pass_lob_detection_mixed_columns() -> None:
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("PAYLOAD", _TypeCode("DB_TYPE_CLOB"))]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (column_names, requires_lob) = resolve_row_metadata(description, {"enable_lowercase_column_names": True}, cache)
    assert column_names == ["id", "payload"]
    assert requires_lob is True


def test_resolve_row_metadata_single_pass_no_lob_columns() -> None:
    description = [("ID", _TypeCode("DB_TYPE_NUMBER")), ("NAME", _TypeCode("DB_TYPE_VARCHAR"))]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (column_names, requires_lob) = resolve_row_metadata(description, {"enable_lowercase_column_names": False}, cache)
    assert column_names == ["ID", "NAME"]
    assert requires_lob is False


def test_resolve_row_metadata_single_pass_lob_detection_via_str_fallback() -> None:
    description = [("PAYLOAD", "DB_TYPE_BLOB")]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (column_names, requires_lob) = resolve_row_metadata(description, {"enable_lowercase_column_names": True}, cache)
    assert column_names == ["payload"]
    assert requires_lob is True


def test_resolve_row_metadata_single_pass_unknown_type_code_shape_is_conservative() -> None:
    description = [object()]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (column_names, requires_lob) = resolve_row_metadata(description, {"enable_lowercase_column_names": True}, cache)
    assert column_names == []
    assert requires_lob is True


def test_resolve_row_metadata_cache_contract_still_holds_after_single_pass_rewrite() -> None:
    description = [("ID", _TypeCode("DB_TYPE_NUMBER"))]
    cache: dict[int, tuple[object, list[str], bool]] = {}
    (first_names, first_requires_lob) = resolve_row_metadata(description, {}, cache)
    (second_names, second_requires_lob) = resolve_row_metadata(description, {}, cache)
    assert second_names is first_names
    assert first_requires_lob is second_requires_lob
    assert cache[id(description)][0] is description


def _raise_attribute_error(*_args: object, **_kwargs: object) -> object:
    raise AttributeError("native arrow unavailable")


def _sync_connection() -> "OracleSyncConnection":
    return cast("OracleSyncConnection", object())


def _async_connection() -> "OracleAsyncConnection":
    return cast("OracleAsyncConnection", object())


def test_select_to_arrow_fallback_sync_select_to_arrow_fallback_passes_prepared_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    def fallback(
        self: SyncDriverAdapterBase,
        statement: object,
        /,
        *parameters: object,
        statement_config: object = None,
        **kwargs: object,
    ) -> object:
        _ = (self, kwargs)
        captured["statement"] = statement
        captured["parameters"] = parameters
        captured["statement_config"] = statement_config
        return sentinel

    monkeypatch.setattr(OracleSyncDriver, "_execute_arrow_dataframe", _raise_attribute_error)
    monkeypatch.setattr(SyncDriverAdapterBase, "select_to_arrow", fallback)
    driver = OracleSyncDriver(connection=_sync_connection())
    result = driver.select_to_arrow("SELECT :id FROM dual", {"id": 1})
    assert result is sentinel
    assert captured["parameters"] == ()
    assert captured["statement"].is_processed is True
    assert captured["statement_config"] is driver.statement_config


@pytest.mark.anyio
async def test_select_to_arrow_fallback_async_select_to_arrow_fallback_passes_prepared_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    async def fallback(
        self: AsyncDriverAdapterBase,
        statement: object,
        /,
        *parameters: object,
        statement_config: object = None,
        **kwargs: object,
    ) -> object:
        _ = (self, kwargs)
        captured["statement"] = statement
        captured["parameters"] = parameters
        captured["statement_config"] = statement_config
        return sentinel

    monkeypatch.setattr(OracleAsyncDriver, "_execute_arrow_dataframe", _raise_attribute_error)
    monkeypatch.setattr(AsyncDriverAdapterBase, "select_to_arrow", fallback)
    driver = OracleAsyncDriver(connection=_async_connection())
    result = await driver.select_to_arrow("SELECT :id FROM dual", {"id": 1})
    assert result is sentinel
    assert captured["parameters"] == ()
    assert captured["statement"].is_processed is True
    assert captured["statement_config"] is driver.statement_config
