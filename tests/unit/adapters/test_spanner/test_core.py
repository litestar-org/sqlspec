"""Unit tests for Spanner core performance helpers."""

from types import SimpleNamespace
from typing import Any, cast

from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.core import (
    build_param_type_signature,
    collect_rows,
    resolve_column_names,
    resolve_row_plan,
)


def _field(name: str, code: int) -> SimpleNamespace:
    return SimpleNamespace(name=name, type_=SimpleNamespace(code=code))


def _json_object(value: str) -> object:
    return cast("Any", JsonObject).from_str(value)


def test_build_param_type_signature_empty_parameters() -> None:
    assert build_param_type_signature(None) == ()
    assert build_param_type_signature({}) == ()


def test_build_param_type_signature_tracks_key_type_pairs() -> None:
    signature = build_param_type_signature({"id": 1, "name": "alice"})

    assert signature == (("id", int), ("name", str))


def test_resolve_row_plan_returns_empty_plan_without_metadata() -> None:
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    column_names, column_plan = resolve_row_plan(None, cache, json_deserializer=lambda value: value)

    assert column_names == []
    assert column_plan is None
    assert cache == {}


def test_resolve_row_plan_returns_empty_plan_for_non_json_columns() -> None:
    fields = [_field("id", TypeCode.INT64), _field("name", TypeCode.STRING)]
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    column_names, column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)

    assert column_names == ["id", "name"]
    assert column_plan is None
    assert len(cache) == 1


def test_resolve_row_plan_caches_json_metadata_and_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON)]
    cache: dict[int, tuple[object, list[str], tuple[tuple[int, object], ...] | None]] = {}

    first_column_names, first_column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)
    second_column_names, second_column_plan = resolve_row_plan(fields, cache, json_deserializer=lambda value: value)

    assert first_column_names == ["id", "payload"]
    assert first_column_names is second_column_names
    assert first_column_plan is not None
    assert first_column_plan is second_column_plan
    assert first_column_plan[0][0] == 1


def test_collect_rows_returns_original_rows_without_a_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.STRING)]
    rows = [(1, '{"kind":"string"}')]

    data, column_names = collect_rows(rows, fields, column_names=["id", "payload"], column_plan=None)

    assert data is rows
    assert column_names == ["id", "payload"]


def test_collect_rows_normalizes_list_rows_without_a_plan() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.STRING)]
    rows: list[Any] = [[1, '{"kind":"string"}']]

    data, column_names = collect_rows(rows, fields, column_names=["id", "payload"], column_plan=None)

    assert data == [(1, '{"kind":"string"}')]
    assert data is not rows
    assert column_names == ["id", "payload"]


def test_collect_rows_applies_json_plan_to_metadata_only() -> None:
    fields = [_field("id", TypeCode.INT64), _field("payload", TypeCode.JSON), _field("note", TypeCode.STRING)]
    rows = [(1, _json_object('{"kind":"payload"}'), '{"kind":"string"}')]
    json_calls: list[str] = []

    def json_deserializer(value: str) -> dict[str, str]:
        json_calls.append(value)
        return {"decoded": value}

    column_names, column_plan = resolve_row_plan(fields, {}, json_deserializer=json_deserializer)
    data, resolved_column_names = collect_rows(
        rows,
        fields,
        column_names=column_names,
        column_plan=column_plan,
    )

    assert resolved_column_names == ["id", "payload", "note"]
    assert data == [(1, {"decoded": '{"kind":"payload"}'}, '{"kind":"string"}')]
    assert json_calls == ['{"kind":"payload"}']


def test_resolve_column_names_reuses_cached_fields() -> None:
    fields = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(fields, cache)
    second = resolve_column_names(fields, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1
