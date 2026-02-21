"""Unit tests for BigQuery core performance helpers."""

from types import SimpleNamespace

from sqlspec.adapters.bigquery.core import collect_rows, resolve_column_names


def _schema_field(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name)


def test_resolve_column_names_reuses_cached_schema() -> None:
    schema = [_schema_field("id"), _schema_field("name")]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(schema, cache)
    second = resolve_column_names(schema, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1


def test_resolve_column_names_distinguishes_schema_identity() -> None:
    schema_one = [_schema_field("id")]
    schema_two = [_schema_field("id")]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(schema_one, cache)
    second = resolve_column_names(schema_two, cache)

    assert first == second == ["id"]
    assert first is not second
    assert len(cache) == 2


def test_collect_rows_uses_precomputed_column_names() -> None:
    rows = [{"id": 1}]
    schema = [_schema_field("ignored")]

    data, column_names = collect_rows(rows, schema, column_names=["id"])

    assert data is rows
    assert column_names == ["id"]


def test_collect_rows_uses_cache_when_column_names_not_precomputed() -> None:
    rows = [{"id": 1, "name": "x"}]
    schema = [_schema_field("id"), _schema_field("name")]
    cache: dict[int, tuple[object, list[str]]] = {}

    data_one, column_names_one = collect_rows(rows, schema, column_name_cache=cache)
    data_two, column_names_two = collect_rows(rows, schema, column_name_cache=cache)

    assert data_one is rows
    assert data_two is rows
    assert column_names_one == ["id", "name"]
    assert column_names_two is column_names_one
    assert len(cache) == 1
