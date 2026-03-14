"""Unit tests for ADBC core execute-many helpers."""

from types import SimpleNamespace
from typing import Any

from sqlspec.adapters.adbc import core as adbc_core
from sqlspec.adapters.adbc.core import (
    collect_rows,
    get_statement_config,
    prepare_parameters_with_casts,
    prepare_postgres_parameters,
    resolve_column_names,
    resolve_many_rowcount,
)


def test_prepare_postgres_parameters_fast_path_without_casts() -> None:
    statement_config = get_statement_config("postgres")

    prepared = prepare_postgres_parameters(
        {}, {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared is None


def test_resolve_many_rowcount_prefers_driver_rowcount() -> None:
    cursor = SimpleNamespace(rowcount=7)

    assert resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}]) == 7


def test_resolve_many_rowcount_falls_back_to_parameter_count() -> None:
    cursor = SimpleNamespace(rowcount=-1)

    assert resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}, {"id": 3}]) == 3


def test_resolve_many_rowcount_returns_unknown_for_unsized_payload() -> None:
    cursor = SimpleNamespace(rowcount=-1)
    payload: Any = iter(({"id": 1}, {"id": 2}))

    assert resolve_many_rowcount(cursor, payload) == -1


def test_resolve_many_rowcount_supports_sized_non_sequence_payload() -> None:
    cursor = SimpleNamespace(rowcount=-1)

    assert resolve_many_rowcount(cursor, range(4)) == 4


def test_resolve_many_rowcount_uses_precomputed_fallback_count() -> None:
    cursor = SimpleNamespace(rowcount=-1)
    payload: Any = iter(({"id": 1}, {"id": 2}))

    assert resolve_many_rowcount(cursor, payload, fallback_count=6) == 6


def test_resolve_column_names_reuses_cached_description() -> None:
    description = [("id", object()), ("name", object())]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(description, cache)
    second = resolve_column_names(description, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1


def test_collect_rows_uses_precomputed_column_names() -> None:
    rows = [(1, "alice")]
    description = [("ignored", object())]

    data, column_names = collect_rows(rows, description, column_names=["id", "name"])

    assert data is rows
    assert column_names == ["id", "name"]


def test_prepare_parameters_with_casts_uses_type_converter_factory(monkeypatch: Any) -> None:
    statement_config = get_statement_config("postgres")
    factory_calls: list[tuple[str, int]] = []

    class FakeConverter:
        def convert_dict(self, value: dict[str, Any]) -> str:
            return f"factory:{sorted(value.items())!r}"

    def fake_factory(dialect: str, cache_size: int = 5000) -> FakeConverter:
        factory_calls.append((dialect, cache_size))
        return FakeConverter()

    monkeypatch.setattr(adbc_core, "get_adbc_type_converter", fake_factory)

    prepared = prepare_parameters_with_casts(
        [{"id": 1}],
        {},
        statement_config,
        dialect="postgres",
        json_serializer=lambda value: str(value),
    )

    assert prepared == ["factory:[('id', 1)]"]
    assert factory_calls == [("postgres", 5000)]
