"""Unit tests for ADBC core execute-many helpers."""

from types import SimpleNamespace
from typing import Any

from sqlspec.adapters.adbc.core import (
    collect_rows,
    get_statement_config,
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
