"""Unit tests for ADBC core execute-many helpers."""

from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any

from sqlspec.adapters.adbc import core as adbc_core
from sqlspec.adapters.adbc.core import (
    _prepare_batch_with_casts,
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
        [{"id": 1}], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == ["factory:[('id', 1)]"]
    assert factory_calls == [("postgres", 5000)]


def test_prepare_batch_with_casts_creates_converter_once(monkeypatch: Any) -> None:
    statement_config = get_statement_config("postgres")
    factory_calls: list[tuple[str, int]] = []

    class FakeConverter:
        def convert_dict(self, value: dict[str, Any]) -> str:
            return f"factory:{sorted(value.items())!r}"

    def fake_factory(dialect: str, cache_size: int = 5000) -> FakeConverter:
        factory_calls.append((dialect, cache_size))
        return FakeConverter()

    monkeypatch.setattr(adbc_core, "get_adbc_type_converter", fake_factory)

    prepared = _prepare_batch_with_casts(
        [(1, {"name": "alice"}), (2, {"name": "bob"}), (3, {"name": "carol"})],
        {1: "INT"},
        statement_config,
        dialect="postgres",
        json_serializer=lambda value: str(value),
    )

    assert prepared == [
        (1, "factory:[('name', 'alice')]"),
        (2, "factory:[('name', 'bob')]"),
        (3, "factory:[('name', 'carol')]"),
    ]
    assert factory_calls == [("postgres", 5000)]


def test_prepare_parameters_with_casts_supports_subclass_type_dispatch() -> None:
    class MyInt(int):
        pass

    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(type_coercion_map={int: lambda value: value + 1})
    )

    prepared = prepare_parameters_with_casts(
        [MyInt(4)], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [5]


def test_prepare_parameters_with_casts_supports_virtual_abc_dispatch() -> None:
    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            type_coercion_map={Sequence: lambda value: tuple(value)}
        )
    )

    prepared = prepare_parameters_with_casts(
        [[1, 2]], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [(1, 2)]


def test_detect_dialect_uses_fallback_when_introspection_returns_unknown() -> None:
    """detect_dialect honors fallback_dialect when GetInfo yields no pattern match."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect="sqlite")
    assert result == "sqlite"


def test_detect_dialect_still_defaults_to_postgres_without_fallback() -> None:
    """Without fallback_dialect, the legacy 'default to postgres' path still fires."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect=None)
    assert result == "postgres"


def test_detect_dialect_introspection_match_beats_fallback() -> None:
    """An introspection match always wins over the fallback signal."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "duckdb", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect="sqlite")
    assert result == "duckdb"


def test_handle_postgres_rollback_issues_rollback_for_pgvector() -> None:
    """pgvector is PostgreSQL-compatible and must receive rollback handling."""
    executed: list[str] = []
    cursor = SimpleNamespace(execute=lambda sql: executed.append(sql))

    adbc_core.handle_postgres_rollback("pgvector", cursor)

    assert executed == ["ROLLBACK"]


def test_handle_postgres_rollback_issues_rollback_for_paradedb() -> None:
    """paradedb is PostgreSQL-compatible and must receive rollback handling."""
    executed: list[str] = []
    cursor = SimpleNamespace(execute=lambda sql: executed.append(sql))

    adbc_core.handle_postgres_rollback("paradedb", cursor)

    assert executed == ["ROLLBACK"]


def test_normalize_postgres_empty_parameters_returns_none_for_pgvector() -> None:
    """pgvector empty dict parameters should use the PostgreSQL empty-params path."""
    assert adbc_core.normalize_postgres_empty_parameters("pgvector", {}) is None


def test_normalize_postgres_empty_parameters_returns_none_for_paradedb() -> None:
    """paradedb empty dict parameters should use the PostgreSQL empty-params path."""
    assert adbc_core.normalize_postgres_empty_parameters("paradedb", {}) is None


def test_base_type_coercion_map_replaces_getter_function() -> None:
    assert isinstance(adbc_core._BASE_TYPE_COERCION_MAP, dict)
    assert not hasattr(adbc_core, "_get_type_coercion_map")
    assert adbc_core.driver_profile is not None
