"""Unit tests for Spanner core performance helpers."""

from types import SimpleNamespace

from sqlspec.adapters.spanner.core import build_param_type_signature, collect_rows, resolve_column_names


def test_build_param_type_signature_empty_parameters() -> None:
    assert build_param_type_signature(None) == ()
    assert build_param_type_signature({}) == ()


def test_build_param_type_signature_tracks_key_type_pairs() -> None:
    signature = build_param_type_signature({"id": 1, "name": "alice"})

    assert signature == (("id", int), ("name", str))


def test_collect_rows_only_converts_string_values() -> None:
    class Converter:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def convert_if_detected(self, value: str) -> str:
            self.calls.append(value)
            return value.upper()

    fields = [SimpleNamespace(name="id"), SimpleNamespace(name="name"), SimpleNamespace(name="score")]
    rows = [(1, "alice", 98.5), (2, "bob", 91.0)]
    converter = Converter()

    data, column_names = collect_rows(rows, fields, converter)

    assert column_names == ["id", "name", "score"]
    assert data == [(1, "ALICE", 98.5), (2, "BOB", 91.0)]
    assert converter.calls == ["alice", "bob"]


def test_collect_rows_keeps_values_when_converter_makes_no_changes() -> None:
    class Converter:
        def convert_if_detected(self, value: str) -> str:
            return value

    fields = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    rows = [(1, "alice"), (2, "bob")]

    data, column_names = collect_rows(rows, fields, Converter())

    assert column_names == ["id", "name"]
    assert data == rows


def test_resolve_column_names_reuses_cached_fields() -> None:
    fields = [SimpleNamespace(name="id"), SimpleNamespace(name="name")]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(fields, cache)
    second = resolve_column_names(fields, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1
