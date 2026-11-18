"""Tests for msgspec schema conversion."""

from typing import Annotated

import msgspec
import pytest

import sqlspec.utils.schema as schema_utils
from sqlspec.utils.schema import to_schema


def _upper_field(name: str) -> str:
    return name.upper()


class CamelChild(msgspec.Struct, rename="camel"):
    item_name: str
    item_count: int


class SingleWordEnvelope(msgspec.Struct, rename="camel"):
    rows: list[CamelChild]
    total: int


class KebabEnvelope(msgspec.Struct, rename="kebab"):
    child_rows: list[CamelChild]


class ExplicitAliasChild(msgspec.Struct):
    display_name: str = msgspec.field(name="label")


class CallableAliasEnvelope(msgspec.Struct, rename=_upper_field):
    child: ExplicitAliasChild
    item_count: int


class OptionalCollectionEnvelope(msgspec.Struct):
    child: Annotated[CamelChild | None, "nested child"]
    children: tuple[CamelChild, ...]
    children_by_key: dict[str, CamelChild]


class ArbitraryPayloadEnvelope(msgspec.Struct, rename="camel"):
    metadata: dict[str, object]


class StrictEnvelope(msgspec.Struct, rename="camel", forbid_unknown_fields=True):
    child: CamelChild


def test_msgspec_single_word_envelope_decodes_nested_encoded_field_names() -> None:
    result = to_schema({"rows": [{"itemName": "a", "itemCount": 1}], "total": 1}, schema_type=SingleWordEnvelope)

    assert result == SingleWordEnvelope(rows=[CamelChild(item_name="a", item_count=1)], total=1)


def test_msgspec_single_word_envelope_decodes_batch() -> None:
    result = to_schema(
        [
            {"rows": [{"itemName": "a", "itemCount": 1}], "total": 1},
            {"rows": [{"itemName": "b", "itemCount": 2}], "total": 1},
        ],
        schema_type=SingleWordEnvelope,
    )

    assert result == [
        SingleWordEnvelope(rows=[CamelChild(item_name="a", item_count=1)], total=1),
        SingleWordEnvelope(rows=[CamelChild(item_name="b", item_count=2)], total=1),
    ]


def test_msgspec_mixed_parent_and_child_rename_conventions() -> None:
    result = to_schema({"child_rows": [{"itemName": "a", "itemCount": 1}]}, schema_type=KebabEnvelope)

    assert result == KebabEnvelope(child_rows=[CamelChild(item_name="a", item_count=1)])


def test_msgspec_callable_and_explicit_aliases() -> None:
    result = to_schema({"child": {"label": "Ada"}, "item_count": 1}, schema_type=CallableAliasEnvelope)

    assert result == CallableAliasEnvelope(child=ExplicitAliasChild(display_name="Ada"), item_count=1)


def test_msgspec_optional_and_collection_fields() -> None:
    child = {"itemName": "a", "itemCount": 1}
    result = to_schema(
        {"child": child, "children": [child], "children_by_key": {"arbitrary_key": child}},
        schema_type=OptionalCollectionEnvelope,
    )

    expected = CamelChild(item_name="a", item_count=1)
    assert result == OptionalCollectionEnvelope(
        child=expected, children=(expected,), children_by_key={"arbitrary_key": expected}
    )


def test_msgspec_opt_in_normalizes_declared_nested_fields() -> None:
    child = {"item_name": "python", "itemName": "encoded", "item_count": 1}
    row = {"child": child, "children": [child], "children_by_key": {"arbitrary_key": child}}
    expected = CamelChild(item_name="encoded", item_count=1)

    with pytest.raises(msgspec.ValidationError, match="itemCount"):
        to_schema(row, schema_type=OptionalCollectionEnvelope)

    assert to_schema(row, schema_type=OptionalCollectionEnvelope, normalize_nested=True) == OptionalCollectionEnvelope(
        child=expected, children=(expected,), children_by_key={"arbitrary_key": expected}
    )
    assert child == {"item_name": "python", "itemName": "encoded", "item_count": 1}


def test_msgspec_opt_in_normalizes_nested_list_in_batch() -> None:
    row = {"child_rows": [{"item_name": "a", "itemCount": 1}]}

    assert to_schema([row], schema_type=KebabEnvelope, normalize_nested=True) == [
        KebabEnvelope(child_rows=[CamelChild(item_name="a", item_count=1)])
    ]


def test_msgspec_opt_in_respects_explicit_nested_alias() -> None:
    row = {"child": {"display_name": "Ada"}, "item_count": 1}

    assert to_schema(row, schema_type=CallableAliasEnvelope, normalize_nested=True) == CallableAliasEnvelope(
        child=ExplicitAliasChild(display_name="Ada"), item_count=1
    )


def test_msgspec_optional_field_accepts_none() -> None:
    result = to_schema({"child": None, "children": [], "children_by_key": {}}, schema_type=OptionalCollectionEnvelope)

    assert result == OptionalCollectionEnvelope(child=None, children=(), children_by_key={})


def test_msgspec_already_encoded_keys_are_unchanged() -> None:
    result = to_schema({"rows": [{"itemName": "a", "itemCount": 1}], "total": 1}, schema_type=SingleWordEnvelope)

    assert result == SingleWordEnvelope(rows=[CamelChild(item_name="a", item_count=1)], total=1)


def test_msgspec_encoded_key_wins_over_python_alias() -> None:
    result = to_schema(
        {"rows": [{"item_name": "python", "itemName": "encoded", "item_count": 1, "itemCount": 2}], "total": 1},
        schema_type=SingleWordEnvelope,
    )

    assert result.rows == [CamelChild(item_name="encoded", item_count=2)]


def test_msgspec_arbitrary_mapping_keys_are_preserved() -> None:
    metadata = {"snake_case": {"nested_key": "value"}}
    result = to_schema({"metadata": metadata}, schema_type=ArbitraryPayloadEnvelope)

    assert result.metadata == metadata


def test_msgspec_unknown_fields_remain_validation_errors() -> None:
    with pytest.raises(msgspec.ValidationError, match="unknown_field"):
        to_schema({"child": {"item_name": "a", "item_count": 1}, "unknown_field": True}, schema_type=StrictEnvelope)


def test_msgspec_ambiguous_union_is_not_normalized() -> None:
    payload = {"item_name": "a", "item_count": 1}

    assert schema_utils._normalize_msgspec_input(payload, CamelChild | int) is payload


def test_msgspec_field_plan_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    class CachedStruct(msgspec.Struct, rename="camel"):
        item_name: str

    original_fields = msgspec.structs.fields
    call_count = 0

    def count_fields(schema_type: type) -> tuple[msgspec.structs.FieldInfo, ...]:
        nonlocal call_count
        call_count += 1
        return original_fields(schema_type)

    monkeypatch.setattr(msgspec.structs, "fields", count_fields)
    assert to_schema({"item_name": "a"}, schema_type=CachedStruct) == CachedStruct(item_name="a")
    assert to_schema({"item_name": "b"}, schema_type=CachedStruct) == CachedStruct(item_name="b")
    assert call_count == 1


class Prefs(msgspec.Struct, rename="camel"):
    theme: str = "light"
    is_admin: bool = False


class PrefsUser(msgspec.Struct, rename="camel"):
    id: int
    is_admin: bool = False
    prefs: Prefs | None = None


def test_nested_python_name_does_not_set_renamed_field() -> None:
    payload = {"id": 1, "prefs": {"theme": "dark", "is_admin": True}}

    result = to_schema(payload, schema_type=PrefsUser)

    assert result.prefs is not None
    assert result.prefs.is_admin is False
    assert result == msgspec.convert(payload, PrefsUser)


def test_nested_python_name_does_not_set_renamed_field_in_batch() -> None:
    result = to_schema([{"id": 1, "prefs": {"theme": "dark", "is_admin": True}}], schema_type=PrefsUser)

    assert result[0].prefs is not None
    assert result[0].prefs.is_admin is False


def test_top_level_python_names_still_normalize() -> None:
    assert to_schema({"id": 1, "is_admin": True}, schema_type=PrefsUser).is_admin is True
