"""Tests for to_value_type() value conversion utility."""

import datetime
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, TypedDict
from unittest.mock import patch
from uuid import UUID

import attrs
import msgspec
import pytest
from pydantic import BaseModel

import sqlspec.utils.schema as schema_utils
from sqlspec.utils.schema import to_value_type


def test_foreign_key_metadata_recognized_via_issubclass() -> None:
    """Real ForeignKeyMetadata resolves to a schema converter."""
    from sqlspec.data_dictionary import ForeignKeyMetadata
    from sqlspec.utils.schema import _get_schema_converter

    assert _get_schema_converter(ForeignKeyMetadata) is not None


def test_foreign_key_metadata_conversion_roundtrip() -> None:
    """Dict with FK column data converts to ForeignKeyMetadata."""
    from sqlspec.data_dictionary import ForeignKeyMetadata

    result = to_value_type(
        {
            "table_name": "orders",
            "column_name": "user_id",
            "referenced_table": "users",
            "referenced_column": "id",
            "constraint_name": "fk_orders_user",
        },
        ForeignKeyMetadata,
    )
    assert isinstance(result, ForeignKeyMetadata)
    assert result.table_name == "orders"
    assert result.column_name == "user_id"
    assert result.referenced_table == "users"
    assert result.referenced_column == "id"
    assert result.constraint_name == "fk_orders_user"


def test_unrelated_class_named_foreign_key_metadata_not_matched() -> None:
    """A same-named third-party class must not use the FK metadata converter."""
    from sqlspec.utils.schema import _get_schema_converter

    class ForeignKeyMetadata:
        __slots__ = ("column_name", "referenced_column", "referenced_table", "table_name")

    assert _get_schema_converter(ForeignKeyMetadata) is None


def test_foreign_key_metadata_list_conversion() -> None:
    """A list of FK dictionaries converts to FK metadata instances."""
    from sqlspec.data_dictionary import ForeignKeyMetadata
    from sqlspec.utils.schema import _convert_foreign_key_metadata

    result = _convert_foreign_key_metadata(
        [
            {"table_name": "orders", "column_name": "user_id", "referenced_table": "users", "referenced_column": "id"},
            {"table_name": "items", "column_name": "order_id", "referenced_table": "orders", "referenced_column": "id"},
        ],
        ForeignKeyMetadata,
    )
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(item, ForeignKeyMetadata) for item in result)
    assert result[0].table_name == "orders"
    assert result[1].table_name == "items"


@pytest.mark.parametrize(
    ("value", "target_type"),
    [
        pytest.param(42, int, id="int"),
        pytest.param(3.14, float, id="float"),
        pytest.param("hello", str, id="str"),
        pytest.param(True, bool, id="bool"),
        pytest.param(datetime.datetime(2024, 1, 15, 12, 30, 45), datetime.datetime, id="datetime"),
        pytest.param(datetime.date(2024, 1, 15), datetime.date, id="date"),
        pytest.param(datetime.time(12, 30, 45), datetime.time, id="time"),
        pytest.param(Decimal("123.45"), Decimal, id="decimal"),
        pytest.param(UUID("550e8400-e29b-41d4-a716-446655440000"), UUID, id="uuid"),
        pytest.param(Path("/tmp/test.txt"), Path, id="path"),
        pytest.param({"key": "value"}, dict, id="dict"),
        pytest.param({}, dict, id="empty_dict"),
        pytest.param([1, 2, 3], list, id="list"),
        pytest.param([], list, id="empty_list"),
    ],
)
def test_identity_conversions(value: Any, target_type: type) -> None:
    """Exact type match returns the same object instance."""
    result = to_value_type(value, target_type)
    assert result is value


@pytest.mark.parametrize(
    ("val", "expected"), [pytest.param(True, 1, id="true_to_1"), pytest.param(False, 0, id="false_to_0")]
)
def test_subclass_bug_fixes_bool_to_int(val: bool, expected: int) -> None:
    """Boolean values should convert to actual int instances, not return bool."""
    result = to_value_type(val, int)
    assert result == expected
    assert type(result) is int
    assert result is not val


@pytest.mark.parametrize(
    ("target_type", "expected_type", "expected_value"),
    [
        pytest.param(datetime.date, datetime.date, datetime.date(2024, 1, 15), id="datetime_to_date"),
        pytest.param(datetime.time, datetime.time, datetime.time(12, 30, 45), id="datetime_to_time"),
    ],
)
def test_subclass_bug_fixes_datetime_subtypes(target_type: type, expected_type: type, expected_value: Any) -> None:
    """Datetime instances should convert to strict date or time instances."""
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(dt, target_type)
    assert result == expected_value
    assert type(result) is expected_type
    if target_type is datetime.date:
        assert not isinstance(result, datetime.datetime)


def test_convert_numpy_recursive_preserves_tuple_shape() -> None:
    """Numpy recursive conversion should keep tuple containers intact."""
    if not schema_utils.NUMPY_INSTALLED:
        pytest.skip("numpy is not installed")
    import numpy as np

    payload = {"items": (np.array([1, 2]), {"values": np.array([3, 4])})}
    converted = schema_utils._convert_numpy_recursive(payload)
    assert converted == {"items": ([1, 2], {"values": [3, 4]})}


def test_msgspec_conversion_does_not_walk_numpy_for_plain_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plain msgspec conversion should not recurse through numpy helpers."""
    if not schema_utils.NUMPY_INSTALLED:
        pytest.skip("numpy is not installed")

    class User(msgspec.Struct):
        id: int
        name: str

    def fail_walk(_obj: object) -> object:
        msg = "numpy walk should not run for plain msgspec payloads"
        raise AssertionError(msg)

    monkeypatch.setattr(schema_utils, "_convert_numpy_recursive", fail_walk)
    result = schema_utils._convert_msgspec([{"id": 1, "name": "Alice"}], User)
    assert result == [User(id=1, name="Alice")]


@pytest.mark.skipif(not schema_utils.NUMPY_INSTALLED, reason="numpy is not installed")
def test_msgspec_conversion_falls_back_to_numpy_walk_for_ndarray_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ndarray payloads should still convert through the numpy fallback path."""
    import numpy as np

    class Measurement(msgspec.Struct):
        values: list[float]

    original_walk = schema_utils._convert_numpy_recursive
    call_count = 0

    def count_walk(obj: object) -> object:
        nonlocal call_count
        if isinstance(obj, list):
            call_count += 1
        return original_walk(obj)

    monkeypatch.setattr(schema_utils, "_convert_numpy_recursive", count_walk)
    result = schema_utils._convert_msgspec([{"values": np.array([1.0, 2.0])}], Measurement)
    assert result == [Measurement(values=[1.0, 2.0])]
    assert call_count == 1


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(3.7, 3, id="float_positive_round_down"),
        pytest.param(3.2, 3, id="float_positive_fraction"),
        pytest.param(-3.7, -3, id="float_negative"),
        pytest.param("42", 42, id="str_positive"),
        pytest.param("-123", -123, id="str_negative"),
        pytest.param("42.7", 42, id="str_float_positive"),
        pytest.param("-3.9", -3, id="str_float_negative"),
        pytest.param(Decimal("42.7"), 42, id="decimal"),
    ],
)
def test_int_conversions(value: Any, expected: int) -> None:
    """Values convert to integer with truncation where applicable."""
    assert to_value_type(value, int) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(42, 42.0, id="int"),
        pytest.param("3.14", 3.14, id="str_positive"),
        pytest.param("-2.5", -2.5, id="str_negative"),
        pytest.param(Decimal("3.14159"), 3.14159, id="decimal"),
        pytest.param(True, 1.0, id="bool_true"),
        pytest.param(False, 0.0, id="bool_false"),
    ],
)
def test_float_conversions(value: Any, expected: float) -> None:
    """Values convert to float with matching numeric precision."""
    assert to_value_type(value, float) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(42, "42", id="int"),
        pytest.param(3.14, "3.14", id="float"),
        pytest.param(True, "True", id="bool_true"),
        pytest.param(False, "False", id="bool_false"),
        pytest.param(UUID("550e8400-e29b-41d4-a716-446655440000"), "550e8400-e29b-41d4-a716-446655440000", id="uuid"),
    ],
)
def test_str_conversions(value: Any, expected: str) -> None:
    """Values convert to string representation."""
    assert to_value_type(value, str) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(1, True, id="int_one"),
        pytest.param(0, False, id="int_zero"),
        pytest.param(42, True, id="int_positive"),
        pytest.param(1.0, True, id="float_one"),
        pytest.param(0.0, False, id="float_zero"),
        pytest.param(0.1, True, id="float_fraction"),
    ],
)
def test_bool_numeric_conversions(value: Any, expected: bool) -> None:
    """Numeric values convert to bool according to zero/non-zero rules."""
    assert to_value_type(value, bool) is expected


@pytest.mark.parametrize("val", ["true", "True", "TRUE", "1", "yes", "Yes", "y", "Y", "t", "T", "on", "ON"])
def test_bool_conversion_str_true_values(val: str) -> None:
    """String representations of truth convert to True."""
    assert to_value_type(val, bool) is True


@pytest.mark.parametrize(
    "val", ["false", "False", "FALSE", "0", "no", "No", "n", "N", "f", "F", "off", "OFF", "", "anything"]
)
def test_bool_conversion_str_false_values(val: str) -> None:
    """String representations of falsity or empty strings convert to False."""
    assert to_value_type(val, bool) is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2024-01-15T12:30:45", datetime.datetime(2024, 1, 15, 12, 30, 45), id="iso_str"),
        pytest.param(datetime.date(2024, 1, 15), datetime.datetime(2024, 1, 15, 0, 0, 0), id="date_to_datetime"),
    ],
)
def test_datetime_conversions(value: Any, expected: datetime.datetime) -> None:
    """Values convert to datetime instances."""
    assert to_value_type(value, datetime.datetime) == expected


def test_datetime_conversion_str_iso_with_tz_to_datetime() -> None:
    """ISO format string with timezone converts to datetime."""
    result = to_value_type("2024-01-15T12:30:45+00:00", datetime.datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("2024-01-15", datetime.date(2024, 1, 15), id="iso_date_str"),
        pytest.param("2024-01-15T12:30:45", datetime.date(2024, 1, 15), id="iso_datetime_str"),
        pytest.param(datetime.datetime(2024, 1, 15, 12, 30, 45), datetime.date(2024, 1, 15), id="datetime_instance"),
    ],
)
def test_date_conversions(value: Any, expected: datetime.date) -> None:
    """Values convert to date instances."""
    assert to_value_type(value, datetime.date) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("12:30:45", datetime.time(12, 30, 45), id="iso_time_str"),
        pytest.param(datetime.datetime(2024, 1, 15, 12, 30, 45), datetime.time(12, 30, 45), id="datetime_instance"),
    ],
)
def test_time_conversions(value: Any, expected: datetime.time) -> None:
    """Values convert to time instances."""
    assert to_value_type(value, datetime.time) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(42, Decimal(42), id="int"),
        pytest.param(3.14, Decimal("3.14"), id="float"),
        pytest.param("123.456789", Decimal("123.456789"), id="str"),
    ],
)
def test_decimal_conversions(value: Any, expected: Decimal) -> None:
    """Values convert to Decimal instances."""
    assert to_value_type(value, Decimal) == expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("550e8400-e29b-41d4-a716-446655440000", id="str_lowercase"),
        pytest.param("550E8400-E29B-41D4-A716-446655440000", id="str_uppercase"),
        pytest.param(UUID("550e8400-e29b-41d4-a716-446655440000").bytes, id="bytes"),
    ],
)
def test_uuid_conversions(value: Any) -> None:
    """Values convert to UUID instances."""
    assert to_value_type(value, UUID) == UUID("550e8400-e29b-41d4-a716-446655440000")


@pytest.mark.parametrize(
    "value", [pytest.param("/tmp/test.txt", id="str"), pytest.param(PurePosixPath("/tmp/test.txt"), id="pure_path")]
)
def test_path_conversions(value: Any) -> None:
    """Values convert to Path instances."""
    assert to_value_type(value, Path) == Path("/tmp/test.txt")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param('{"key": "value", "count": 42}', {"key": "value", "count": 42}, id="flat_json"),
        pytest.param('{"outer": {"inner": [1, 2, 3]}}', {"outer": {"inner": [1, 2, 3]}}, id="nested_json"),
        pytest.param("{}", {}, id="empty_json"),
    ],
)
def test_dict_conversions(value: str, expected: "dict[str, Any]") -> None:
    """JSON strings convert to dictionary structures."""
    assert to_value_type(value, dict) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param('[1, 2, 3, "four"]', [1, 2, 3, "four"], id="flat_json_array"),
        pytest.param("[[1, 2], [3, 4]]", [[1, 2], [3, 4]], id="nested_json_array"),
        pytest.param("[]", [], id="empty_json_array"),
        pytest.param((1, 2, 3), [1, 2, 3], id="tuple"),
    ],
)
def test_list_conversions(value: Any, expected: "list[Any]") -> None:
    """Sequences and JSON array strings convert to list structures."""
    assert to_value_type(value, list) == expected


@pytest.mark.parametrize(
    "value", [pytest.param({1, 2, 3}, id="set"), pytest.param(frozenset({1, 2, 3}), id="frozenset")]
)
def test_set_to_list_conversions(value: Any) -> None:
    """Sets and frozensets convert to lists."""
    result = to_value_type(value, list)
    assert sorted(result) == [1, 2, 3]


@pytest.mark.parametrize(
    ("value", "target_type", "match"),
    [
        pytest.param("not a number", int, "Cannot convert str to int", id="str_to_int"),
        pytest.param("not a number", float, "Cannot convert str to float", id="str_to_float"),
        pytest.param("not a date", datetime.datetime, "Cannot convert str to datetime", id="str_to_datetime"),
        pytest.param("not a date", datetime.date, "Cannot convert str to date", id="str_to_date"),
        pytest.param("not a time", datetime.time, "Cannot convert str to time", id="str_to_time"),
        pytest.param("not a number", Decimal, "Cannot convert str to Decimal", id="str_to_decimal"),
        pytest.param("not-a-uuid", UUID, "Cannot convert str to UUID", id="str_to_uuid"),
        pytest.param(123, Path, "Cannot convert int to Path", id="int_to_path"),
        pytest.param("[1, 2, 3]", dict, "JSON string did not parse to dict", id="json_array_to_dict"),
        pytest.param("not json", dict, "Cannot convert str to dict", id="invalid_json_to_dict"),
        pytest.param('{"key": "value"}', list, "JSON string did not parse to list", id="json_obj_to_list"),
        pytest.param("not json", list, "Cannot convert str to list", id="invalid_json_to_list"),
    ],
)
def test_conversion_type_errors(value: Any, target_type: type, match: str) -> None:
    """Invalid input representations raise expected TypeError on conversion."""
    with pytest.raises(TypeError, match=match):
        to_value_type(value, target_type)


def test_fallback_conversion_custom_type_with_constructor() -> None:
    """Custom type with constructor from value works."""

    class CustomInt:
        def __init__(self, value: int) -> None:
            self.value = value

        def __eq__(self, other: object) -> bool:
            if isinstance(other, CustomInt):
                return self.value == other.value
            return False

    result = to_value_type(42, CustomInt)
    assert isinstance(result, CustomInt)
    assert result.value == 42


def test_fallback_conversion_unsupported_conversion_raises() -> None:
    """Conversion to unsupported type raises TypeError."""

    class NoConstructor:
        def __init__(self) -> None:
            pass

    with pytest.raises(TypeError, match="Cannot convert"):
        to_value_type("value", NoConstructor)


class UserPydantic(BaseModel):
    """Pydantic model for testing."""

    name: str
    email: str


@dataclass
class UserDataclass:
    """Dataclass for testing."""

    name: str
    email: str


class UserMsgspec(msgspec.Struct):
    """Msgspec struct for testing."""

    name: str
    email: str


@attrs.define
class UserAttrs:
    """Attrs class for testing."""

    name: str
    email: str


class UserTypedDict(TypedDict):
    """TypedDict for testing."""

    name: str
    email: str


@pytest.mark.parametrize(
    ("model_cls", "payload"),
    [
        pytest.param(UserPydantic, {"name": "Alice", "email": "alice@example.com"}, id="pydantic_dict"),
        pytest.param(UserPydantic, '{"name": "Bob", "email": "bob@example.com"}', id="pydantic_json"),
        pytest.param(UserDataclass, {"name": "Alice", "email": "alice@example.com"}, id="dataclass_dict"),
        pytest.param(UserDataclass, '{"name": "Bob", "email": "bob@example.com"}', id="dataclass_json"),
        pytest.param(UserMsgspec, {"name": "Alice", "email": "alice@example.com"}, id="msgspec_dict"),
        pytest.param(UserMsgspec, '{"name": "Bob", "email": "bob@example.com"}', id="msgspec_json"),
        pytest.param(UserAttrs, {"name": "Alice", "email": "alice@example.com"}, id="attrs_dict"),
        pytest.param(UserAttrs, '{"name": "Bob", "email": "bob@example.com"}', id="attrs_json"),
    ],
)
def test_schema_model_conversions(model_cls: type, payload: Any) -> None:
    """Dicts and JSON strings convert to supported schema model instances."""
    result = to_value_type(payload, model_cls)
    assert isinstance(result, model_cls)
    if isinstance(payload, str):
        assert result.name == "Bob"
        assert result.email == "bob@example.com"
    else:
        assert result.name == "Alice"
        assert result.email == "alice@example.com"


def test_pydantic_conversion_pydantic_identity() -> None:
    """Pydantic model instance passes through (via to_schema validation)."""
    user = UserPydantic(name="Charlie", email="charlie@example.com")
    result = to_value_type(user, UserPydantic)
    assert isinstance(result, UserPydantic)
    assert result.name == "Charlie"


def test_pydantic_conversion_schema_conversion_uses_cached_converter_path() -> None:
    """Schema conversion should not re-enter schema-type detection before dispatch."""
    data = {"name": "Alice", "email": "alice@example.com"}
    schema_utils._SCHEMA_CONVERTER_CACHE[UserPydantic] = schema_utils._convert_pydantic
    with patch.object(schema_utils, "is_pydantic_model", side_effect=AssertionError("unexpected schema detection")):
        result = to_value_type(data, UserPydantic)
    assert isinstance(result, UserPydantic)
    assert result.name == "Alice"


@pytest.mark.parametrize(
    ("payload", "expected_name", "expected_email"),
    [
        pytest.param({"name": "Alice", "email": "alice@example.com"}, "Alice", "alice@example.com", id="dict"),
        pytest.param('{"name": "Bob", "email": "bob@example.com"}', "Bob", "bob@example.com", id="json_str"),
    ],
)
def test_typed_dict_conversions(payload: Any, expected_name: str, expected_email: str) -> None:
    """Dict and JSON strings convert to TypedDict mapping representations."""
    result = to_value_type(payload, UserTypedDict)
    assert isinstance(result, dict)
    assert result["name"] == expected_name
    assert result["email"] == expected_email


def test_schema_type_edge_cases_nested_json_to_pydantic() -> None:
    """Nested JSON converts to Pydantic model with nested data."""

    class Profile(BaseModel):
        bio: str
        followers: int

    class UserWithProfile(BaseModel):
        name: str
        profile: Profile

    json_str = '{"name": "Alice", "profile": {"bio": "Developer", "followers": 100}}'
    result = to_value_type(json_str, UserWithProfile)
    assert isinstance(result, UserWithProfile)
    assert result.name == "Alice"
    assert result.profile.bio == "Developer"
    assert result.profile.followers == 100


def test_schema_type_edge_cases_invalid_json_string_fallback() -> None:
    """Invalid JSON string is passed as-is to schema converter (which will fail)."""
    with pytest.raises(Exception):
        to_value_type("not valid json", UserPydantic)


def test_schema_type_edge_cases_already_parsed_dict_works() -> None:
    """Pre-parsed dict (from DB driver) works correctly."""
    data = {"name": "Charlie", "email": "charlie@example.com"}
    result = to_value_type(data, UserMsgspec)
    assert isinstance(result, UserMsgspec)
    assert result.name == "Charlie"
