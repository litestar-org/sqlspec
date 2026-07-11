"""Tests for to_value_type() value conversion utility."""

import datetime
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import TypedDict
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


def test_identity_conversions_int_identity() -> None:
    """Integer with exact type match returns the same object."""
    value = 42
    result = to_value_type(value, int)
    assert result is value
    assert result == 42


def test_identity_conversions_float_identity() -> None:
    """Float with exact type match returns the same object."""
    value = 3.14
    result = to_value_type(value, float)
    assert result is value


def test_identity_conversions_str_identity() -> None:
    """String with exact type match returns the same object."""
    value = "hello"
    result = to_value_type(value, str)
    assert result is value


def test_identity_conversions_bool_identity() -> None:
    """Boolean with exact type match returns the same object."""
    value = True
    result = to_value_type(value, bool)
    assert result is value


def test_identity_conversions_datetime_identity() -> None:
    """Datetime with exact type match returns the same object."""
    value = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(value, datetime.datetime)
    assert result is value


def test_identity_conversions_date_identity() -> None:
    """Date with exact type match returns the same object."""
    value = datetime.date(2024, 1, 15)
    result = to_value_type(value, datetime.date)
    assert result is value


def test_identity_conversions_time_identity() -> None:
    """Time with exact type match returns the same object."""
    value = datetime.time(12, 30, 45)
    result = to_value_type(value, datetime.time)
    assert result is value


def test_identity_conversions_decimal_identity() -> None:
    """Decimal with exact type match returns the same object."""
    value = Decimal("123.45")
    result = to_value_type(value, Decimal)
    assert result is value


def test_identity_conversions_uuid_identity() -> None:
    """UUID with exact type match returns the same object."""
    value = UUID("550e8400-e29b-41d4-a716-446655440000")
    result = to_value_type(value, UUID)
    assert result is value


def test_identity_conversions_path_identity() -> None:
    """Path with exact type match returns the same object."""
    value = Path("/tmp/test.txt")
    result = to_value_type(value, Path)
    assert result is value


def test_identity_conversions_dict_identity() -> None:
    """Dict with exact type match returns the same object."""
    value = {"key": "value"}
    result = to_value_type(value, dict)
    assert result is value


def test_identity_conversions_list_identity() -> None:
    """List with exact type match returns the same object."""
    value = [1, 2, 3]
    result = to_value_type(value, list)
    assert result is value


def test_subclass_bug_fixes_bool_to_int_converts_true() -> None:
    """True should convert to 1 (not return True)."""
    result = to_value_type(True, int)
    assert result == 1
    assert type(result) is int
    assert result is not True


def test_subclass_bug_fixes_bool_to_int_converts_false() -> None:
    """False should convert to 0 (not return False)."""
    result = to_value_type(False, int)
    assert result == 0
    assert type(result) is int
    assert result is not False


def test_subclass_bug_fixes_datetime_to_date_converts() -> None:
    """Datetime should convert to date (not return datetime)."""
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(dt, datetime.date)
    assert result == datetime.date(2024, 1, 15)
    assert type(result) is datetime.date
    assert not isinstance(result, datetime.datetime)


def test_subclass_bug_fixes_datetime_to_time_converts() -> None:
    """Datetime should convert to time."""
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(dt, datetime.time)
    assert result == datetime.time(12, 30, 45)
    assert type(result) is datetime.time


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


def test_int_conversion_float_to_int() -> None:
    """Float truncates to int."""
    assert to_value_type(3.7, int) == 3
    assert to_value_type(3.2, int) == 3
    assert to_value_type(-3.7, int) == -3


def test_int_conversion_str_to_int() -> None:
    """String with integer value converts to int."""
    assert to_value_type("42", int) == 42
    assert to_value_type("-123", int) == -123


def test_int_conversion_str_float_to_int() -> None:
    """String with float value converts to int (truncated)."""
    assert to_value_type("42.7", int) == 42
    assert to_value_type("-3.9", int) == -3


def test_int_conversion_decimal_to_int() -> None:
    """Decimal converts to int (truncated)."""
    assert to_value_type(Decimal("42.7"), int) == 42


def test_int_conversion_invalid_str_to_int_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to int"):
        to_value_type("not a number", int)


def test_float_conversion_int_to_float() -> None:
    """Integer converts to float."""
    assert to_value_type(42, float) == 42.0


def test_float_conversion_str_to_float() -> None:
    """String with numeric value converts to float."""
    assert to_value_type("3.14", float) == 3.14
    assert to_value_type("-2.5", float) == -2.5


def test_float_conversion_decimal_to_float() -> None:
    """Decimal converts to float."""
    assert to_value_type(Decimal("3.14159"), float) == pytest.approx(3.14159)


def test_float_conversion_bool_to_float() -> None:
    """Boolean converts to float."""
    assert to_value_type(True, float) == 1.0
    assert to_value_type(False, float) == 0.0


def test_float_conversion_invalid_str_to_float_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to float"):
        to_value_type("not a number", float)


def test_str_conversion_int_to_str() -> None:
    """Integer converts to string."""
    assert to_value_type(42, str) == "42"


def test_str_conversion_float_to_str() -> None:
    """Float converts to string."""
    assert to_value_type(3.14, str) == "3.14"


def test_str_conversion_bool_to_str() -> None:
    """Boolean converts to string."""
    assert to_value_type(True, str) == "True"
    assert to_value_type(False, str) == "False"


def test_str_conversion_uuid_to_str() -> None:
    """UUID converts to string."""
    uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
    assert to_value_type(uuid, str) == "550e8400-e29b-41d4-a716-446655440000"


def test_bool_conversion_int_to_bool() -> None:
    """Integer converts to bool."""
    assert to_value_type(1, bool) is True
    assert to_value_type(0, bool) is False
    assert to_value_type(42, bool) is True


def test_bool_conversion_str_true_values_to_bool() -> None:
    """String true values convert to True."""
    for val in ["true", "True", "TRUE", "1", "yes", "Yes", "y", "Y", "t", "T", "on", "ON"]:
        assert to_value_type(val, bool) is True, f"Expected '{val}' to be True"


def test_bool_conversion_str_false_values_to_bool() -> None:
    """String false values convert to False."""
    for val in ["false", "False", "FALSE", "0", "no", "No", "n", "N", "f", "F", "off", "OFF", "", "anything"]:
        assert to_value_type(val, bool) is False, f"Expected '{val}' to be False"


def test_bool_conversion_float_to_bool() -> None:
    """Float converts to bool."""
    assert to_value_type(1.0, bool) is True
    assert to_value_type(0.0, bool) is False
    assert to_value_type(0.1, bool) is True


def test_datetime_conversion_str_iso_to_datetime() -> None:
    """ISO format string converts to datetime."""
    result = to_value_type("2024-01-15T12:30:45", datetime.datetime)
    assert result == datetime.datetime(2024, 1, 15, 12, 30, 45)


def test_datetime_conversion_str_iso_with_tz_to_datetime() -> None:
    """ISO format string with timezone converts to datetime."""
    result = to_value_type("2024-01-15T12:30:45+00:00", datetime.datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_datetime_conversion_date_to_datetime() -> None:
    """Date converts to datetime at midnight."""
    date = datetime.date(2024, 1, 15)
    result = to_value_type(date, datetime.datetime)
    assert result == datetime.datetime(2024, 1, 15, 0, 0, 0)


def test_datetime_conversion_invalid_str_to_datetime_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to datetime"):
        to_value_type("not a date", datetime.datetime)


def test_date_conversion_str_iso_to_date() -> None:
    """ISO format string converts to date."""
    result = to_value_type("2024-01-15", datetime.date)
    assert result == datetime.date(2024, 1, 15)


def test_date_conversion_str_datetime_to_date() -> None:
    """Datetime string extracts date portion."""
    result = to_value_type("2024-01-15T12:30:45", datetime.date)
    assert result == datetime.date(2024, 1, 15)


def test_date_conversion_datetime_to_date() -> None:
    """Datetime extracts date portion."""
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(dt, datetime.date)
    assert result == datetime.date(2024, 1, 15)


def test_date_conversion_invalid_str_to_date_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to date"):
        to_value_type("not a date", datetime.date)


def test_time_conversion_str_iso_to_time() -> None:
    """ISO format string converts to time."""
    result = to_value_type("12:30:45", datetime.time)
    assert result == datetime.time(12, 30, 45)


def test_time_conversion_datetime_to_time() -> None:
    """Datetime extracts time portion."""
    dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
    result = to_value_type(dt, datetime.time)
    assert result == datetime.time(12, 30, 45)


def test_time_conversion_invalid_str_to_time_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to time"):
        to_value_type("not a time", datetime.time)


def test_decimal_conversion_int_to_decimal() -> None:
    """Integer converts to Decimal."""
    result = to_value_type(42, Decimal)
    assert result == Decimal(42)


def test_decimal_conversion_float_to_decimal() -> None:
    """Float converts to Decimal (via string for precision)."""
    result = to_value_type(3.14, Decimal)
    assert result == Decimal("3.14")


def test_decimal_conversion_str_to_decimal() -> None:
    """String converts to Decimal."""
    result = to_value_type("123.456789", Decimal)
    assert result == Decimal("123.456789")


def test_decimal_conversion_invalid_str_to_decimal_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to Decimal"):
        to_value_type("not a number", Decimal)


def test_uuid_conversion_str_to_uuid() -> None:
    """UUID string converts to UUID."""
    result = to_value_type("550e8400-e29b-41d4-a716-446655440000", UUID)
    assert result == UUID("550e8400-e29b-41d4-a716-446655440000")


def test_uuid_conversion_str_uppercase_to_uuid() -> None:
    """Uppercase UUID string converts to UUID."""
    result = to_value_type("550E8400-E29B-41D4-A716-446655440000", UUID)
    assert result == UUID("550e8400-e29b-41d4-a716-446655440000")


def test_uuid_conversion_bytes_to_uuid() -> None:
    """Bytes converts to UUID."""
    uuid_bytes = UUID("550e8400-e29b-41d4-a716-446655440000").bytes
    result = to_value_type(uuid_bytes, UUID)
    assert result == UUID("550e8400-e29b-41d4-a716-446655440000")


def test_uuid_conversion_invalid_str_to_uuid_raises() -> None:
    """Invalid string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to UUID"):
        to_value_type("not-a-uuid", UUID)


def test_path_conversion_str_to_path() -> None:
    """String converts to Path."""
    result = to_value_type("/tmp/test.txt", Path)
    assert result == Path("/tmp/test.txt")


def test_path_conversion_pure_path_to_path() -> None:
    """PurePath converts to Path."""
    pure = PurePosixPath("/tmp/test.txt")
    result = to_value_type(pure, Path)
    assert result == Path("/tmp/test.txt")


def test_path_conversion_invalid_type_to_path_raises() -> None:
    """Invalid type raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert int to Path"):
        to_value_type(123, Path)


def test_dict_conversion_json_str_to_dict() -> None:
    """JSON string converts to dict."""
    result = to_value_type('{"key": "value", "count": 42}', dict)
    assert result == {"key": "value", "count": 42}


def test_dict_conversion_json_nested_to_dict() -> None:
    """Nested JSON string converts to dict."""
    result = to_value_type('{"outer": {"inner": [1, 2, 3]}}', dict)
    assert result == {"outer": {"inner": [1, 2, 3]}}


def test_dict_conversion_json_array_to_dict_raises() -> None:
    """JSON array string raises TypeError when converting to dict."""
    with pytest.raises(TypeError, match="JSON string did not parse to dict"):
        to_value_type("[1, 2, 3]", dict)


def test_dict_conversion_invalid_json_to_dict_raises() -> None:
    """Invalid JSON string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to dict"):
        to_value_type("not json", dict)


def test_list_conversion_json_array_str_to_list() -> None:
    """JSON array string converts to list."""
    result = to_value_type('[1, 2, 3, "four"]', list)
    assert result == [1, 2, 3, "four"]


def test_list_conversion_json_nested_array_to_list() -> None:
    """Nested JSON array converts to list."""
    result = to_value_type("[[1, 2], [3, 4]]", list)
    assert result == [[1, 2], [3, 4]]


def test_list_conversion_json_object_to_list_raises() -> None:
    """JSON object string raises TypeError when converting to list."""
    with pytest.raises(TypeError, match="JSON string did not parse to list"):
        to_value_type('{"key": "value"}', list)


def test_list_conversion_tuple_to_list() -> None:
    """Tuple converts to list."""
    result = to_value_type((1, 2, 3), list)
    assert result == [1, 2, 3]


def test_list_conversion_set_to_list() -> None:
    """Set converts to list (order may vary)."""
    result = to_value_type({1, 2, 3}, list)
    assert sorted(result) == [1, 2, 3]


def test_list_conversion_frozenset_to_list() -> None:
    """Frozenset converts to list (order may vary)."""
    result = to_value_type(frozenset({1, 2, 3}), list)
    assert sorted(result) == [1, 2, 3]


def test_list_conversion_invalid_json_to_list_raises() -> None:
    """Invalid JSON string raises TypeError."""
    with pytest.raises(TypeError, match="Cannot convert str to list"):
        to_value_type("not json", list)


def test_edge_cases_empty_string_to_bool_is_false() -> None:
    """Empty string converts to False."""
    assert to_value_type("", bool) is False


def test_edge_cases_zero_to_bool_is_false() -> None:
    """Zero converts to False."""
    assert to_value_type(0, bool) is False
    assert to_value_type(0.0, bool) is False


def test_edge_cases_empty_dict_preserved() -> None:
    """Empty dict is preserved."""
    value: dict[str, str] = {}
    result = to_value_type(value, dict)
    assert result == {}
    assert result is value


def test_edge_cases_empty_list_preserved() -> None:
    """Empty list is preserved."""
    value: list[int] = []
    result = to_value_type(value, list)
    assert result == []
    assert result is value


def test_edge_cases_empty_json_object_to_dict() -> None:
    """Empty JSON object converts to empty dict."""
    result = to_value_type("{}", dict)
    assert result == {}


def test_edge_cases_empty_json_array_to_list() -> None:
    """Empty JSON array converts to empty list."""
    result = to_value_type("[]", list)
    assert result == []


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


def test_pydantic_conversion_dict_to_pydantic() -> None:
    """Dict converts to Pydantic model."""
    data = {"name": "Alice", "email": "alice@example.com"}
    result = to_value_type(data, UserPydantic)
    assert isinstance(result, UserPydantic)
    assert result.name == "Alice"
    assert result.email == "alice@example.com"


def test_pydantic_conversion_json_string_to_pydantic() -> None:
    """JSON string converts to Pydantic model."""
    json_str = '{"name": "Bob", "email": "bob@example.com"}'
    result = to_value_type(json_str, UserPydantic)
    assert isinstance(result, UserPydantic)
    assert result.name == "Bob"
    assert result.email == "bob@example.com"


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


def test_dataclass_conversion_dict_to_dataclass() -> None:
    """Dict converts to dataclass."""
    data = {"name": "Alice", "email": "alice@example.com"}
    result = to_value_type(data, UserDataclass)
    assert isinstance(result, UserDataclass)
    assert result.name == "Alice"
    assert result.email == "alice@example.com"


def test_dataclass_conversion_json_string_to_dataclass() -> None:
    """JSON string converts to dataclass."""
    json_str = '{"name": "Bob", "email": "bob@example.com"}'
    result = to_value_type(json_str, UserDataclass)
    assert isinstance(result, UserDataclass)
    assert result.name == "Bob"
    assert result.email == "bob@example.com"


def test_msgspec_conversion_dict_to_msgspec() -> None:
    """Dict converts to msgspec Struct."""
    data = {"name": "Alice", "email": "alice@example.com"}
    result = to_value_type(data, UserMsgspec)
    assert isinstance(result, UserMsgspec)
    assert result.name == "Alice"
    assert result.email == "alice@example.com"


def test_msgspec_conversion_json_string_to_msgspec() -> None:
    """JSON string converts to msgspec Struct."""
    json_str = '{"name": "Bob", "email": "bob@example.com"}'
    result = to_value_type(json_str, UserMsgspec)
    assert isinstance(result, UserMsgspec)
    assert result.name == "Bob"
    assert result.email == "bob@example.com"


def test_attrs_conversion_dict_to_attrs() -> None:
    """Dict converts to attrs class."""
    data = {"name": "Alice", "email": "alice@example.com"}
    result = to_value_type(data, UserAttrs)
    assert isinstance(result, UserAttrs)
    assert result.name == "Alice"
    assert result.email == "alice@example.com"


def test_attrs_conversion_json_string_to_attrs() -> None:
    """JSON string converts to attrs class."""
    json_str = '{"name": "Bob", "email": "bob@example.com"}'
    result = to_value_type(json_str, UserAttrs)
    assert isinstance(result, UserAttrs)
    assert result.name == "Bob"
    assert result.email == "bob@example.com"


def test_typed_dict_conversion_dict_to_typed_dict() -> None:
    """Dict converts to TypedDict (returns dict since TypedDict is runtime dict)."""
    data = {"name": "Alice", "email": "alice@example.com"}
    result = to_value_type(data, UserTypedDict)
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["email"] == "alice@example.com"


def test_typed_dict_conversion_json_string_to_typed_dict() -> None:
    """JSON string converts to TypedDict."""
    json_str = '{"name": "Bob", "email": "bob@example.com"}'
    result = to_value_type(json_str, UserTypedDict)
    assert isinstance(result, dict)
    assert result["name"] == "Bob"
    assert result["email"] == "bob@example.com"


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
