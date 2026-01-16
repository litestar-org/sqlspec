"""Tests for to_value_type() value conversion utility."""

import datetime
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import TypedDict
from uuid import UUID

import attrs
import msgspec
import pytest
from pydantic import BaseModel

from sqlspec.utils.schema import to_value_type

# =============================================================================
# Identity Tests - Fast Path (exact type match returns same object)
# =============================================================================


class TestIdentityConversions:
    """Tests for identity conversions where value is already the correct type."""

    def test_int_identity(self) -> None:
        """Integer with exact type match returns the same object."""
        value = 42
        result = to_value_type(value, int)
        assert result is value
        assert result == 42

    def test_float_identity(self) -> None:
        """Float with exact type match returns the same object."""
        value = 3.14
        result = to_value_type(value, float)
        assert result is value

    def test_str_identity(self) -> None:
        """String with exact type match returns the same object."""
        value = "hello"
        result = to_value_type(value, str)
        assert result is value

    def test_bool_identity(self) -> None:
        """Boolean with exact type match returns the same object."""
        value = True
        result = to_value_type(value, bool)
        assert result is value

    def test_datetime_identity(self) -> None:
        """Datetime with exact type match returns the same object."""
        value = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = to_value_type(value, datetime.datetime)
        assert result is value

    def test_date_identity(self) -> None:
        """Date with exact type match returns the same object."""
        value = datetime.date(2024, 1, 15)
        result = to_value_type(value, datetime.date)
        assert result is value

    def test_time_identity(self) -> None:
        """Time with exact type match returns the same object."""
        value = datetime.time(12, 30, 45)
        result = to_value_type(value, datetime.time)
        assert result is value

    def test_decimal_identity(self) -> None:
        """Decimal with exact type match returns the same object."""
        value = Decimal("123.45")
        result = to_value_type(value, Decimal)
        assert result is value

    def test_uuid_identity(self) -> None:
        """UUID with exact type match returns the same object."""
        value = UUID("550e8400-e29b-41d4-a716-446655440000")
        result = to_value_type(value, UUID)
        assert result is value

    def test_path_identity(self) -> None:
        """Path with exact type match returns the same object."""
        value = Path("/tmp/test.txt")
        result = to_value_type(value, Path)
        assert result is value

    def test_dict_identity(self) -> None:
        """Dict with exact type match returns the same object."""
        value = {"key": "value"}
        result = to_value_type(value, dict)
        assert result is value

    def test_list_identity(self) -> None:
        """List with exact type match returns the same object."""
        value = [1, 2, 3]
        result = to_value_type(value, list)
        assert result is value


# =============================================================================
# Critical Bug Fix Tests - Subclass Relationships
# =============================================================================


class TestSubclassBugFixes:
    """Tests verifying correct handling of problematic subclass relationships.

    These tests validate the critical bug fix where:
    - bool is a subclass of int (isinstance(True, int) is True)
    - datetime is a subclass of date (isinstance(datetime(...), date) is True)
    """

    def test_bool_to_int_converts_true(self) -> None:
        """True should convert to 1 (not return True)."""
        result = to_value_type(True, int)
        assert result == 1
        assert type(result) is int
        assert result is not True  # Must be int, not bool

    def test_bool_to_int_converts_false(self) -> None:
        """False should convert to 0 (not return False)."""
        result = to_value_type(False, int)
        assert result == 0
        assert type(result) is int
        assert result is not False  # Must be int, not bool

    def test_datetime_to_date_converts(self) -> None:
        """Datetime should convert to date (not return datetime)."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = to_value_type(dt, datetime.date)
        assert result == datetime.date(2024, 1, 15)
        assert type(result) is datetime.date
        assert not isinstance(result, datetime.datetime)

    def test_datetime_to_time_converts(self) -> None:
        """Datetime should convert to time."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = to_value_type(dt, datetime.time)
        assert result == datetime.time(12, 30, 45)
        assert type(result) is datetime.time


# =============================================================================
# Integer Conversion Tests
# =============================================================================


class TestIntConversion:
    """Tests for converting values to int."""

    def test_float_to_int(self) -> None:
        """Float truncates to int."""
        assert to_value_type(3.7, int) == 3
        assert to_value_type(3.2, int) == 3
        assert to_value_type(-3.7, int) == -3

    def test_str_to_int(self) -> None:
        """String with integer value converts to int."""
        assert to_value_type("42", int) == 42
        assert to_value_type("-123", int) == -123

    def test_str_float_to_int(self) -> None:
        """String with float value converts to int (truncated)."""
        assert to_value_type("42.7", int) == 42
        assert to_value_type("-3.9", int) == -3

    def test_decimal_to_int(self) -> None:
        """Decimal converts to int (truncated)."""
        assert to_value_type(Decimal("42.7"), int) == 42

    def test_invalid_str_to_int_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to int"):
            to_value_type("not a number", int)


# =============================================================================
# Float Conversion Tests
# =============================================================================


class TestFloatConversion:
    """Tests for converting values to float."""

    def test_int_to_float(self) -> None:
        """Integer converts to float."""
        assert to_value_type(42, float) == 42.0

    def test_str_to_float(self) -> None:
        """String with numeric value converts to float."""
        assert to_value_type("3.14", float) == 3.14
        assert to_value_type("-2.5", float) == -2.5

    def test_decimal_to_float(self) -> None:
        """Decimal converts to float."""
        assert to_value_type(Decimal("3.14159"), float) == pytest.approx(3.14159)

    def test_bool_to_float(self) -> None:
        """Boolean converts to float."""
        assert to_value_type(True, float) == 1.0
        assert to_value_type(False, float) == 0.0

    def test_invalid_str_to_float_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to float"):
            to_value_type("not a number", float)


# =============================================================================
# String Conversion Tests
# =============================================================================


class TestStrConversion:
    """Tests for converting values to str."""

    def test_int_to_str(self) -> None:
        """Integer converts to string."""
        assert to_value_type(42, str) == "42"

    def test_float_to_str(self) -> None:
        """Float converts to string."""
        assert to_value_type(3.14, str) == "3.14"

    def test_bool_to_str(self) -> None:
        """Boolean converts to string."""
        assert to_value_type(True, str) == "True"
        assert to_value_type(False, str) == "False"

    def test_uuid_to_str(self) -> None:
        """UUID converts to string."""
        uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert to_value_type(uuid, str) == "550e8400-e29b-41d4-a716-446655440000"


# =============================================================================
# Boolean Conversion Tests
# =============================================================================


class TestBoolConversion:
    """Tests for converting values to bool."""

    def test_int_to_bool(self) -> None:
        """Integer converts to bool."""
        assert to_value_type(1, bool) is True
        assert to_value_type(0, bool) is False
        assert to_value_type(42, bool) is True

    def test_str_true_values_to_bool(self) -> None:
        """String true values convert to True."""
        for val in ["true", "True", "TRUE", "1", "yes", "Yes", "y", "Y", "t", "T", "on", "ON"]:
            assert to_value_type(val, bool) is True, f"Expected '{val}' to be True"

    def test_str_false_values_to_bool(self) -> None:
        """String false values convert to False."""
        for val in ["false", "False", "FALSE", "0", "no", "No", "n", "N", "f", "F", "off", "OFF", "", "anything"]:
            assert to_value_type(val, bool) is False, f"Expected '{val}' to be False"

    def test_float_to_bool(self) -> None:
        """Float converts to bool."""
        assert to_value_type(1.0, bool) is True
        assert to_value_type(0.0, bool) is False
        assert to_value_type(0.1, bool) is True


# =============================================================================
# Datetime Conversion Tests
# =============================================================================


class TestDatetimeConversion:
    """Tests for converting values to datetime."""

    def test_str_iso_to_datetime(self) -> None:
        """ISO format string converts to datetime."""
        result = to_value_type("2024-01-15T12:30:45", datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 12, 30, 45)

    def test_str_iso_with_tz_to_datetime(self) -> None:
        """ISO format string with timezone converts to datetime."""
        result = to_value_type("2024-01-15T12:30:45+00:00", datetime.datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_date_to_datetime(self) -> None:
        """Date converts to datetime at midnight."""
        date = datetime.date(2024, 1, 15)
        result = to_value_type(date, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0, 0)

    def test_invalid_str_to_datetime_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to datetime"):
            to_value_type("not a date", datetime.datetime)


# =============================================================================
# Date Conversion Tests
# =============================================================================


class TestDateConversion:
    """Tests for converting values to date."""

    def test_str_iso_to_date(self) -> None:
        """ISO format string converts to date."""
        result = to_value_type("2024-01-15", datetime.date)
        assert result == datetime.date(2024, 1, 15)

    def test_str_datetime_to_date(self) -> None:
        """Datetime string extracts date portion."""
        result = to_value_type("2024-01-15T12:30:45", datetime.date)
        assert result == datetime.date(2024, 1, 15)

    def test_datetime_to_date(self) -> None:
        """Datetime extracts date portion."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = to_value_type(dt, datetime.date)
        assert result == datetime.date(2024, 1, 15)

    def test_invalid_str_to_date_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to date"):
            to_value_type("not a date", datetime.date)


# =============================================================================
# Time Conversion Tests
# =============================================================================


class TestTimeConversion:
    """Tests for converting values to time."""

    def test_str_iso_to_time(self) -> None:
        """ISO format string converts to time."""
        result = to_value_type("12:30:45", datetime.time)
        assert result == datetime.time(12, 30, 45)

    def test_datetime_to_time(self) -> None:
        """Datetime extracts time portion."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = to_value_type(dt, datetime.time)
        assert result == datetime.time(12, 30, 45)

    def test_invalid_str_to_time_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to time"):
            to_value_type("not a time", datetime.time)


# =============================================================================
# Decimal Conversion Tests
# =============================================================================


class TestDecimalConversion:
    """Tests for converting values to Decimal."""

    def test_int_to_decimal(self) -> None:
        """Integer converts to Decimal."""
        result = to_value_type(42, Decimal)
        assert result == Decimal(42)

    def test_float_to_decimal(self) -> None:
        """Float converts to Decimal (via string for precision)."""
        result = to_value_type(3.14, Decimal)
        assert result == Decimal("3.14")

    def test_str_to_decimal(self) -> None:
        """String converts to Decimal."""
        result = to_value_type("123.456789", Decimal)
        assert result == Decimal("123.456789")

    def test_invalid_str_to_decimal_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to Decimal"):
            to_value_type("not a number", Decimal)


# =============================================================================
# UUID Conversion Tests
# =============================================================================


class TestUuidConversion:
    """Tests for converting values to UUID."""

    def test_str_to_uuid(self) -> None:
        """UUID string converts to UUID."""
        result = to_value_type("550e8400-e29b-41d4-a716-446655440000", UUID)
        assert result == UUID("550e8400-e29b-41d4-a716-446655440000")

    def test_str_uppercase_to_uuid(self) -> None:
        """Uppercase UUID string converts to UUID."""
        result = to_value_type("550E8400-E29B-41D4-A716-446655440000", UUID)
        assert result == UUID("550e8400-e29b-41d4-a716-446655440000")

    def test_bytes_to_uuid(self) -> None:
        """Bytes converts to UUID."""
        uuid_bytes = UUID("550e8400-e29b-41d4-a716-446655440000").bytes
        result = to_value_type(uuid_bytes, UUID)
        assert result == UUID("550e8400-e29b-41d4-a716-446655440000")

    def test_invalid_str_to_uuid_raises(self) -> None:
        """Invalid string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to UUID"):
            to_value_type("not-a-uuid", UUID)


# =============================================================================
# Path Conversion Tests
# =============================================================================


class TestPathConversion:
    """Tests for converting values to Path."""

    def test_str_to_path(self) -> None:
        """String converts to Path."""
        result = to_value_type("/tmp/test.txt", Path)
        assert result == Path("/tmp/test.txt")

    def test_pure_path_to_path(self) -> None:
        """PurePath converts to Path."""
        pure = PurePosixPath("/tmp/test.txt")
        result = to_value_type(pure, Path)
        assert result == Path("/tmp/test.txt")

    def test_invalid_type_to_path_raises(self) -> None:
        """Invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert int to Path"):
            to_value_type(123, Path)


# =============================================================================
# Dict Conversion Tests (JSON/JSONB support)
# =============================================================================


class TestDictConversion:
    """Tests for converting values to dict (JSON/JSONB support)."""

    def test_json_str_to_dict(self) -> None:
        """JSON string converts to dict."""
        result = to_value_type('{"key": "value", "count": 42}', dict)
        assert result == {"key": "value", "count": 42}

    def test_json_nested_to_dict(self) -> None:
        """Nested JSON string converts to dict."""
        result = to_value_type('{"outer": {"inner": [1, 2, 3]}}', dict)
        assert result == {"outer": {"inner": [1, 2, 3]}}

    def test_json_array_to_dict_raises(self) -> None:
        """JSON array string raises TypeError when converting to dict."""
        with pytest.raises(TypeError, match="JSON string did not parse to dict"):
            to_value_type("[1, 2, 3]", dict)

    def test_invalid_json_to_dict_raises(self) -> None:
        """Invalid JSON string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to dict"):
            to_value_type("not json", dict)


# =============================================================================
# List Conversion Tests (JSON array support)
# =============================================================================


class TestListConversion:
    """Tests for converting values to list (JSON array support)."""

    def test_json_array_str_to_list(self) -> None:
        """JSON array string converts to list."""
        result = to_value_type('[1, 2, 3, "four"]', list)
        assert result == [1, 2, 3, "four"]

    def test_json_nested_array_to_list(self) -> None:
        """Nested JSON array converts to list."""
        result = to_value_type("[[1, 2], [3, 4]]", list)
        assert result == [[1, 2], [3, 4]]

    def test_json_object_to_list_raises(self) -> None:
        """JSON object string raises TypeError when converting to list."""
        with pytest.raises(TypeError, match="JSON string did not parse to list"):
            to_value_type('{"key": "value"}', list)

    def test_tuple_to_list(self) -> None:
        """Tuple converts to list."""
        result = to_value_type((1, 2, 3), list)
        assert result == [1, 2, 3]

    def test_set_to_list(self) -> None:
        """Set converts to list (order may vary)."""
        result = to_value_type({1, 2, 3}, list)
        assert sorted(result) == [1, 2, 3]

    def test_frozenset_to_list(self) -> None:
        """Frozenset converts to list (order may vary)."""
        result = to_value_type(frozenset({1, 2, 3}), list)
        assert sorted(result) == [1, 2, 3]

    def test_invalid_json_to_list_raises(self) -> None:
        """Invalid JSON string raises TypeError."""
        with pytest.raises(TypeError, match="Cannot convert str to list"):
            to_value_type("not json", list)


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_string_to_bool_is_false(self) -> None:
        """Empty string converts to False."""
        assert to_value_type("", bool) is False

    def test_zero_to_bool_is_false(self) -> None:
        """Zero converts to False."""
        assert to_value_type(0, bool) is False
        assert to_value_type(0.0, bool) is False

    def test_empty_dict_preserved(self) -> None:
        """Empty dict is preserved."""
        value: dict[str, str] = {}
        result = to_value_type(value, dict)
        assert result == {}
        assert result is value

    def test_empty_list_preserved(self) -> None:
        """Empty list is preserved."""
        value: list[int] = []
        result = to_value_type(value, list)
        assert result == []
        assert result is value

    def test_empty_json_object_to_dict(self) -> None:
        """Empty JSON object converts to empty dict."""
        result = to_value_type("{}", dict)
        assert result == {}

    def test_empty_json_array_to_list(self) -> None:
        """Empty JSON array converts to empty list."""
        result = to_value_type("[]", list)
        assert result == []


class TestFallbackConversion:
    """Tests for fallback conversion using type constructor."""

    def test_custom_type_with_constructor(self) -> None:
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

    def test_unsupported_conversion_raises(self) -> None:
        """Conversion to unsupported type raises TypeError."""

        class NoConstructor:
            def __init__(self) -> None:
                pass

        with pytest.raises(TypeError, match="Cannot convert"):
            to_value_type("value", NoConstructor)


# =============================================================================
# Schema Type Conversion Tests (Pydantic, dataclass, msgspec, attrs, TypedDict)
# =============================================================================


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


class TestPydanticConversion:
    """Tests for converting values to Pydantic models."""

    def test_dict_to_pydantic(self) -> None:
        """Dict converts to Pydantic model."""
        data = {"name": "Alice", "email": "alice@example.com"}
        result = to_value_type(data, UserPydantic)
        assert isinstance(result, UserPydantic)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_json_string_to_pydantic(self) -> None:
        """JSON string converts to Pydantic model."""
        json_str = '{"name": "Bob", "email": "bob@example.com"}'
        result = to_value_type(json_str, UserPydantic)
        assert isinstance(result, UserPydantic)
        assert result.name == "Bob"
        assert result.email == "bob@example.com"

    def test_pydantic_identity(self) -> None:
        """Pydantic model instance passes through (via to_schema validation)."""
        user = UserPydantic(name="Charlie", email="charlie@example.com")
        result = to_value_type(user, UserPydantic)
        assert isinstance(result, UserPydantic)
        assert result.name == "Charlie"


class TestDataclassConversion:
    """Tests for converting values to dataclasses."""

    def test_dict_to_dataclass(self) -> None:
        """Dict converts to dataclass."""
        data = {"name": "Alice", "email": "alice@example.com"}
        result = to_value_type(data, UserDataclass)
        assert isinstance(result, UserDataclass)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_json_string_to_dataclass(self) -> None:
        """JSON string converts to dataclass."""
        json_str = '{"name": "Bob", "email": "bob@example.com"}'
        result = to_value_type(json_str, UserDataclass)
        assert isinstance(result, UserDataclass)
        assert result.name == "Bob"
        assert result.email == "bob@example.com"


class TestMsgspecConversion:
    """Tests for converting values to msgspec Structs."""

    def test_dict_to_msgspec(self) -> None:
        """Dict converts to msgspec Struct."""
        data = {"name": "Alice", "email": "alice@example.com"}
        result = to_value_type(data, UserMsgspec)
        assert isinstance(result, UserMsgspec)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_json_string_to_msgspec(self) -> None:
        """JSON string converts to msgspec Struct."""
        json_str = '{"name": "Bob", "email": "bob@example.com"}'
        result = to_value_type(json_str, UserMsgspec)
        assert isinstance(result, UserMsgspec)
        assert result.name == "Bob"
        assert result.email == "bob@example.com"


class TestAttrsConversion:
    """Tests for converting values to attrs classes."""

    def test_dict_to_attrs(self) -> None:
        """Dict converts to attrs class."""
        data = {"name": "Alice", "email": "alice@example.com"}
        result = to_value_type(data, UserAttrs)
        assert isinstance(result, UserAttrs)
        assert result.name == "Alice"
        assert result.email == "alice@example.com"

    def test_json_string_to_attrs(self) -> None:
        """JSON string converts to attrs class."""
        json_str = '{"name": "Bob", "email": "bob@example.com"}'
        result = to_value_type(json_str, UserAttrs)
        assert isinstance(result, UserAttrs)
        assert result.name == "Bob"
        assert result.email == "bob@example.com"


class TestTypedDictConversion:
    """Tests for converting values to TypedDict."""

    def test_dict_to_typed_dict(self) -> None:
        """Dict converts to TypedDict (returns dict since TypedDict is runtime dict)."""
        data = {"name": "Alice", "email": "alice@example.com"}
        result = to_value_type(data, UserTypedDict)
        # TypedDict is a dict at runtime
        assert isinstance(result, dict)
        assert result["name"] == "Alice"
        assert result["email"] == "alice@example.com"

    def test_json_string_to_typed_dict(self) -> None:
        """JSON string converts to TypedDict."""
        json_str = '{"name": "Bob", "email": "bob@example.com"}'
        result = to_value_type(json_str, UserTypedDict)
        assert isinstance(result, dict)
        assert result["name"] == "Bob"
        assert result["email"] == "bob@example.com"


class TestSchemaTypeEdgeCases:
    """Tests for edge cases with schema type conversion."""

    def test_nested_json_to_pydantic(self) -> None:
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

    def test_invalid_json_string_fallback(self) -> None:
        """Invalid JSON string is passed as-is to schema converter (which will fail)."""
        with pytest.raises(Exception):
            to_value_type("not valid json", UserPydantic)

    def test_already_parsed_dict_works(self) -> None:
        """Pre-parsed dict (from DB driver) works correctly."""
        # This simulates a DB driver that already parses JSONB to dict
        data = {"name": "Charlie", "email": "charlie@example.com"}
        result = to_value_type(data, UserMsgspec)
        assert isinstance(result, UserMsgspec)
        assert result.name == "Charlie"
