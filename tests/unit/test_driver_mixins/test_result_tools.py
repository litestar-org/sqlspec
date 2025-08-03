"""Tests for driver mixins result tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from msgspec import Struct
from pydantic import BaseModel

from sqlspec.driver.mixins._result_tools import ToSchemaMixin
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import ATTRS_INSTALLED, attrs_define, attrs_field


@dataclass
class UserDataclass:
    """Sample dataclass for testing."""

    name: str
    email: str
    age: int = 18


class UserPydantic(BaseModel):
    """Sample Pydantic model for testing."""

    name: str
    email: str
    age: int = 18


class UserMsgspec(Struct):
    """Sample msgspec struct for testing."""

    name: str
    email: str
    age: int = 18


@attrs_define
class UserAttrs:
    """Sample attrs class for testing."""

    name: str = attrs_field()
    email: str = attrs_field()
    age: int = attrs_field(default=18)


@pytest.fixture
def sample_dict() -> dict[str, Any]:
    """Sample dict data."""
    return {"name": "John", "email": "john@example.com", "age": 30}


@pytest.fixture
def sample_dict_list() -> list[dict[str, Any]]:
    """Sample list of dict data."""
    return [
        {"name": "John", "email": "john@example.com", "age": 30},
        {"name": "Jane", "email": "jane@example.com", "age": 25},
    ]


class TestToSchemaMixin:
    """Test ToSchemaMixin conversion functionality."""

    def test_to_schema_no_schema_type_single(self, sample_dict: dict[str, Any]) -> None:
        """Test to_schema returns data as-is when no schema_type provided."""
        result = ToSchemaMixin.to_schema(sample_dict)
        assert result == sample_dict

    def test_to_schema_no_schema_type_list(self, sample_dict_list: list[dict[str, Any]]) -> None:
        """Test to_schema returns data as-is when no schema_type provided for list."""
        result = ToSchemaMixin.to_schema(sample_dict_list)
        assert result == sample_dict_list

    def test_to_schema_dataclass_single(self, sample_dict: dict[str, Any]) -> None:
        """Test conversion to dataclass from single dict."""
        result = ToSchemaMixin.to_schema(sample_dict, schema_type=UserDataclass)
        assert isinstance(result, UserDataclass)
        assert result.name == "John"
        assert result.email == "john@example.com"
        assert result.age == 30

    def test_to_schema_dataclass_list(self, sample_dict_list: list[dict[str, Any]]) -> None:
        """Test conversion to dataclass from list of dicts."""
        result = ToSchemaMixin.to_schema(sample_dict_list, schema_type=UserDataclass)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, UserDataclass) for item in result)
        assert result[0].name == "John"
        assert result[1].name == "Jane"

    def test_to_schema_pydantic_single(self, sample_dict: dict[str, Any]) -> None:
        """Test conversion to Pydantic model from single dict."""
        result = ToSchemaMixin.to_schema(sample_dict, schema_type=UserPydantic)
        assert isinstance(result, UserPydantic)
        assert result.name == "John"
        assert result.email == "john@example.com"
        assert result.age == 30

    def test_to_schema_pydantic_list(self, sample_dict_list: list[dict[str, Any]]) -> None:
        """Test conversion to Pydantic model from list of dicts."""
        result = ToSchemaMixin.to_schema(sample_dict_list, schema_type=UserPydantic)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, UserPydantic) for item in result)
        assert result[0].name == "John"
        assert result[1].name == "Jane"

    def test_to_schema_msgspec_single(self, sample_dict: dict[str, Any]) -> None:
        """Test conversion to msgspec struct from single dict."""
        result = ToSchemaMixin.to_schema(sample_dict, schema_type=UserMsgspec)
        assert isinstance(result, UserMsgspec)
        assert result.name == "John"
        assert result.email == "john@example.com"
        assert result.age == 30

    def test_to_schema_msgspec_list(self, sample_dict_list: list[dict[str, Any]]) -> None:
        """Test conversion to msgspec struct from list of dicts."""
        result = ToSchemaMixin.to_schema(sample_dict_list, schema_type=UserMsgspec)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, UserMsgspec) for item in result)
        assert result[0].name == "John"
        assert result[1].name == "Jane"

    @pytest.mark.skipif(not ATTRS_INSTALLED, reason="attrs not installed")
    def test_to_schema_attrs_single(self, sample_dict: dict[str, Any]) -> None:
        """Test conversion to attrs class from single dict."""
        result = ToSchemaMixin.to_schema(sample_dict, schema_type=UserAttrs)
        assert isinstance(result, UserAttrs)
        assert result.name == "John"
        assert result.email == "john@example.com"
        assert result.age == 30

    @pytest.mark.skipif(not ATTRS_INSTALLED, reason="attrs not installed")
    def test_to_schema_attrs_list(self, sample_dict_list: list[dict[str, Any]]) -> None:
        """Test conversion to attrs class from list of dicts."""
        result = ToSchemaMixin.to_schema(sample_dict_list, schema_type=UserAttrs)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, UserAttrs) for item in result)
        assert result[0].name == "John"
        assert result[1].name == "Jane"

    def test_to_schema_invalid_schema_type(self, sample_dict: dict[str, Any]) -> None:
        """Test error when invalid schema_type is provided."""
        with pytest.raises(
            SQLSpecError, match="should be a valid Dataclass, Pydantic model, Msgspec struct, or Attrs class"
        ):
            ToSchemaMixin.to_schema(sample_dict, schema_type=str)  # type: ignore[arg-type,type-var]

    def test_to_schema_dataclass_from_existing_dataclass(self) -> None:
        """Test conversion from existing dataclass instance."""
        existing = UserDataclass(name="Alice", email="alice@example.com", age=25)
        result = ToSchemaMixin.to_schema(existing, schema_type=UserDataclass)
        assert isinstance(result, UserDataclass)
        assert result.name == "Alice"

    def test_to_schema_pydantic_from_existing_model(self) -> None:
        """Test conversion from existing Pydantic model."""
        existing = UserPydantic(name="Bob", email="bob@example.com", age=35)
        result = ToSchemaMixin.to_schema(existing, schema_type=UserPydantic)
        assert isinstance(result, UserPydantic)
        assert result.name == "Bob"

    def test_to_schema_msgspec_from_existing_struct(self) -> None:
        """Test conversion from existing msgspec struct."""
        existing = UserMsgspec(name="Charlie", email="charlie@example.com", age=40)
        result = ToSchemaMixin.to_schema(existing, schema_type=UserMsgspec)
        assert isinstance(result, UserMsgspec)
        assert result.name == "Charlie"

    @pytest.mark.skipif(not ATTRS_INSTALLED, reason="attrs not installed")
    def test_to_schema_attrs_from_existing_attrs(self) -> None:
        """Test conversion from existing attrs instance."""
        existing = UserAttrs(name="Dave", email="dave@example.com", age=45)
        result = ToSchemaMixin.to_schema(existing, schema_type=UserAttrs)
        assert isinstance(result, UserAttrs)
        assert result.name == "Dave"

    def test_to_schema_dataclass_with_dict_like_object(self) -> None:
        """Test conversion with dict-like object (has keys method)."""

        class DictLike:
            def __init__(self, data: dict[str, Any]) -> None:
                self._data = data

            def keys(self) -> Any:
                return self._data.keys()

            def __getitem__(self, key: str) -> Any:
                return self._data[key]

            def __iter__(self) -> Any:
                return iter(self._data)

            def __len__(self) -> int:
                return len(self._data)

        dict_like = DictLike({"name": "Test", "email": "test@example.com", "age": 22})
        result = ToSchemaMixin.to_schema(dict_like, schema_type=UserDataclass)
        assert isinstance(result, UserDataclass)
        assert result.name == "Test"
        assert result.email == "test@example.com"
        assert result.age == 22

    def test_to_schema_list_mixed_types(self) -> None:
        """Test conversion of list with mixed dict and dict-like objects."""

        class DictLike:
            def __init__(self, data: dict[str, Any]) -> None:
                self._data = data

            def keys(self) -> Any:
                return self._data.keys()

            def __getitem__(self, key: str) -> Any:
                return self._data[key]

            def __iter__(self) -> Any:
                return iter(self._data)

            def __len__(self) -> int:
                return len(self._data)

        mixed_data: list[dict[str, Any]] = [
            {"name": "John", "email": "john@example.com", "age": 30},
            DictLike({"name": "Jane", "email": "jane@example.com", "age": 25}),  # type: ignore[list-item]
        ]

        result = ToSchemaMixin.to_schema(mixed_data, schema_type=UserDataclass)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, UserDataclass) for item in result)
        assert result[0].name == "John"
        assert result[1].name == "Jane"
