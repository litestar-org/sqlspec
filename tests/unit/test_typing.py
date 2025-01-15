"""Tests for typing utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

import pytest
from msgspec import Struct
from pydantic import BaseModel

from sqlspec.typing import (
    Empty,
    dataclass_to_dict,
    extract_dataclass_fields,
    extract_dataclass_items,
    is_dataclass,
    is_dataclass_instance,
    is_dataclass_with_field,
    is_dataclass_without_field,
    is_dict,
    is_dict_with_field,
    is_dict_without_field,
    is_msgspec_struct,
    is_msgspec_struct_with_field,
    is_msgspec_struct_without_field,
    is_pydantic_model,
    is_pydantic_model_with_field,
    is_pydantic_model_without_field,
    schema_dump,
)


@dataclass
class SampleDataclass:
    """Sample dataclass for testing."""

    name: str
    value: int | None = None
    empty_field: Any = Empty
    meta: ClassVar[str] = "test"


class SamplePydanticModel(BaseModel):
    """Sample Pydantic model for testing."""

    name: str
    value: int | None = None


class SampleMsgspecModel(Struct):
    """Sample Msgspec model for testing."""

    name: str
    value: int | None = None


@pytest.fixture
def sample_dataclass() -> SampleDataclass:
    """Create a sample dataclass instance."""
    return SampleDataclass(name="test", value=42)


@pytest.fixture
def sample_pydantic() -> SamplePydanticModel:
    """Create a sample Pydantic model instance."""
    return SamplePydanticModel(name="test", value=42)


@pytest.fixture
def sample_msgspec() -> SampleMsgspecModel:
    """Create a sample Msgspec model instance."""
    return SampleMsgspecModel(name="test", value=42)


@pytest.fixture
def sample_dict() -> dict[str, Any]:
    """Create a sample dictionary."""
    return {"name": "test", "value": 42}


class TestTypeChecking:
    """Test type checking functions."""

    def test_is_dataclass(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass type checking."""
        assert is_dataclass(sample_dataclass)
        assert not is_dataclass({"name": "test"})

    def test_is_dataclass_instance(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass instance checking."""
        assert is_dataclass_instance(sample_dataclass)
        assert not is_dataclass_instance(SampleDataclass)
        assert not is_dataclass_instance({"name": "test"})

    def test_is_dataclass_with_field(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass field checking."""
        assert is_dataclass_with_field(sample_dataclass, "name")
        assert not is_dataclass_with_field(sample_dataclass, "nonexistent")

    def test_is_dataclass_without_field(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass field absence checking."""
        assert is_dataclass_without_field(sample_dataclass, "nonexistent")
        assert not is_dataclass_without_field(sample_dataclass, "name")

    def test_is_pydantic_model(self, sample_pydantic: SamplePydanticModel) -> None:
        """Test Pydantic model type checking."""
        assert is_pydantic_model(sample_pydantic)
        assert not is_pydantic_model({"name": "test"})

    def test_is_pydantic_model_with_field(self, sample_pydantic: SamplePydanticModel) -> None:
        """Test Pydantic model field checking."""
        assert is_pydantic_model_with_field(sample_pydantic, "name")
        assert not is_pydantic_model_with_field(sample_pydantic, "nonexistent")

    def test_is_pydantic_model_without_field(self, sample_pydantic: SamplePydanticModel) -> None:
        """Test Pydantic model field absence checking."""
        assert is_pydantic_model_without_field(sample_pydantic, "nonexistent")
        assert not is_pydantic_model_without_field(sample_pydantic, "name")

    def test_is_msgspec_struct(self, sample_msgspec: SampleMsgspecModel) -> None:
        """Test Msgspec model type checking."""
        assert is_msgspec_struct(sample_msgspec)
        assert not is_msgspec_struct({"name": "test"})

    def test_is_msgspec_struct_with_field(self, sample_msgspec: SampleMsgspecModel) -> None:
        """Test Msgspec model field checking."""
        assert is_msgspec_struct_with_field(sample_msgspec, "name")
        assert not is_msgspec_struct_with_field(sample_msgspec, "nonexistent")

    def test_is_msgspec_struct_without_field(self, sample_msgspec: SampleMsgspecModel) -> None:
        """Test Msgspec model field absence checking."""
        assert is_msgspec_struct_without_field(sample_msgspec, "nonexistent")
        assert not is_msgspec_struct_without_field(sample_msgspec, "name")

    def test_is_dict(self, sample_dict: dict[str, Any]) -> None:
        """Test dictionary type checking."""
        assert is_dict(sample_dict)
        assert not is_dict([1, 2, 3])

    def test_is_dict_with_field(self, sample_dict: dict[str, Any]) -> None:
        """Test dictionary field checking."""
        assert is_dict_with_field(sample_dict, "name")
        assert not is_dict_with_field(sample_dict, "nonexistent")

    def test_is_dict_without_field(self, sample_dict: dict[str, Any]) -> None:
        """Test dictionary field absence checking."""
        assert is_dict_without_field(sample_dict, "nonexistent")
        assert not is_dict_without_field(sample_dict, "name")


class TestDataclassUtils:
    """Test dataclass utility functions."""

    def test_extract_dataclass_fields(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass field extraction."""
        fields = extract_dataclass_fields(sample_dataclass)
        assert len(fields) == 3
        assert all(f.name in {"name", "value", "empty_field"} for f in fields)

        # Test exclusions
        fields_no_none = extract_dataclass_fields(sample_dataclass, exclude_none=True)
        assert all(getattr(sample_dataclass, f.name) is not None for f in fields_no_none)

        fields_no_empty = extract_dataclass_fields(sample_dataclass, exclude_empty=True)
        assert all(getattr(sample_dataclass, f.name) is not Empty for f in fields_no_empty)

        # Test include/exclude
        fields_included = extract_dataclass_fields(sample_dataclass, include={"name"})
        assert len(fields_included) == 1
        assert fields_included[0].name == "name"

        fields_excluded = extract_dataclass_fields(sample_dataclass, exclude={"name"})
        assert all(f.name != "name" for f in fields_excluded)

        # Test conflicting include/exclude
        with pytest.raises(ValueError, match="both included and excluded"):
            extract_dataclass_fields(sample_dataclass, include={"name"}, exclude={"name"})

    def test_extract_dataclass_items(self, sample_dataclass: SampleDataclass) -> None:
        """Test dataclass item extraction."""
        items = extract_dataclass_items(sample_dataclass)
        assert len(items) == 3
        assert dict(items) == {
            "name": "test",
            "value": 42,
            "empty_field": Empty,
        }

    def test_dataclass_to_dict(self) -> None:
        """Test dataclass to dictionary conversion."""

        @dataclass
        class NestedDataclass:
            """Nested dataclass for testing."""

            x: int
            y: int

        @dataclass
        class ComplexDataclass:
            """Complex dataclass for testing."""

            name: str
            nested: NestedDataclass
            value: int | None = None
            empty_field: Any = Empty
            items: list[str] = field(default_factory=list)

        nested = NestedDataclass(x=1, y=2)
        obj = ComplexDataclass(
            name="test",
            nested=nested,
            value=42,
            items=["a", "b"],
        )

        # Test basic conversion
        result = dataclass_to_dict(obj)
        assert result["name"] == "test"
        assert result["value"] == 42
        assert result["empty_field"] is Empty
        assert result["items"] == ["a", "b"]
        assert isinstance(result["nested"], dict)
        assert result["nested"] == {"x": 1, "y": 2}

        # Test with exclude_empty
        result = dataclass_to_dict(obj, exclude_empty=True)
        assert "empty_field" not in result

        # Test with exclude_none
        obj.value = None
        result = dataclass_to_dict(obj, exclude_none=True)
        assert "value" not in result

        # Test without nested conversion
        result = dataclass_to_dict(obj, convert_nested=False)
        assert isinstance(result["nested"], NestedDataclass)

        # Test with exclusions
        result = dataclass_to_dict(obj, exclude={"nested", "items"})
        assert "nested" not in result
        assert "items" not in result


class TestSchemaDump:
    """Test schema dumping functionality."""

    def test_schema_dump_dataclass(self, sample_dataclass: SampleDataclass) -> None:
        """Test schema dumping for dataclasses."""
        result = schema_dump(sample_dataclass)
        assert result == {
            "name": "test",
            "value": 42,
        }

    def test_schema_dump_pydantic(self, sample_pydantic: SamplePydanticModel) -> None:
        """Test schema dumping for Pydantic models."""
        result = schema_dump(sample_pydantic)
        assert result == {
            "name": "test",
            "value": 42,
        }

    def test_schema_dump_msgspec(self, sample_msgspec: SampleMsgspecModel) -> None:
        """Test schema dumping for Msgspec models."""
        result = schema_dump(sample_msgspec)
        assert result == {
            "name": "test",
            "value": 42,
        }

    def test_schema_dump_dict(self, sample_dict: dict[str, Any]) -> None:
        """Test schema dumping for dictionaries."""
        result = schema_dump(sample_dict)
        assert result == sample_dict
