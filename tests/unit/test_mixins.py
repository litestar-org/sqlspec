"""Tests for mixins utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

import pytest
from msgspec import Struct
from pydantic import BaseModel

from sqlspec.mixins import (
    ResultConverter
)
from sqlspec.typing import (
    Empty,
    is_dataclass_instance,
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


@pytest.fixture(scope="session")
def sample_dataclass() -> SampleDataclass:
    """Create a sample dataclass instance."""
    return SampleDataclass(name="test", value=42)


@pytest.fixture(scope="session")
def sample_pydantic() -> SamplePydanticModel:
    """Create a sample Pydantic model instance."""
    return SamplePydanticModel(name="test", value=42)


@pytest.fixture(scope="session")
def sample_msgspec() -> SampleMsgspecModel:
    """Create a sample Msgspec model instance."""
    return SampleMsgspecModel(name="test", value=42)


@pytest.fixture(scope="session")
def sample_dict() -> dict[str, Any]:
    """Create a sample dictionary."""
    return {"name": "test", "value": 42}


def test_is_result_converter_dataclass(sample_dataclass: SampleDataclass) -> None:
    """Test dataclass type checking."""
    assert is_dataclass_instance(ResultConverter.to_schema(data=SampleDataclass(name="test", value=42), schema_type=SampleDataclass))

