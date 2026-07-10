"""Static analyzer contract for lazy optional typing exports."""

from typing import TypeVar

from litestar.dto.data_structures import DTOData as LitestarDTOData
from pyarrow import Table as PyArrowTable
from pydantic import BaseModel as PydanticBaseModel
from typing_extensions import assert_type

from sqlspec.typing import ArrowTable, BaseModel, DTOData

T = TypeVar("T")


def arrow_to_vendor(value: ArrowTable) -> PyArrowTable:
    assert_type(value, PyArrowTable)
    return value


def arrow_from_vendor(value: PyArrowTable) -> ArrowTable:
    return value


def model_to_vendor(value: BaseModel) -> PydanticBaseModel:
    assert_type(value, PydanticBaseModel)
    return value


def model_from_vendor(value: PydanticBaseModel) -> BaseModel:
    return value


def dto_to_vendor(value: "DTOData[T]") -> "LitestarDTOData[T]":
    assert_type(value, LitestarDTOData[T])
    return value


def dto_from_vendor(value: "LitestarDTOData[T]") -> "DTOData[T]":
    return value


def dto_create(value: "DTOData[T]") -> T:
    return value.create_instance()
