"""Static analyzer contract for lazy optional typing exports."""

from typing import TypeVar

from litestar.dto.data_structures import DTOData as LitestarDTOData
from pyarrow import Table as PyArrowTable
from pydantic import BaseModel as PydanticBaseModel
from typing_extensions import assert_type

from sqlspec import SQL, Select, SQLSpec, StatementConfig, sql
from sqlspec.builder import SQLFactory
from sqlspec.builder._base import BuiltQuery
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import ArrowTable, BaseModel, DTOData

T = TypeVar("T")


def public_exports() -> SQL:
    assert_type(SQLSpec(), SQLSpec)
    assert_type(StatementConfig(), StatementConfig)
    assert_type(EventRuntimeHints(), EventRuntimeHints)
    assert_type(sql, SQLFactory)
    assert_type(sql.select("1"), Select)
    assert_type(sql.select("1").build(), BuiltQuery)
    return SQL("SELECT 1")


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
