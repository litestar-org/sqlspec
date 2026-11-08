"""Static analyzer contract for lazy optional typing exports."""

from typing import TypeVar, assert_type

from litestar.dto.data_structures import DTOData as LitestarDTOData
from pyarrow import Table as PyArrowTable
from pydantic import BaseModel as PydanticBaseModel

from sqlspec import SQL, Select, SQLSpec, StatementConfig, sql
from sqlspec.builder import SQLFactory
from sqlspec.builder._base import BuiltQuery
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.protocols import AsyncWritableBytesSink, DirectStreamWriteProtocol, ObjectStoreProtocol, WritableBytesSink
from sqlspec.storage import supports_direct_stream_write
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


def direct_stream_narrowing(store: ObjectStoreProtocol) -> None:
    if supports_direct_stream_write(store):
        assert_type(store, DirectStreamWriteProtocol)
        with store.open_write_stream_sync("out.parquet") as sync_sink:
            assert_type(sync_sink, WritableBytesSink)


async def async_direct_stream_narrowing(store: ObjectStoreProtocol) -> None:
    if supports_direct_stream_write(store):
        assert_type(store, DirectStreamWriteProtocol)
        async with store.open_write_stream_async("out.parquet") as async_sink:
            assert_type(async_sink, AsyncWritableBytesSink)
