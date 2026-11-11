"""Static analyzer contract for lazy optional typing exports."""

from contextvars import Token
from pathlib import Path
from typing import TypeVar, assert_type

from litestar.dto.data_structures import DTOData as LitestarDTOData
from pyarrow import Table as PyArrowTable
from pydantic import BaseModel as PydanticBaseModel

from sqlspec import (
    SQL,
    Select,
    SQLSpec,
    StatementConfig,
    TenantCleanupError,
    TenantContext,
    TenantContextError,
    TenantContextPayload,
    sql,
)
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.builder import SQLFactory
from sqlspec.builder._base import BuiltQuery
from sqlspec.config import ADKConfig, EventsConfig, ExtensionMigrationMode, LitestarConfig
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.protocols import AsyncWritableBytesSink, DirectStreamWriteProtocol, ObjectStoreProtocol, WritableBytesSink
from sqlspec.storage import encode_arrow_to_async_sink, encode_arrow_to_sink, supports_direct_stream_write
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


def tenant_exports() -> TenantContextPayload:
    payload = TenantContextPayload("tenant-a")
    assert_type(payload, TenantContextPayload)
    assert_type(payload.tenant_id, str)
    assert_type(TenantContext.get(), TenantContextPayload | None)
    assert_type(TenantContext.get_tenant_id(), str | None)
    assert_type(TenantContext.is_bypassed(), bool)
    token = TenantContext.set(None)
    assert_type(token, Token[TenantContextPayload | None])
    TenantContext.reset(token)
    with TenantContext.scope(payload) as scoped:
        assert_type(scoped, TenantContextPayload)
    with TenantContext.bypass() as bypassed:
        assert_type(bypassed, TenantContextPayload)
        assert_type(TenantContext.is_bypassed(), bool)
    assert_type(TenantContextError("missing tenant"), TenantContextError)
    assert_type(TenantCleanupError("reset failed"), TenantCleanupError)
    return payload


async def async_tenant_exports() -> TenantContextPayload:
    async with TenantContext.scope("tenant-a") as payload:
        assert_type(payload, TenantContextPayload)
    async with TenantContext.bypass() as bypassed:
        assert_type(bypassed, TenantContextPayload)
        assert_type(TenantContext.is_bypassed(), bool)
    return payload


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


def storage_encoder_typing(table: ArrowTable, sync_sink: WritableBytesSink) -> None:
    assert_type(encode_arrow_to_sink(table, sync_sink), tuple[int, int])


async def async_storage_encoder_typing(table: ArrowTable, async_sink: AsyncWritableBytesSink) -> None:
    assert_type(await encode_arrow_to_async_sink(table, async_sink), tuple[int, int])


def extension_migration_mode_exports() -> ExtensionMigrationMode:
    adk_cfg: ADKConfig = {"migration_mode": "reconcile"}
    litestar_cfg: LitestarConfig = {"session_table": True, "migration_mode": "reconcile"}
    events_cfg: EventsConfig = {"migration_mode": "versioned"}
    config = SqliteConfig(extension_config={"adk": adk_cfg, "litestar": litestar_cfg, "events": events_cfg})
    config.add_extension_migrations("ext_demo", Path("migrations"), migration_mode="reconcile")
    return "reconcile"
