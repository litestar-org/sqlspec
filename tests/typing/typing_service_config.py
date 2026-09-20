"""Config constructors preserve the driver and schema result types."""

from dataclasses import dataclass
from typing import Any

from sqlspec import sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import CursorFilter, CursorKey, CursorPagination, LimitOffsetFilter, OffsetPagination
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService


@dataclass
class ServiceRow:
    value: int


def sync_rows(config: SqliteConfig) -> tuple[OffsetPagination[ServiceRow], ServiceRow]:
    service = SQLSpecSyncService(config=config)
    with service.provide_session() as session:
        driver: SqliteDriver = session
        page = service.paginate(sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver)
        row = service.get_one(sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver)
        assert isinstance(page, OffsetPagination)
        return page, row


async def async_rows(config: AiosqliteConfig) -> tuple[OffsetPagination[ServiceRow], ServiceRow]:
    service = SQLSpecAsyncService(config=config)
    async with service.provide_session() as session:
        driver: AiosqliteDriver = session
        page = await service.paginate(
            sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver
        )
        row = await service.get_one(sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver)
        assert isinstance(page, OffsetPagination)
        return page, row


def sync_cursor_rows(config: SqliteConfig) -> tuple[CursorPagination[ServiceRow], CursorPagination[dict[str, Any]]]:
    service = SQLSpecSyncService(config=config)
    with service.provide_session() as driver:
        statement = sql.select("value").from_("service_values")
        flt = CursorFilter([CursorKey("value")], 10)
        typed = service.paginate(statement, flt, schema_type=ServiceRow, session=driver)
        untyped = service.paginate(statement, flt, session=driver)
        return typed, untyped


async def async_cursor_rows(
    config: AiosqliteConfig,
) -> tuple[CursorPagination[ServiceRow], CursorPagination[dict[str, Any]]]:
    service = SQLSpecAsyncService(config=config)
    async with service.provide_session() as driver:
        statement = sql.select("value").from_("service_values")
        flt = CursorFilter([CursorKey("value")], 10)
        typed = await service.paginate(statement, flt, schema_type=ServiceRow, session=driver)
        untyped = await service.paginate(statement, flt, session=driver)
        return typed, untyped


def sync_explicit_rows(
    service: SQLSpecSyncService[SqliteDriver],
    cursor_filters: list[CursorFilter],
    offset_filters: list[LimitOffsetFilter],
) -> tuple[
    CursorPagination[ServiceRow],
    CursorPagination[dict[str, Any]],
    OffsetPagination[ServiceRow],
    OffsetPagination[dict[str, Any]],
]:
    cursor_page = service.paginate_cursor("SELECT value FROM service_values", *cursor_filters, schema_type=ServiceRow)
    raw_cursor = service.paginate_cursor("SELECT value FROM service_values", *cursor_filters)
    offset_page = service.paginate_limit_offset(
        "SELECT value FROM service_values", *offset_filters, schema_type=ServiceRow
    )
    raw_offset = service.paginate_limit_offset("SELECT value FROM service_values", *offset_filters)
    return cursor_page, raw_cursor, offset_page, raw_offset


async def async_explicit_rows(
    service: SQLSpecAsyncService[AiosqliteDriver],
    cursor_filters: list[CursorFilter],
    offset_filters: list[LimitOffsetFilter],
) -> tuple[
    CursorPagination[ServiceRow],
    CursorPagination[dict[str, Any]],
    OffsetPagination[ServiceRow],
    OffsetPagination[dict[str, Any]],
]:
    cursor_page = await service.paginate_cursor(
        "SELECT value FROM service_values", *cursor_filters, schema_type=ServiceRow
    )
    raw_cursor = await service.paginate_cursor("SELECT value FROM service_values", *cursor_filters)
    offset_page = await service.paginate_limit_offset(
        "SELECT value FROM service_values", *offset_filters, schema_type=ServiceRow
    )
    raw_offset = await service.paginate_limit_offset("SELECT value FROM service_values", *offset_filters)
    return cursor_page, raw_cursor, offset_page, raw_offset
