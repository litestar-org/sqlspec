"""Config constructors preserve the driver and schema result types."""

from dataclasses import dataclass
from typing import Any

from sqlspec import sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import CursorFilter, CursorKey, CursorPagination, OffsetPagination
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
