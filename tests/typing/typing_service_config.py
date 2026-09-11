"""Config constructors preserve the driver and schema result types."""

from dataclasses import dataclass

from sqlspec import sql
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.core import OffsetPagination
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
        return page, row


async def async_rows(config: AiosqliteConfig) -> tuple[OffsetPagination[ServiceRow], ServiceRow]:
    service = SQLSpecAsyncService(config=config)
    async with service.provide_session() as session:
        driver: AiosqliteDriver = session
        page = await service.paginate(
            sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver
        )
        row = await service.get_one(sql.select("value").from_("service_values"), schema_type=ServiceRow, session=driver)
        return page, row
