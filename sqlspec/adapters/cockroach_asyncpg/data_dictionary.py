"""CockroachDB AsyncPG data dictionary."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.driver import AsyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver

__all__ = ("CockroachAsyncpgDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachAsyncpgDataDictionary(AsyncDataDictionaryBase):
    """CockroachDB async data dictionary (AsyncPG)."""

    dialect: ClassVar[str] = "cockroachdb"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: "CockroachAsyncpgDriver") -> "VersionInfo | None":
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "CockroachAsyncpgDriver", feature: str) -> bool:
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "CockroachAsyncpgDriver", type_category: str) -> str:
        config = self.get_dialect_config()
        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "CockroachAsyncpgDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return await driver.select(
            self.get_query("columns_by_table"), schema_name=schema_name, table_name=table, schema_type=ColumnMetadata
        )

    async def get_indexes(
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return await driver.select(
            self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
        )

    async def get_foreign_keys(
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )
