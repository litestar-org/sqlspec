"""MySQL-specific data dictionary for metadata queries via asyncmy."""

from typing import TYPE_CHECKING

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary._helpers import DialectSQLMixin
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
)

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

__all__ = ("AsyncmyDataDictionary",)


@mypyc_attr(native_class=False)
class AsyncmyDataDictionary(DialectSQLMixin, AsyncDataDictionaryBase["AsyncmyDriver"]):
    """MySQL-specific async data dictionary."""

    __slots__ = ()

    dialect = "mysql"

    async def get_version(self, driver: "AsyncmyDriver") -> "VersionInfo | None":
        """Get MySQL database version information."""
        was_cached, cached_version = self.get_cached_version_for_driver(driver)
        if was_cached:
            return cached_version

        version_value = await driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(self.dialect, "missing")
            self.cache_version_for_driver(driver, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(self.dialect, "parse_failed")
            self.cache_version_for_driver(driver, None)
            return None

        self._log_version_detected(self.dialect, version_info)
        self.cache_version_for_driver(driver, version_info)
        return version_info

    async def get_feature_flag(self, driver: "AsyncmyDriver", feature: str) -> bool:
        """Check if MySQL database supports a specific feature."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "AsyncmyDriver", type_category: str) -> str:
        """Get optimal MySQL type for a category."""
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "AsyncmyDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using the MySQL catalog."""
        schema_name = self.resolve_schema(schema)
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        return await driver.select(
            self.get_query("columns_by_table"), table_name=table, schema_name=schema_name, schema_type=ColumnMetadata
        )

    async def get_indexes(
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        return await driver.select(
            self.get_query("indexes_by_table"), table_name=table, schema_name=schema_name, schema_type=IndexMetadata
        )

    async def get_foreign_keys(
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )
