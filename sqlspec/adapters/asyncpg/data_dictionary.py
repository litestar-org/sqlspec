"""PostgreSQL-specific data dictionary for metadata queries via asyncpg."""

from typing import TYPE_CHECKING, ClassVar

from sqlspec.data_dictionary import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo
from sqlspec.data_dictionary.dialects.postgres import resolve_postgres_json_type
from sqlspec.driver import AsyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

__all__ = ("AsyncpgDataDictionary",)


class AsyncpgDataDictionary(AsyncDataDictionaryBase):
    """PostgreSQL-specific async data dictionary."""

    dialect: ClassVar[str] = "postgres"

    async def get_version(self, driver: "AsyncpgDriver") -> "VersionInfo | None":
        """Get PostgreSQL database version information.

        Args:
            driver: Async database driver instance.

        Returns:
            PostgreSQL version information or None if detection fails.
        """
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        # Not cached, fetch from database

        version_value = await driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        config = self.get_dialect_config()
        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(id(driver), version_info)
        return version_info

    async def get_feature_flag(self, driver: "AsyncpgDriver", feature: str) -> bool:
        """Check if PostgreSQL database supports a specific feature.

        Args:
            driver: Async database driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.
        """
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "AsyncpgDriver", type_category: str) -> str:
        """Get optimal PostgreSQL type for a category.

        Args:
            driver: Async database driver instance.
            type_category: Type category.

        Returns:
            PostgreSQL-specific type name.
        """
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            return resolve_postgres_json_type(version_info)

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "AsyncpgDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using Recursive CTE."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "AsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
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
        self, driver: "AsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
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
        self, driver: "AsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )
