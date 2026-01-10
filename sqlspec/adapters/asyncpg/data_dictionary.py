"""PostgreSQL-specific data dictionary for metadata queries via asyncpg."""

from typing import TYPE_CHECKING

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import DialectSQLMixin
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
)

if TYPE_CHECKING:
    import re

    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
    from sqlspec.data_dictionary import DialectConfig

__all__ = ("AsyncpgDataDictionary",)


@mypyc_attr(native_class=False)
class AsyncpgDataDictionary(AsyncDataDictionaryBase, DialectSQLMixin):
    """PostgreSQL-specific async data dictionary."""

    __slots__ = ()

    dialect = "postgres"

    def get_cached_version(self, driver_id: int) -> "tuple[bool, VersionInfo | None]":
        """Get cached version info for a driver."""
        return AsyncDataDictionaryBase.get_cached_version(self, driver_id)

    def cache_version(self, driver_id: int, version: "VersionInfo | None") -> None:
        """Cache version info for a driver."""
        AsyncDataDictionaryBase.cache_version(self, driver_id, version)

    def parse_version_with_pattern(self, pattern: "re.Pattern[str]", version_str: str) -> "VersionInfo | None":
        """Parse version string using a specific regex pattern."""
        return AsyncDataDictionaryBase.parse_version_with_pattern(self, pattern, version_str)

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return DialectSQLMixin.get_dialect_config(self)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        return DialectSQLMixin.resolve_schema(self, schema)

    def resolve_feature_flag(self, feature: str, version: "VersionInfo | None") -> bool:
        """Resolve a feature flag using dialect config and version info."""
        return DialectSQLMixin.resolve_feature_flag(self, feature, version)

    def get_cached_version_for_driver(self, driver: "AsyncpgDriver") -> "tuple[bool, VersionInfo | None]":
        """Get cached version info for a driver instance."""
        return self.get_cached_version(id(driver))

    async def get_version(self, driver: "AsyncpgDriver") -> "VersionInfo | None":
        """Get PostgreSQL database version information.

        Args:
            driver: Async database driver instance.

        Returns:
            PostgreSQL version information or None if detection fails.

        """
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
            jsonb_version = config.get_feature_version("supports_jsonb")
            json_version = config.get_feature_version("supports_json")
            if version_info and jsonb_version and version_info >= jsonb_version:
                return "JSONB"
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

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

    def list_available_features(self) -> "list[str]":
        """List all features available for the dialect."""
        return DialectSQLMixin.list_available_features(self)
