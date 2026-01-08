"""DuckDB-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING

from sqlspec.data_dictionary._helpers import DialectSQLMixin
from sqlspec.driver import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    SyncDataDictionaryBase,
    TableMetadata,
    VersionInfo,
)
from sqlspec.utils.logging import get_logger

logger = get_logger("adapters.duckdb.data_dictionary")

__all__ = ("DuckDBDataDictionary",)

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.driver import DuckDBDriver


class DuckDBDataDictionary(DialectSQLMixin, SyncDataDictionaryBase["DuckDBDriver"]):
    """DuckDB-specific sync data dictionary."""

    __slots__ = ()

    dialect = "duckdb"

    def get_version(self, driver: "DuckDBDriver") -> "VersionInfo | None":
        """Get DuckDB database version information.

        Args:
            driver: DuckDB driver instance.

        Returns:
            DuckDB version information or None if detection fails.

        """
        was_cached, cached_version = self.get_cached_version_for_driver(driver)
        if was_cached:
            return cached_version

        version_value = driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            logger.warning("No DuckDB version information found")
            self.cache_version_for_driver(driver, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            logger.warning("Could not parse DuckDB version: %s", version_value)
            self.cache_version_for_driver(driver, None)
            return None

        logger.debug("Detected DuckDB version: %s", version_info)
        self.cache_version_for_driver(driver, version_info)
        return version_info

    def get_feature_flag(self, driver: "DuckDBDriver", feature: str) -> bool:
        """Check if DuckDB database supports a specific feature.

        Args:
            driver: DuckDB driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.

        """
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "DuckDBDriver", type_category: str) -> str:
        """Get optimal DuckDB type for a category.

        Args:
            driver: DuckDB driver instance.
            type_category: Type category.

        Returns:
            DuckDB-specific type name.

        """
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "DuckDBDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using DuckDB catalog."""
        schema_name = self.resolve_schema(schema)
        return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        return driver.select(
            self.get_query("columns_by_table"), table_name=table, schema_name=schema_name, schema_type=ColumnMetadata
        )

    def get_indexes(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        return driver.select(
            self.get_query("indexes_by_table"), table_name=table, schema_name=schema_name, schema_type=IndexMetadata
        )

    def get_foreign_keys(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        return driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )
