"""SQLite-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite.core import format_identifier
from sqlspec.data_dictionary import DialectSQLMixin
from sqlspec.driver import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    SyncDataDictionaryBase,
    TableMetadata,
    VersionInfo,
)

__all__ = ("SqliteDataDictionary",)

if TYPE_CHECKING:
    import re

    from sqlspec.adapters.sqlite.driver import SqliteDriver
    from sqlspec.data_dictionary import DialectConfig


@mypyc_attr(native_class=False)
class SqliteDataDictionary(DialectSQLMixin, SyncDataDictionaryBase["SqliteDriver"]):
    """SQLite-specific sync data dictionary."""

    __slots__ = ()

    dialect = "sqlite"

    def get_cached_version(self, driver_id: int) -> "tuple[bool, VersionInfo | None]":
        """Get cached version info for a driver."""
        return super().get_cached_version(driver_id)

    def cache_version(self, driver_id: int, version: "VersionInfo | None") -> None:
        """Cache version info for a driver."""
        super().cache_version(driver_id, version)

    def parse_version_with_pattern(self, pattern: "re.Pattern[str]", version_str: str) -> "VersionInfo | None":
        """Parse version string using a specific regex pattern."""
        return super().parse_version_with_pattern(pattern, version_str)

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return super().get_dialect_config()

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        return super().resolve_schema(schema)

    def resolve_feature_flag(self, feature: str, version: "VersionInfo | None") -> bool:
        """Resolve a feature flag using dialect config and version info."""
        return super().resolve_feature_flag(feature, version)

    def get_cached_version_for_driver(self, driver: "SqliteDriver") -> "tuple[bool, VersionInfo | None]":
        """Get cached version info for a driver instance."""
        return self.get_cached_version(id(driver))

    def get_version(self, driver: "SqliteDriver") -> "VersionInfo | None":
        """Get SQLite database version information.

        Args:
            driver: Sync database driver instance.

        Returns:
            SQLite version information or None if detection fails.

        """
        was_cached, cached_version = self.get_cached_version_for_driver(driver)
        if was_cached:
            return cached_version

        version_value = driver.select_value_or_none(self.get_query("version"))
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

    def get_feature_flag(self, driver: "SqliteDriver", feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: Sync database driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.

        """
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "SqliteDriver", type_category: str) -> str:
        """Get optimal SQLite type for a category.

        Args:
            driver: Sync database driver instance.
            type_category: Type category.

        Returns:
            SQLite-specific type name.

        """
        config = self.get_dialect_config()
        version_info = self.get_version(driver)

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    def get_tables(self, driver: "SqliteDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using SQLite catalog."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self.get_query_text("tables_by_schema").format(schema_prefix=schema_prefix)
        return driver.select(query_text, schema_type=TableMetadata)

    def get_columns(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = self.get_query_text("columns_by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_type=ColumnMetadata)

        assert table is not None
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        table_name = table
        table_identifier = f"{schema_name}.{table_name}" if schema_name else table_name
        query_text = self.get_query_text("columns_by_table").format(table_name=format_identifier(table_identifier))
        return driver.select(query_text, schema_type=ColumnMetadata)

    def get_indexes(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        indexes: list[IndexMetadata] = []
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            for table_info in self.get_tables(driver, schema=schema_name):
                table_name = table_info.get("table_name")
                if not table_name:
                    continue
                indexes.extend(self.get_indexes(driver, table=table_name, schema=schema_name))
            return indexes

        assert table is not None
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        table_name = table
        table_identifier = f"{schema_name}.{table_name}" if schema_name else table_name
        index_list_sql = self.get_query_text("indexes_by_table").format(table_name=format_identifier(table_identifier))
        index_rows = driver.select(index_list_sql)
        for row in index_rows:
            index_name = row.get("name")
            if not index_name:
                continue
            index_identifier = f"{schema_name}.{index_name}" if schema_name else index_name
            columns_sql = self.get_query_text("index_columns_by_index").format(
                index_name=format_identifier(index_identifier)
            )
            columns_rows = driver.select(columns_sql)
            columns: list[str] = []
            for col in columns_rows:
                column_name = col.get("name")
                if column_name is None:
                    continue
                columns.append(str(column_name))
            is_primary = row.get("origin") == "pk"
            index_metadata: IndexMetadata = {
                "index_name": index_name,
                "table_name": table_name,
                "columns": columns,
                "is_primary": is_primary,
            }
            if schema_name is not None:
                index_metadata["schema_name"] = schema_name
            unique_value = row.get("unique")
            if unique_value is not None:
                index_metadata["is_unique"] = unique_value
            indexes.append(index_metadata)
        return indexes

    def get_foreign_keys(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            query_text = self.get_query_text("foreign_keys_by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_type=ForeignKeyMetadata)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        table_label = table.replace("'", "''")
        table_identifier = f"{schema_name}.{table}" if schema_name else table
        query_text = self.get_query_text("foreign_keys_by_table").format(
            table_name=format_identifier(table_identifier), table_label=table_label
        )
        return driver.select(query_text, schema_type=ForeignKeyMetadata)

    def list_available_features(self) -> "list[str]":
        """List all features available for the dialect."""
        return super().list_available_features()
