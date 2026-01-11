"""Mock-specific data dictionary for metadata queries.

This module provides data dictionary functionality for the mock adapter,
delegating to SQLite's catalog since mock uses SQLite as its execution backend.
"""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.adapters.mock.core import format_identifier
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

__all__ = ("MockAsyncDataDictionary", "MockDataDictionary")

if TYPE_CHECKING:
    from sqlspec.adapters.mock.driver import MockAsyncDriver, MockSyncDriver


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MockDataDictionary(SyncDataDictionaryBase):
    """Mock-specific sync data dictionary.

    Delegates metadata queries to SQLite's catalog (sqlite_master, PRAGMA table_info).
    """

    dialect: ClassVar[str] = "sqlite"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "MockSyncDriver") -> "VersionInfo | None":
        """Get SQLite database version information.

        Args:
            driver: Sync database driver instance.

        Returns:
            SQLite version information or None if detection fails.
        """
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        # Not cached, fetch from database

        version_value = driver.select_value_or_none(self.get_query("version"))
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

    def get_feature_flag(self, driver: "MockSyncDriver", feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: Sync database driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.
        """
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "MockSyncDriver", type_category: str) -> str:
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

    def get_tables(self, driver: "MockSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using SQLite catalog."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self.get_query_text("tables_by_schema").format(schema_prefix=schema_prefix)
        return driver.select(query_text, schema_type=TableMetadata)

    def get_columns(
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
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
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
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
        self, driver: "MockSyncDriver", table: "str | None" = None, schema: "str | None" = None
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


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MockAsyncDataDictionary(AsyncDataDictionaryBase):
    """Mock-specific async data dictionary.

    Delegates metadata queries to SQLite's catalog (sqlite_master, PRAGMA table_info).
    """

    dialect: ClassVar[str] = "sqlite"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: "MockAsyncDriver") -> "VersionInfo | None":
        """Get SQLite database version information.

        Args:
            driver: Async database driver instance.

        Returns:
            SQLite version information or None if detection fails.
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

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "MockAsyncDriver", feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: Async database driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.
        """
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "MockAsyncDriver", type_category: str) -> str:
        """Get optimal SQLite type for a category.

        Args:
            driver: Async database driver instance.
            type_category: Type category.

        Returns:
            SQLite-specific type name.
        """
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            if version_info and json_version and version_info >= json_version:
                return "JSON"
            return "TEXT"

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "MockAsyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using SQLite catalog."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self.get_query_text("tables_by_schema").format(schema_prefix=schema_prefix)
        return await driver.select(query_text, schema_type=TableMetadata)

    async def get_columns(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = self.get_query_text("columns_by_schema").format(schema_prefix=schema_prefix)
            return await driver.select(query_text, schema_type=ColumnMetadata)

        assert table is not None
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        table_name = table
        table_identifier = f"{schema_name}.{table_name}" if schema_name else table_name
        query_text = self.get_query_text("columns_by_table").format(table_name=format_identifier(table_identifier))
        return await driver.select(query_text, schema_type=ColumnMetadata)

    async def get_indexes(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        indexes: list[IndexMetadata] = []
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            for table_info in await self.get_tables(driver, schema=schema_name):
                table_name = table_info.get("table_name")
                if not table_name:
                    continue
                indexes.extend(await self.get_indexes(driver, table=table_name, schema=schema_name))
            return indexes

        assert table is not None
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        table_name = table
        table_identifier = f"{schema_name}.{table_name}" if schema_name else table_name
        index_list_sql = self.get_query_text("indexes_by_table").format(table_name=format_identifier(table_identifier))
        index_rows = await driver.select(index_list_sql)
        for row in index_rows:
            index_name = row.get("name")
            if not index_name:
                continue
            index_identifier = f"{schema_name}.{index_name}" if schema_name else index_name
            columns_sql = self.get_query_text("index_columns_by_index").format(
                index_name=format_identifier(index_identifier)
            )
            columns_rows = await driver.select(columns_sql)
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

    async def get_foreign_keys(
        self, driver: "MockAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            query_text = self.get_query_text("foreign_keys_by_schema").format(schema_prefix=schema_prefix)
            return await driver.select(query_text, schema_type=ForeignKeyMetadata)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        table_label = table.replace("'", "''")
        table_identifier = f"{schema_name}.{table}" if schema_name else table
        query_text = self.get_query_text("foreign_keys_by_table").format(
            table_name=format_identifier(table_identifier), table_label=table_label
        )
        return await driver.select(query_text, schema_type=ForeignKeyMetadata)
