"""SQLite-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite.core import format_identifier
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.data_dictionary.dialects.sqlite import list_sqlite_available_features, resolve_sqlite_json_type
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.driver import SqliteDriver

__all__ = ("SqliteDataDictionary",)

SQLITE_INDEX_EXPRESSION_COLUMN_ID = -2


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SqliteDataDictionary(SyncDataDictionaryBase):
    """SQLite-specific sync data dictionary."""

    dialect: ClassVar[str] = "sqlite"

    def __init__(self) -> None:
        super().__init__()

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return get_dialect_config(type(self).dialect).default_schema

    def get_version(self, driver: "SqliteDriver") -> "VersionInfo | None":
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

        config = get_dialect_config(type(self).dialect)
        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
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
        config = get_dialect_config(type(self).dialect)
        version_info = self.get_version(driver)

        if type_category == "json":
            return resolve_sqlite_json_type(version_info)

        return config.get_optimal_type(type_category)

    def list_available_features(self) -> "list[str]":
        """List available feature flags for this dialect."""
        return list_sqlite_available_features()

    def get_tables(self, driver: "SqliteDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using SQLite catalog."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        query_text = self._get_domain_query_text("tables", "by_schema")
        return driver.select(query_text, schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = self._get_domain_query_text("columns", "by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_name=schema_name, schema_type=ColumnMetadata)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        query_text = self._get_domain_query_text("columns", "by_table").format(schema_prefix=schema_prefix)
        return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ColumnMetadata)

    def get_indexes(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
            query_text = self._get_domain_query_text("indexes", "by_schema").format(schema_prefix=schema_prefix)
            rows = driver.select(query_text, schema_name=schema_name)
            return _sqlite_index_rows_to_metadata(rows, schema_name)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("indexes", "by_table").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, table_name=table, schema_name=schema_name)
        return _sqlite_index_rows_to_metadata(rows, schema_name)

    def get_foreign_keys(
        self, driver: "SqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            query_text = self._get_domain_query_text("constraints", "foreign_keys_by_schema")
            return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        query_text = self._get_domain_query_text("constraints", "foreign_keys_by_table")
        return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ForeignKeyMetadata)

    def _get_domain_query_text(self, domain: str, query_name: str) -> str:
        """Return a required direct-domain query for SQLite."""
        query_text = get_data_dictionary_loader().get_domain_query_text(type(self).dialect, domain, query_name)
        if query_text is None:
            msg = f"Missing SQLite data-dictionary query: {domain}/{query_name}"
            raise RuntimeError(msg)
        return query_text


def _sqlite_index_rows_to_metadata(rows: "list[dict[str, Any]]", schema_name: "str | None") -> "list[IndexMetadata]":
    """Group SQLite index_xinfo rows into the public index metadata shape."""
    if not rows:
        return []
    first_row = rows[0]
    if "columns" in first_row:
        return cast("list[IndexMetadata]", rows)

    grouped: dict[tuple[str | None, str, str], dict[str, Any]] = {}
    for row in rows:
        table_name_value = row.get("table_name")
        index_name_value = row.get("index_name")
        if table_name_value is None or index_name_value is None:
            continue
        row_schema = cast("str | None", row.get("schema_name", schema_name))
        table_name = str(table_name_value)
        index_name = str(index_name_value)
        key = (row_schema, table_name, index_name)
        index_metadata = grouped.get(key)
        if index_metadata is None:
            index_metadata = {
                "index_name": index_name,
                "table_name": table_name,
                "columns": [],
                "is_primary": bool(row.get("is_primary")),
            }
            if row_schema is not None:
                index_metadata["schema_name"] = row_schema
            unique_value = row.get("is_unique")
            if unique_value is not None:
                index_metadata["is_unique"] = unique_value
            if row.get("is_partial") is not None:
                index_metadata["is_partial"] = row.get("is_partial")
            if row.get("index_sql") is not None:
                index_metadata["native_sql"] = row.get("index_sql")
            grouped[key] = index_metadata

        if row.get("is_key_column") in (0, False):
            continue
        columns = cast("list[str]", index_metadata["columns"])
        column_name = row.get("column_name")
        if column_name is not None:
            columns.append(str(column_name))
        elif row.get("column_id") == SQLITE_INDEX_EXPRESSION_COLUMN_ID:
            columns.append("<expression>")

    return [cast("IndexMetadata", metadata) for metadata in grouped.values()]
