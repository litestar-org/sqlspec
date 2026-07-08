"""DuckDB-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DependencyMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataResult,
    MetadataSource,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.driver import DuckDBDriver

__all__ = ("DuckDBDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class DuckDBDataDictionary(SyncDataDictionaryBase):
    """DuckDB-specific sync data dictionary."""

    dialect: ClassVar[str] = "duckdb"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "DuckDBDriver") -> "VersionInfo | None":
        """Get DuckDB database version information.

        Args:
            driver: DuckDB driver instance.

        Returns:
            DuckDB version information or None if detection fails.
        """
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

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
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self._get_domain_query_text("columns", "by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return driver.select(
            self._get_domain_query_text("columns", "by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ColumnMetadata,
        )

    def get_indexes(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = driver.select(self._get_domain_query_text("indexes", "by_schema"), schema_name=schema_name)
            return _duckdb_index_rows_to_metadata(rows)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        rows = driver.select(
            self._get_domain_query_text("indexes", "by_table"), table_name=table, schema_name=schema_name
        )
        return _duckdb_index_rows_to_metadata(rows)

    def get_foreign_keys(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )

    def get_dependencies(
        self, driver: "DuckDBDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get DuckDB object dependency metadata from duckdb_dependencies()."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=object_name, operation="dependencies")
        rows = driver.select(self._get_domain_query_text("dependencies", "by_schema"), schema_name=schema_name)
        items: list[DependencyMetadata] = []
        for row in rows:
            if (
                object_name is not None
                and row.get("object_name") != object_name
                and row.get("referenced_object_name") != object_name
            ):
                continue
            name = str(row.get("object_name") or row.get("objid") or "dependency")
            object_type = str(row.get("object_type") or "dependency")
            identity = ObjectIdentity(
                name=name,
                object_type=object_type,
                schema=cast("str | None", row.get("schema_name", schema_name)),
                dialect=type(self).dialect,
                source=MetadataSource.CATALOG,
            )
            items.append(DependencyMetadata(identity, attributes=dict(row)))
        return MetadataResult("dependencies", items=tuple(items))

    def _get_domain_query_text(self, domain: str, query_name: str) -> str:
        """Return a required direct-domain query for DuckDB."""
        query_text = get_data_dictionary_loader().get_domain_query_text(type(self).dialect, domain, query_name)
        if query_text is None:
            msg = f"Missing DuckDB data-dictionary query: {domain}/{query_name}"
            raise RuntimeError(msg)
        return query_text


def _duckdb_index_rows_to_metadata(rows: "list[dict[str, Any]]") -> "list[IndexMetadata]":
    """Normalize duckdb_indexes() rows into the public index metadata shape."""
    indexes: list[IndexMetadata] = []
    for row in rows:
        index_name = row.get("index_name")
        table_name = row.get("table_name")
        if index_name is None or table_name is None:
            continue
        index_metadata: dict[str, Any] = {
            "index_name": str(index_name),
            "table_name": str(table_name),
            "columns": _duckdb_index_columns(row.get("columns")),
            "is_unique": bool(row.get("is_unique")),
            "is_primary": bool(row.get("is_primary")),
        }
        schema_name = row.get("schema_name")
        if schema_name is not None:
            index_metadata["schema_name"] = str(schema_name)
        native_sql = row.get("native_sql")
        if native_sql is not None:
            index_metadata["native_sql"] = native_sql
        indexes.append(cast("IndexMetadata", index_metadata))
    return indexes


def _duckdb_index_columns(value: Any) -> list[str]:
    """Return a stable list from duckdb_indexes().expressions."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return [str(value)]
    cleaned = value.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    if not cleaned:
        return []
    return [part.strip().strip('"') for part in cleaned.split(",")]
