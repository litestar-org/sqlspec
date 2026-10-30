"""SAP SQL Anywhere database data dictionary implementation."""

import logging
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    TableMetadata,
)
from sqlspec.data_dictionary.dialects.sqlanywhere import (
    SQLANYWHERE_CONFIG,
    SQLANYWHERE_VERSION_PATTERN,
    SqlanywhereVersionInfo,
    build_sqlanywhere_metadata_capability_profile,
    extract_sqlanywhere_version_value,
    list_sqlanywhere_available_features,
    merge_sqlanywhere_table_lists,
    resolve_sqlanywhere_feature_flag,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlspec.adapters.sqlanywhere.driver import SqlanywhereDriver

__all__ = (
    "SQLANYWHERE_CONFIG",
    "SqlanywhereDataDictionary",
    "SqlanywhereSyncDataDictionary",
    "SqlanywhereVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_sqlanywhere_version",
)

logger = get_logger("sqlspec.adapters.sqlanywhere.data_dictionary")
_VERSION_PARTS_WITH_BUILD = 4


def parse_sqlanywhere_version(row: "dict[str, Any] | None") -> "SqlanywhereVersionInfo | None":
    """Parse the SQL Anywhere version string returned by the version query."""
    version_str = extract_sqlanywhere_version_value(row)
    match = SQLANYWHERE_VERSION_PATTERN.search(version_str) if version_str else None
    if match is None:
        log_with_context(
            logger, logging.DEBUG, "data_dictionary.version.unparsed", dialect="sqlanywhere", value=version_str
        )
        return None
    patch_val = int(match.group(3)) if match.group(3) else 0
    parts = version_str.split(".") if version_str else []
    build_number = parts[3].split()[0] if len(parts) >= _VERSION_PARTS_WITH_BUILD else None
    return SqlanywhereVersionInfo(int(match.group(1)), int(match.group(2)), patch_val, build_number=build_number)


def build_column_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ColumnMetadata]":
    """Convert SQL Anywhere SYS.SYSTABCOL query rows to column metadata."""
    return [
        ColumnMetadata(
            schema_name=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            data_type=str(r.get("data_type", "")),
            is_nullable=bool(r.get("is_nullable", True)),
            column_default=str(r["column_default"]) if r.get("column_default") is not None else None,
            ordinal_position=int(r.get("ordinal_position", 1)),
            max_length=int(r.get("max_length", 0)) if r.get("max_length") is not None else 0,
            numeric_scale=int(r.get("numeric_scale", 0)) if r.get("numeric_scale") is not None else 0,
            is_primary=bool(r.get("is_primary")),
            is_unique=bool(r.get("is_unique")),
            identity_generation=str(r["identity_generation"])
            if r.get("identity_generation")
            else ("BY DEFAULT" if r.get("is_identity") else None),
            is_generated=bool(r.get("is_generated")),
        )
        for r in rows
    ]


def build_index_metadata(rows: "Iterable[dict[str, Any]]") -> "list[IndexMetadata]":
    """Group per-column SQL Anywhere SYS.SYSIDX rows into index metadata."""
    indexes_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r.get("schema_name", "")), str(r.get("table_name", "")), str(r.get("index_name", "")))
        if key not in indexes_map:
            indexes_map[key] = {
                "schema_name": key[0],
                "table_name": key[1],
                "index_name": key[2],
                "columns": [],
                "is_unique": bool(r.get("is_unique", 0)),
                "is_primary": bool(r.get("is_primary", 0)),
            }
        col_name = str(r.get("column_name", ""))
        if col_name:
            indexes_map[key]["columns"].append(col_name)

    return [
        IndexMetadata(
            schema_name=idx["schema_name"],
            table_name=idx["table_name"],
            index_name=idx["index_name"],
            columns=idx["columns"],
            is_unique=idx["is_unique"],
            is_primary=idx["is_primary"],
        )
        for idx in indexes_map.values()
    ]


def build_foreign_key_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ForeignKeyMetadata]":
    """Convert SQL Anywhere SYS.SYSFOREIGNKEY query rows to foreign key metadata."""
    return [
        ForeignKeyMetadata(
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            referenced_table=str(r.get("referenced_table_name", "")),
            referenced_column=str(r.get("referenced_column_name", "")),
            constraint_name=str(r.get("constraint_name", "")),
            schema=str(r["schema_name"]) if r.get("schema_name") else None,
            referenced_schema=str(r["referenced_schema"]) if r.get("referenced_schema") else None,
        )
        for r in rows
    ]


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SqlanywhereSyncDataDictionary(SyncDataDictionaryBase):
    """Synchronous SAP SQL Anywhere data dictionary."""

    dialect: ClassVar[str] = "sqlanywhere"

    def __init__(self) -> None:
        super().__init__()

    def get_metadata_capabilities(
        self, driver: "SqlanywhereDriver", domains: "Sequence[str] | None" = None
    ) -> MetadataCapabilityProfile:
        """Return the SQL Anywhere metadata capability profile."""
        _ = driver
        return build_sqlanywhere_metadata_capability_profile(type(self).__name__, domains)

    def get_version(self, driver: "SqlanywhereDriver") -> "SqlanywhereVersionInfo | None":
        """Detect and cache the SQL Anywhere server version."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("SqlanywhereVersionInfo | None", self._version_cache.get(driver_id))

        version_row = driver.select_one_or_none(self.get_query("version", "current"))
        version_info = parse_sqlanywhere_version(version_row)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "missing")
        else:
            self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "SqlanywhereDriver", feature: str) -> bool:
        """Check whether the connected SQL Anywhere server supports a feature."""
        version_info = self.get_version(driver)
        return resolve_sqlanywhere_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "SqlanywhereDriver", type_category: str) -> str:
        """Return the optimal SQL Anywhere SQL type for a logical category."""
        _ = driver
        return SQLANYWHERE_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> "list[str]":
        """List feature flags defined for the SQL Anywhere dialect."""
        return list_sqlanywhere_available_features()

    def get_tables(self, driver: "SqlanywhereDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Return tables in topological dependency order for a SQL Anywhere schema."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered_rows = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        all_rows = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "all_by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        return merge_sqlanywhere_table_lists(ordered_rows, all_rows)

    def get_columns(
        self, driver: "SqlanywhereDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Return column metadata for a table or entire SQL Anywhere schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            rows = driver.select(self.get_query("columns", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
            rows = driver.select(self.get_query("columns", "by_table"), schema_name=schema_name, table_name=table_name)
        return build_column_metadata(rows)

    def get_indexes(
        self, driver: "SqlanywhereDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Return index metadata for a table or entire SQL Anywhere schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = driver.select(self.get_query("indexes", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
            rows = driver.select(self.get_query("indexes", "by_table"), schema_name=schema_name, table_name=table_name)
        return build_index_metadata(rows)

    def get_foreign_keys(
        self, driver: "SqlanywhereDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Return foreign key metadata for a table or entire SQL Anywhere schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
            rows = driver.select(
                self.get_query("foreign_keys", "by_table"), schema_name=schema_name, table_name=table_name
            )
        return build_foreign_key_metadata(rows)

    def get_constraints(
        self, driver: "SqlanywhereDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get SQL Anywhere constraint metadata."""
        schema_name = self.resolve_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        query = self.get_query("constraints", "by_table" if table_name else "by_schema")
        rows = driver.select(query, schema_name=schema_name, table_name=table_name)
        return MetadataResult("constraints", items=tuple(rows))

    def get_views(self, driver: "SqlanywhereDriver", schema: "str | None" = None) -> MetadataResult:
        """Get SQL Anywhere view metadata."""
        schema_name = self.resolve_schema(schema)
        query = self.get_query("views", "by_schema")
        rows = driver.select(query, schema_name=schema_name, view_name=None)
        return MetadataResult("views", items=tuple(rows))

    def get_objects(self, driver: "SqlanywhereDriver", schema: "str | None" = None) -> MetadataResult:
        """Get tables, views, and procedures from the SQL Anywhere catalog."""
        rows = driver.select(
            self.get_query("objects", "by_schema"), schema_name=self.resolve_schema(schema), object_name=None
        )
        return MetadataResult("objects", items=tuple(rows))

    def get_schemas(self, driver: "SqlanywhereDriver") -> MetadataResult:
        """Get SQL Anywhere schema metadata."""
        query = self.get_query("schemas", "by_schema")
        rows = driver.select(query, schema_name=None)
        return MetadataResult("schemas", items=tuple(rows))


SqlanywhereDataDictionary = SqlanywhereSyncDataDictionary
