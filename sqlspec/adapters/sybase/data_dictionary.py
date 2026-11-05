"""Sybase ASE database data dictionary implementation."""

import logging
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    MetadataSource,
    ObjectIdentity,
    TableMetadata,
    dependency_edges_from_foreign_keys,
    sort_dependencies,
)
from sqlspec.data_dictionary.dialects.sybase import (
    SYBASE_CONFIG,
    SYBASE_VERSION_PATTERN,
    SybaseVersionInfo,
    build_sybase_metadata_capability_profile,
    extract_sybase_version_value,
    list_sybase_available_features,
    merge_sybase_table_lists,
    resolve_sybase_feature_flag,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlspec.adapters.sybase.driver import SybaseDriver

__all__ = (
    "SYBASE_CONFIG",
    "SybaseDataDictionary",
    "SybaseSyncDataDictionary",
    "SybaseVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_sybase_version",
)

logger = get_logger("sqlspec.adapters.sybase.data_dictionary")


def parse_sybase_version(row: "dict[str, Any] | None") -> "SybaseVersionInfo | None":
    """Parse the Sybase ASE version banner returned by the version query."""
    version_str = extract_sybase_version_value(row)
    match = SYBASE_VERSION_PATTERN.search(version_str) if version_str else None
    if match is None:
        log_with_context(logger, logging.DEBUG, "data_dictionary.version.unparsed", dialect="sybase", value=version_str)
        return None
    sp_num = int(match.group(3)) if match.group(3) else 0
    sp_label = f"SP{sp_num:02d}" if match.group(3) else None
    return SybaseVersionInfo(int(match.group(1)), int(match.group(2)), sp_num, service_pack=sp_label)


def build_column_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ColumnMetadata]":
    """Convert Sybase syscolumns query rows to column metadata."""
    return [
        ColumnMetadata(
            schema_name=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            data_type=str(r.get("data_type", "")),
            is_nullable=_catalog_flag(r.get("is_nullable", True)),
            column_default=str(r["column_default"]) if r.get("column_default") is not None else None,
            ordinal_position=int(r.get("ordinal_position", 1)),
            max_length=int(r.get("max_length", 0)) if r.get("max_length") is not None else 0,
            numeric_precision=int(r.get("numeric_precision", 0)) if r.get("numeric_precision") is not None else 0,
            numeric_scale=int(r.get("numeric_scale", 0)) if r.get("numeric_scale") is not None else 0,
            is_primary=_catalog_flag(r.get("is_primary")),
            is_unique=_catalog_flag(r.get("is_unique")),
            is_identity=_catalog_flag(r.get("is_identity")),
            identity_generation=str(r["identity_generation"])
            if r.get("identity_generation")
            else ("ALWAYS" if _catalog_flag(r.get("is_identity")) else None),
            is_generated=_catalog_flag(r.get("is_generated")),
            generation_expression=str(r["generation_expression"])
            if r.get("generation_expression") is not None
            else None,
            is_hidden=_catalog_flag(r.get("is_hidden")),
        )
        for r in _merge_definition_rows(
            rows, ("schema_name", "table_name", "column_name"), ("column_default", "generation_expression")
        )
    ]


def _catalog_flag(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().upper() in {"YES", "Y", "TRUE", "1"}
    return bool(value)


def _merge_definition_rows(
    rows: "Iterable[dict[str, Any]]", identity_fields: "tuple[str, ...]", text_fields: "tuple[str, ...]"
) -> "list[dict[str, Any]]":
    """Assemble catalog definition chunks without changing their whitespace or multiplicity."""
    chunk_fields = ("_definition_id", "_definition_number", "_definition_colid2", "_definition_colid")
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(field) for field in identity_fields)
        grouped.setdefault(key, []).append(row)
    result: list[dict[str, Any]] = []
    for chunks in grouped.values():
        ordered = sorted(chunks, key=lambda row: tuple(int(row.get(field) or 0) for field in chunk_fields))
        merged = {field: value for field, value in ordered[0].items() if field not in chunk_fields}
        for field in text_fields:
            parts = [str(row[field]) for row in ordered if row.get(field) is not None]
            merged[field] = "".join(parts) if parts else None
        result.append(merged)
    return result


def build_index_metadata(rows: "Iterable[dict[str, Any]]") -> "list[IndexMetadata]":
    """Group per-column Sybase sysindexes rows into index metadata."""
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
            schema_name=v["schema_name"],
            table_name=v["table_name"],
            index_name=v["index_name"],
            columns=v["columns"],
            is_unique=v["is_unique"],
            is_primary=v["is_primary"],
        )
        for v in indexes_map.values()
    ]


def build_foreign_key_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ForeignKeyMetadata]":
    """Convert Sybase sysreferences query rows to foreign key metadata."""
    return [
        ForeignKeyMetadata(
            schema=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            constraint_name=str(r.get("constraint_name", "")),
            column_name=str(r.get("column_name", "")),
            referenced_schema=str(r.get("referenced_schema", "")),
            referenced_table=str(r.get("referenced_table", "")),
            referenced_column=str(r.get("referenced_column", "")),
        )
        for r in rows
    ]


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SybaseSyncDataDictionary(SyncDataDictionaryBase):
    """Sybase ASE sync data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "sybase"

    def __init__(self) -> None:
        """Initialize Sybase ASE sync data dictionary."""
        super().__init__()

    def get_metadata_capabilities(
        self, driver: "SybaseDriver", domains: "Sequence[str] | None" = None
    ) -> MetadataCapabilityProfile:
        """Get Sybase ASE data-dictionary capability profile."""
        _ = driver
        return build_sybase_metadata_capability_profile(type(self).__name__, domains)

    def get_version(self, driver: "SybaseDriver") -> SybaseVersionInfo | None:
        """Get Sybase ASE database version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("SybaseVersionInfo | None", self._version_cache.get(driver_id))

        version_info = parse_sybase_version(driver.select_one_or_none(self.get_query("version", "current")))
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "SybaseDriver", feature: str) -> bool:
        """Check whether Sybase ASE supports a feature."""
        version_info = self.get_version(driver)
        return resolve_sybase_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "SybaseDriver", type_category: str) -> str:
        """Get optimal Sybase ASE type for a category."""
        _ = driver
        return SYBASE_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> list[str]:
        """List available feature flags for this dialect."""
        return list_sybase_available_features()

    def get_tables(self, driver: "SybaseDriver", schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
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
        rows = merge_sybase_table_lists(ordered_rows, all_rows)
        table_rows = {
            ObjectIdentity(
                row["table_name"],
                "table",
                schema=row.get("schema_name"),
                dialect="sybase",
                source=MetadataSource.CATALOG,
            ): row
            for row in rows
            if row.get("table_name") and row.get("table_type", "TABLE").upper() != "VIEW"
        }
        if not table_rows:
            return rows
        identities = sorted(table_rows, key=lambda node: (node.schema or "", node.name))
        edges = tuple(
            edge
            for edge in dependency_edges_from_foreign_keys(
                self.get_foreign_keys(driver, schema=schema_name), dialect="sybase"
            )
            if edge.from_object in table_rows and edge.to_object in table_rows
        )
        dependencies: dict[ObjectIdentity, set[ObjectIdentity]] = {node: set() for node in identities}
        for edge in edges:
            dependencies[edge.from_object].add(edge.to_object)
        ordered = sort_dependencies(identities, edges).ordered
        levels: dict[ObjectIdentity, int] = {}
        result: list[TableMetadata] = []
        for node in ordered:
            level = max((levels[parent] + 1 for parent in dependencies[node]), default=0)
            levels[node] = level
            result.append({**table_rows[node], "dependency_level": level, "level": level})
        result.extend(
            {**table_rows[node], "dependency_level": 0, "level": 0} for node in identities if node not in levels
        )
        result.extend(row for row in rows if row.get("table_type", "TABLE").upper() == "VIEW")
        return result

    def get_columns(
        self, driver: "SybaseDriver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get columns for a table or schema from dbo.syscolumns."""
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
        self, driver: "SybaseDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get indexes for a table or schema from dbo.sysindexes."""
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
        self, driver: "SybaseDriver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign keys from dbo.sysreferences."""
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
        self, driver: "SybaseDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get Sybase ASE constraint metadata."""
        schema_name = self.resolve_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        query = self.get_query("constraints", "by_table" if table_name else "by_schema")
        rows = driver.select(query, schema_name=schema_name, table_name=table_name)
        return MetadataResult(
            "constraints",
            items=tuple(
                _merge_definition_rows(
                    rows, ("schema_name", "table_name", "constraint_name", "constraint_type"), ("check_clause",)
                )
            ),
        )

    def get_views(self, driver: "SybaseDriver", schema: str | None = None) -> MetadataResult:
        """Get Sybase ASE view metadata."""
        schema_name = self.resolve_schema(schema)
        query = self.get_query("views", "by_schema")
        rows = driver.select(query, schema_name=schema_name, view_name=None)
        return MetadataResult(
            "views", items=tuple(_merge_definition_rows(rows, ("schema_name", "view_name"), ("definition",)))
        )

    def get_objects(self, driver: "SybaseDriver", schema: "str | None" = None) -> MetadataResult:
        """Get tables, views, and procedures from the Sybase ASE catalog."""
        rows = driver.select(
            self.get_query("objects", "by_schema"), schema_name=self.resolve_schema(schema), object_name=None
        )
        return MetadataResult("objects", items=tuple(rows))

    def get_schemas(self, driver: "SybaseDriver") -> MetadataResult:
        """Get Sybase ASE schema metadata."""
        query = self.get_query("schemas", "by_schema")
        rows = driver.select(query, schema_name=None)
        return MetadataResult("schemas", items=tuple(rows))


SybaseDataDictionary = SybaseSyncDataDictionary
