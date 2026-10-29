"""SQLite-specific data dictionary for metadata queries."""

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite.core import format_identifier
from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
    sort_ddl_results,
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
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = driver.select_value_or_none(self.get_query("version", "current"))
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
            query_text = self._get_domain_query_text("foreign_keys", "by_schema")
            return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        query_text = self._get_domain_query_text("foreign_keys", "by_table")
        return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ForeignKeyMetadata)

    def get_ddl(
        self,
        driver: "SqliteDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Get native SQLite DDL for a database object from sqlite_schema."""
        return self.get_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
        )

    def get_object_ddl(
        self,
        driver: "SqliteDriver",
        object_name: str,
        *,
        schema: "str | None" = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Get native SQLite DDL for a database object from sqlite_schema."""
        _ = include_dependencies, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("ddl", "by_object").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, schema_name=schema_name, object_name=object_name, object_type=object_type)
        return _sqlite_ddl_result_from_rows(
            dialect=type(self).dialect, object_name=object_name, object_type=object_type, schema=schema_name, rows=rows
        )

    def get_schema_ddl(
        self,
        driver: "SqliteDriver",
        schema: "str | None" = None,
        *,
        include_domains: "Sequence[str] | None" = None,
        exclude_domains: "Sequence[str] | None" = None,
        exclude_tables: "Sequence[str] | None" = ("ddl_migrations",),
        direction: "Literal['create', 'drop']" = "create",
        target_dialect: "str | None" = None,
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "MetadataResult":
        """Get topologically sorted native SQLite DDL for all schema objects."""
        _ = include_domains, exclude_domains, target_dialect, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("ddl", "by_schema").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, schema_name=schema_name)
        foreign_keys = self.get_foreign_keys(driver, schema=schema) if include_dependencies else []
        return _sqlite_schema_ddl_from_rows(
            dialect=type(self).dialect,
            schema=schema_name,
            rows=rows,
            foreign_keys=foreign_keys,
            exclude_tables=exclude_tables,
            direction=direction,
            include_dependencies=include_dependencies,
        )

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


def _sqlite_ddl_result_from_rows(
    *, dialect: str, object_name: str, object_type: str, schema: "str | None", rows: "Sequence[dict[str, Any]]"
) -> DDLResult:
    row = rows[0] if rows else None
    resolved_schema = row.get("schema_name") if row is not None else schema
    resolved_type = str(row.get("object_type") or object_type) if row is not None else object_type
    ddl_value = (row.get("ddl") or row.get("native_sql")) if row is not None else None
    identity = ObjectIdentity(
        name=object_name,
        object_type=resolved_type,
        schema=str(resolved_schema) if resolved_schema is not None else None,
        dialect=dialect,
        source=MetadataSource.CATALOG,
    )
    if ddl_value is None:
        return DDLResult.unsupported(identity, source=MetadataSource.CATALOG)
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.CATALOG,
        ddl=str(ddl_value),
    )


def _sqlite_schema_ddl_from_rows(
    *,
    dialect: str,
    schema: "str | None",
    rows: "Sequence[dict[str, Any]]",
    foreign_keys: "Sequence[ForeignKeyMetadata]",
    exclude_tables: "Sequence[str] | None",
    direction: "Literal['create', 'drop']",
    include_dependencies: bool,
) -> MetadataResult:
    excluded = {name.lower() for name in (exclude_tables or ())}
    filtered_rows: list[tuple[ObjectIdentity, str, str | None]] = []
    table_identities: dict[str, ObjectIdentity] = {}
    view_identities: dict[str, ObjectIdentity] = {}

    for row in rows:
        object_name = str(row.get("object_name") or "")
        object_type = str(row.get("object_type") or "table").lower()
        table_name_val = row.get("table_name")
        table_name = str(table_name_val) if table_name_val is not None else None
        ddl_value = row.get("ddl") or row.get("native_sql")
        if not object_name or ddl_value is None:
            continue
        if object_name.lower() in excluded:
            continue
        if table_name is not None and table_name.lower() in excluded:
            continue
        row_schema = row.get("schema_name", schema)
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=str(row_schema) if row_schema is not None else None,
            dialect=dialect,
            source=MetadataSource.CATALOG,
        )
        filtered_rows.append((identity, str(ddl_value), table_name))
        if object_type == "table":
            table_identities[object_name.lower()] = identity
        elif object_type == "view":
            view_identities[object_name.lower()] = identity

    fk_targets_by_table: dict[str, list[ObjectIdentity]] = {}
    if include_dependencies:
        for fk in foreign_keys:
            fk_table = str(fk["table_name"] if isinstance(fk, Mapping) else fk.table_name)
            fk_ref = str(fk["referenced_table"] if isinstance(fk, Mapping) else fk.referenced_table)
            if fk_table.lower() == fk_ref.lower():
                continue
            ref_identity = table_identities.get(fk_ref.lower())
            if ref_identity is not None:
                fk_targets_by_table.setdefault(fk_table.lower(), []).append(ref_identity)

    results: list[DDLResult] = []
    rel_identities = {**table_identities, **view_identities}
    for identity, ddl_text, table_name in filtered_rows:
        edges: list[DependencyEdge] = []
        if include_dependencies:
            obj_type = identity.object_type.lower()
            if obj_type == "table":
                edges.extend(
                    DependencyEdge(
                        identity, ref_identity, DependencyEdgeKind.FOREIGN_KEY, source=MetadataSource.CATALOG
                    )
                    for ref_identity in fk_targets_by_table.get(identity.name.lower(), ())
                )
            elif obj_type in {"index", "trigger"} and table_name is not None:
                parent_identity = table_identities.get(table_name.lower())
                if parent_identity is not None:
                    edge_kind = (
                        DependencyEdgeKind.TRIGGER_TARGET
                        if obj_type == "trigger"
                        else DependencyEdgeKind.INDEX_EXPRESSION
                    )
                    edges.append(DependencyEdge(identity, parent_identity, edge_kind, source=MetadataSource.CATALOG))
            elif obj_type == "view":
                for ref_key, ref_identity in rel_identities.items():
                    if ref_key == identity.name.lower():
                        continue
                    if re.search(rf"\b{re.escape(ref_identity.name)}\b", ddl_text, re.IGNORECASE):
                        edges.append(
                            DependencyEdge(
                                identity, ref_identity, DependencyEdgeKind.VIEW_REFERENCE, source=MetadataSource.CATALOG
                            )
                        )
        results.append(
            DDLResult(
                identity=identity,
                status=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.NATIVE,
                source=MetadataSource.CATALOG,
                ddl=ddl_text,
                dependencies=tuple(edges),
            )
        )

    ordered = (
        sort_ddl_results(results, order=direction, raise_on_cycles=False) if include_dependencies else tuple(results)
    )
    capability = MetadataCapability(
        domain="ddl", support=MetadataSupport.SUPPORTED, fidelity=MetadataFidelity.NATIVE, source=MetadataSource.CATALOG
    )
    return MetadataResult("ddl", capability=capability, items=ordered)
