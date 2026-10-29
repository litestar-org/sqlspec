"""DuckDB-specific data dictionary for metadata queries."""

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    DependencyMetadata,
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
    sort_ddl_results,
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

        version_value = driver.select_value_or_none(self.get_query("version", "current"))
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
        return driver.select(self.get_query("tables", "by_schema"), schema_name=schema_name, schema_type=TableMetadata)

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
                self.get_query("foreign_keys", "by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys", "by_table"),
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

    def get_ddl(
        self,
        driver: "DuckDBDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get native DuckDB DDL for a database object."""
        return self.get_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    def get_object_ddl(
        self,
        driver: "DuckDBDriver",
        object_name: str,
        *,
        schema: "str | None" = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get native DuckDB DDL for a database object."""
        _ = include_dependencies, redact
        if object_type.lower() == "table" and (not prefer_native or target_dialect is not None):
            return self._synthesize_table_ddl(driver, object_name, schema=schema, target_dialect=target_dialect)
        schema_name = self.resolve_schema(schema)
        rows = driver.select(
            self._get_domain_query_text("ddl", "by_object"),
            schema_name=schema_name,
            object_name=object_name,
            object_type=object_type,
        )
        res = _duckdb_ddl_result_from_rows(
            dialect=type(self).dialect, object_name=object_name, object_type=object_type, schema=schema_name, rows=rows
        )
        if target_dialect is not None:
            return self._transpile_ddl_result(res, target_dialect=target_dialect)
        return res

    def get_schema_ddl(
        self,
        driver: "DuckDBDriver",
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
        """Get topologically sorted native DuckDB DDL for all schema objects."""
        if (
            not prefer_native
            or target_dialect is not None
            or include_domains is not None
            or exclude_domains is not None
        ):
            return super().get_schema_ddl(
                driver,
                schema=schema,
                include_domains=include_domains,
                exclude_domains=exclude_domains,
                exclude_tables=exclude_tables,
                direction=direction,
                include_dependencies=include_dependencies,
                prefer_native=prefer_native,
                redact=redact,
                target_dialect=target_dialect,
            )
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("ddl", "by_schema"), schema_name=schema_name)
        foreign_keys = self.get_foreign_keys(driver, schema=schema) if include_dependencies else []
        return _duckdb_schema_ddl_from_rows(
            dialect=type(self).dialect,
            schema=schema_name,
            rows=rows,
            foreign_keys=foreign_keys,
            exclude_tables=exclude_tables,
            direction=direction,
            include_dependencies=include_dependencies,
        )

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


def _duckdb_ddl_result_from_rows(
    *, dialect: str, object_name: str, object_type: str, schema: "str | None", rows: "Sequence[dict[str, Any]]"
) -> DDLResult:
    row = rows[0] if rows else None
    resolved_schema = row.get("schema_name") if row is not None else schema
    resolved_type = str(row.get("object_type") or object_type) if row is not None else object_type
    ddl_value = row.get("ddl") if row is not None else None
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


def _duckdb_schema_ddl_from_rows(
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
    sequence_identities: dict[str, ObjectIdentity] = {}
    type_identities: dict[str, ObjectIdentity] = {}

    for row in rows:
        object_name = str(row.get("object_name") or "")
        object_type = str(row.get("object_type") or "table").lower()
        table_name_val = row.get("table_name")
        table_name = str(table_name_val) if table_name_val is not None else None
        ddl_value = row.get("ddl")
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
        if object_type in {"table", "temporary_table"}:
            table_identities[object_name.lower()] = identity
        elif object_type == "view":
            view_identities[object_name.lower()] = identity
        elif object_type == "sequence":
            sequence_identities[object_name.lower()] = identity
        elif object_type == "type":
            type_identities[object_name.lower()] = identity

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
            if obj_type in {"table", "temporary_table"}:
                edges.extend(
                    DependencyEdge(
                        identity, ref_identity, DependencyEdgeKind.FOREIGN_KEY, source=MetadataSource.CATALOG
                    )
                    for ref_identity in fk_targets_by_table.get(identity.name.lower(), ())
                )
                edges.extend(
                    DependencyEdge(
                        identity, seq_identity, DependencyEdgeKind.SEQUENCE_USE, source=MetadataSource.CATALOG
                    )
                    for seq_identity in sequence_identities.values()
                    if re.search(rf"\b{re.escape(seq_identity.name)}\b", ddl_text, re.IGNORECASE)
                )
                edges.extend(
                    DependencyEdge(
                        identity, type_identity, DependencyEdgeKind.DEFAULT_EXPRESSION, source=MetadataSource.CATALOG
                    )
                    for type_identity in type_identities.values()
                    if re.search(rf"\b{re.escape(type_identity.name)}\b", ddl_text, re.IGNORECASE)
                )
            elif obj_type == "index" and table_name is not None:
                parent_identity = table_identities.get(table_name.lower())
                if parent_identity is not None:
                    edges.append(
                        DependencyEdge(
                            identity,
                            parent_identity,
                            DependencyEdgeKind.INDEX_EXPRESSION,
                            source=MetadataSource.CATALOG,
                        )
                    )
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
