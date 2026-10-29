"""DuckDB-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    DDL_PARENT_TABLE_CONTEXT_KEY,
    ColumnMetadata,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    DependencyMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Mapping

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
        """Get foreign key metadata, including ON DELETE / ON UPDATE actions."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = driver.select(self.get_query("foreign_keys", "by_table"), table_name=table, schema_name=schema_name)
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    def get_constraints(
        self, driver: "DuckDBDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get PRIMARY KEY, UNIQUE, and CHECK constraint metadata from duckdb_constraints().

        Foreign keys are reported by :meth:`get_foreign_keys`; NOT NULL is reported on columns.
        """
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = driver.select(self._get_domain_query_text("constraints", "by_schema"), schema_name=schema_name)
        else:
            rows = driver.select(
                self._get_domain_query_text("constraints", "by_table"), schema_name=schema_name, table_name=table
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            constraint_type = str(row.get("constraint_type") or "").upper()
            if constraint_type not in _DUCKDB_TABLE_CONSTRAINT_TYPES:
                continue
            item: dict[str, Any] = {
                "schema_name": row.get("schema_name"),
                "table_name": row.get("table_name"),
                "constraint_name": row.get("constraint_name"),
                "constraint_type": constraint_type,
                "columns": [str(column) for column in row.get("constraint_column_names") or ()],
            }
            if constraint_type == "CHECK":
                item["check_clause"] = row.get("expression") or row.get("constraint_text")
            items.append(item)
        return MetadataResult("constraints", items=tuple(items))

    def get_views(self, driver: "DuckDBDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata including the catalog CREATE VIEW statement."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("views", "by_schema"), schema_name=schema_name)
        return MetadataResult("views", items=tuple(rows))

    def get_sequences(self, driver: "DuckDBDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get sequence metadata from duckdb_sequences()."""
        schema_name = self.resolve_schema(schema)
        rows = driver.select(self._get_domain_query_text("sequences", "by_schema"), schema_name=schema_name)
        return MetadataResult("sequences", items=tuple(rows))

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

    def _get_native_object_ddl(
        self,
        driver: "DuckDBDriver",
        object_name: str,
        *,
        schema: "str | None",
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> "DDLResult | None":
        """Return catalog DDL for one object, reporting tables with foreign keys as not faithful."""
        _ = include_dependencies, redact
        rows = driver.select(
            self._get_domain_query_text("ddl", "by_object"),
            schema_name=schema,
            object_name=object_name,
            object_type=object_type,
        )
        if not rows:
            return DDLResult.unsupported(
                ObjectIdentity(object_name, object_type, schema=schema, dialect=type(self).dialect),
                source=MetadataSource.CATALOG,
            )
        return self._ddl_result_from_row(rows[0])

    def _get_native_schema_ddl(
        self, driver: "DuckDBDriver", schema: "str | None", *, include_dependencies: bool, redact: bool
    ) -> "list[DDLResult] | None":
        """Return catalog DDL for every schema object with dependency edges."""
        _ = include_dependencies, redact
        rows = driver.select(self._get_domain_query_text("ddl", "by_schema"), schema_name=self.resolve_schema(schema))
        results = [self._ddl_result_from_row(row) for row in rows]
        raw_sql = {
            (str(row.get("object_type") or "").lower(), str(row.get("object_name") or "")): str(row.get("ddl") or "")
            for row in rows
        }
        relations = {
            item.identity.name.lower(): item.identity
            for item in results
            if item.identity.object_type in {"table", "temporary_table", "view"}
        }
        referenced = {
            item.identity.name.lower(): item.identity
            for item in results
            if item.identity.object_type in {"sequence", "type"}
        }
        linked: list[DDLResult] = []
        for item in results:
            edges: list[DependencyEdge] = []
            ddl_text = raw_sql.get((item.identity.object_type, item.identity.name), "")
            object_type = item.identity.object_type
            parent = item.context.get(DDL_PARENT_TABLE_CONTEXT_KEY)
            if object_type == "index" and parent is not None:
                parent_identity = relations.get(str(parent).lower())
                if parent_identity is not None:
                    edges.append(
                        DependencyEdge(
                            item.identity,
                            parent_identity,
                            DependencyEdgeKind.INDEX_EXPRESSION,
                            source=MetadataSource.CATALOG,
                        )
                    )
            elif object_type in {"table", "temporary_table", "view"} and ddl_text:
                candidates = referenced if object_type != "view" else {**relations, **referenced}
                edges.extend(
                    DependencyEdge(
                        item.identity,
                        ref_identity,
                        _DUCKDB_REFERENCE_EDGE_KINDS[ref_identity.object_type],
                        source=MetadataSource.CATALOG,
                    )
                    for ref_key, ref_identity in candidates.items()
                    if ref_key != item.identity.name.lower() and _references_identifier(ddl_text, ref_identity.name)
                )
            linked.append(
                DDLResult(
                    identity=item.identity,
                    status=item.status,
                    fidelity=item.fidelity,
                    source=item.source,
                    ddl=item.ddl,
                    dependencies=tuple(edges),
                    context=item.context,
                    warnings=item.warnings,
                )
            )
        return linked

    def _ddl_result_from_row(self, row: "Mapping[str, Any]") -> DDLResult:
        """Build a DDLResult from a ``ddl.sql`` row.

        DuckDB's stored table SQL drops self-referencing foreign keys and leaves referenced table
        names unquoted, so tables with foreign keys are reported as unsupported; the base class
        then synthesizes them from catalog metadata.
        """
        object_type = str(row.get("object_type") or "table").lower()
        row_schema = row.get("schema_name")
        identity = ObjectIdentity(
            name=str(row.get("object_name") or ""),
            object_type=object_type,
            schema=str(row_schema) if row_schema is not None else None,
            dialect=type(self).dialect,
            source=MetadataSource.CATALOG,
        )
        parent = row.get("table_name")
        context: dict[str, object] = (
            {DDL_PARENT_TABLE_CONTEXT_KEY: str(parent)} if object_type == "index" and parent is not None else {}
        )
        ddl_value = row.get("ddl")
        if ddl_value is None:
            return DDLResult.unsupported(
                identity,
                source=MetadataSource.CATALOG,
                context=context,
                warnings=(f"DuckDB does not expose reconstructable DDL for {object_type} {identity.name}",),
            )
        if object_type in {"table", "temporary_table"} and row.get("has_foreign_keys"):
            return DDLResult.unsupported(
                identity, source=MetadataSource.CATALOG, warnings=(_DUCKDB_FOREIGN_KEY_WARNING,)
            )
        return DDLResult(
            identity=identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl=str(ddl_value),
            context=context,
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


_DUCKDB_TABLE_CONSTRAINT_TYPES: Final = frozenset({"PRIMARY KEY", "UNIQUE", "CHECK"})
_DUCKDB_FOREIGN_KEY_WARNING: Final = (
    "DuckDB catalog SQL omits self-referencing foreign keys and leaves referenced table names unquoted; "
    "table DDL was synthesized from catalog metadata"
)
_DUCKDB_REFERENCE_EDGE_KINDS: Final = {
    "table": DependencyEdgeKind.VIEW_REFERENCE,
    "temporary_table": DependencyEdgeKind.VIEW_REFERENCE,
    "view": DependencyEdgeKind.VIEW_REFERENCE,
    "sequence": DependencyEdgeKind.SEQUENCE_USE,
    "type": DependencyEdgeKind.DEFAULT_EXPRESSION,
}


def _references_identifier(sql_text: str, name: str) -> bool:
    """Return True when ``name`` appears in ``sql_text`` as a whole identifier."""
    return re.search(rf"(?<![\w$]){re.escape(name)}(?![\w$])", sql_text, re.IGNORECASE) is not None
