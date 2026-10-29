"""BigQuery-specific data dictionary for metadata queries."""

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    sort_ddl_results,
)
from sqlspec.data_dictionary.dialects.bigquery import (
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.driver import BigQueryDriver

__all__ = ("BigQueryDataDictionary", "BigQueryMetadataScope")

_DEFAULT_METADATA_DOMAINS = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "privileges",
    "dependencies",
    "ddl",
    "system",
)

_BIGQUERY_INFORMATION_SCHEMA_WARNINGS = (
    "BigQuery INFORMATION_SCHEMA queries require a dataset or region qualifier.",
    "BigQuery INFORMATION_SCHEMA query results are not cached and may be billed.",
    "Region-qualified INFORMATION_SCHEMA views cannot be joined across regions.",
)

_BIGQUERY_PRIVILEGE_WARNINGS = ("BigQuery OBJECT_PRIVILEGES omits inherited IAM and routine-level IAM grants.",)

_BIGQUERY_SYSTEM_WARNINGS = (
    "BigQuery system views are region scoped and require the query job location to match that region.",
    "BigQuery system views are not cached, may be billed, and require elevated IAM permissions.",
)

_BIGQUERY_PROJECT_DATASET_PART_COUNT = 2


class BigQueryMetadataScope:
    """Structured BigQuery metadata qualifiers."""

    __slots__ = ("dataset", "project", "region")

    def __init__(self, *, project: str | None = None, dataset: str | None = None, region: str | None = None) -> None:
        self.project = _clean_bigquery_identifier_part(project)
        self.dataset = _clean_bigquery_identifier_part(dataset)
        self.region = _clean_bigquery_identifier_part(region)

    @property
    def schema_name(self) -> str | None:
        """Return the dataset name used by schema bind parameters."""
        return self.dataset

    @classmethod
    def from_schema(
        cls, schema: str | None, *, project: str | None = None, dataset: str | None = None, region: str | None = None
    ) -> "BigQueryMetadataScope":
        """Create a metadata scope from a legacy schema value and explicit qualifiers.

        Args:
            schema: Optional dataset or ``project.dataset`` value.
            project: Explicit project qualifier.
            dataset: Explicit dataset qualifier.
            region: Optional region or ``region-REGION`` qualifier.

        Returns:
            Parsed BigQuery metadata scope.
        """
        schema_project: str | None = None
        schema_dataset: str | None = None
        if schema:
            cleaned_schema = _clean_bigquery_identifier_part(schema)
            parts = cleaned_schema.split(".") if cleaned_schema is not None else []
            if len(parts) == 1:
                schema_dataset = parts[0]
            elif len(parts) == _BIGQUERY_PROJECT_DATASET_PART_COUNT:
                schema_project, schema_dataset = parts
            else:
                msg = "BigQuery schema must be a dataset or project.dataset qualifier."
                raise ValueError(msg)
        return cls(project=project or schema_project, dataset=dataset or schema_dataset, region=region)

    def dataset_prefix(self) -> str:
        """Return a quoted dataset prefix for legacy INFORMATION_SCHEMA SQL."""
        if self.dataset is None:
            return ""
        if self.project is None:
            return f"{_quote_bigquery_path(self.dataset)}."
        return f"{_quote_bigquery_path(self.project, self.dataset)}."

    def dataset_information_schema_table(self, view_name: str) -> str:
        """Return a dataset-scoped INFORMATION_SCHEMA table identifier."""
        view = _clean_bigquery_identifier_part(view_name)
        if self.dataset is None:
            return f"INFORMATION_SCHEMA.{view}"
        if self.project is None:
            return _quote_bigquery_path(self.dataset, "INFORMATION_SCHEMA", view)
        return _quote_bigquery_path(self.project, self.dataset, "INFORMATION_SCHEMA", view)

    def region_information_schema_table(self, view_name: str) -> str:
        """Return a region-scoped INFORMATION_SCHEMA table identifier."""
        if self.region is None:
            msg = "BigQuery region-scoped metadata requires a region qualifier."
            raise ValueError(msg)
        view = _clean_bigquery_identifier_part(view_name)
        region = _normalize_bigquery_region(self.region)
        if self.project is None:
            return _quote_bigquery_path(region, "INFORMATION_SCHEMA", view)
        return _quote_bigquery_path(self.project, region, "INFORMATION_SCHEMA", view)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class BigQueryDataDictionary(SyncDataDictionaryBase):
    """BigQuery-specific sync data dictionary."""

    dialect: ClassVar[str] = "bigquery"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "BigQueryDriver") -> "VersionInfo | None":
        """Return BigQuery version information.

        Args:
            driver: BigQuery driver instance.

        Returns:
            None because BigQuery does not expose version info.
        """
        _ = driver
        return None

    def get_feature_flag(self, driver: "BigQueryDriver", feature: str) -> bool:
        """Check if BigQuery supports a specific feature.

        Args:
            driver: BigQuery driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.
        """
        _ = driver
        return self.resolve_feature_flag(feature, None)

    def get_optimal_type(self, driver: "BigQueryDriver", type_category: str) -> str:
        """Get optimal BigQuery type for a category.

        Args:
            driver: BigQuery driver instance.
            type_category: Type category.

        Returns:
            BigQuery-specific type name.
        """
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "BigQueryDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using BigQuery catalog."""
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        self._log_schema_introspect(driver, schema_name=scope.schema_name, table_name=None, operation="tables")
        tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables(resolved_schema)

        query_text = self.get_query_text("tables", "by_schema").format(
            tables_table=tables_table, kcu_table=kcu_table, rc_table=rc_table
        )
        return driver.select(query_text, schema_name=scope.schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        schema_prefix = format_bigquery_schema_prefix(resolved_schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=scope.schema_name, table_name=None, operation="columns")
            query_text = self.get_query_text("columns", "by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_name=scope.schema_name, schema_type=ColumnMetadata)

        self._log_table_describe(driver, schema_name=scope.schema_name, table_name=table, operation="columns")
        query_text = self.get_query_text("columns", "by_table").format(schema_prefix=schema_prefix)
        return driver.select(query_text, table_name=table, schema_name=scope.schema_name, schema_type=ColumnMetadata)

    def get_indexes(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        if scope.dataset is None:
            return []
        if table is None:
            self._log_schema_introspect(driver, schema_name=scope.schema_name, table_name=None, operation="indexes")
        else:
            self._log_table_describe(driver, schema_name=scope.schema_name, table_name=table, operation="indexes")

        search_query = _get_domain_query_text("indexes", "search_by_dataset").format(
            search_indexes_table=scope.dataset_information_schema_table("SEARCH_INDEXES"),
            search_index_columns_table=scope.dataset_information_schema_table("SEARCH_INDEX_COLUMNS"),
        )
        vector_query = _get_domain_query_text("indexes", "vector_by_dataset").format(
            vector_indexes_table=scope.dataset_information_schema_table("VECTOR_INDEXES"),
            vector_index_columns_table=scope.dataset_information_schema_table("VECTOR_INDEX_COLUMNS"),
        )
        search_indexes = driver.select(
            search_query, table_name=table, schema_name=scope.schema_name, schema_type=IndexMetadata
        )
        vector_indexes = driver.select(
            vector_query, table_name=table, schema_name=scope.schema_name, schema_type=IndexMetadata
        )
        return [*search_indexes, *vector_indexes]

    def get_foreign_keys(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        if table is None:
            self._log_schema_introspect(
                driver, schema_name=scope.schema_name, table_name=None, operation="foreign_keys"
            )
        else:
            self._log_table_describe(driver, schema_name=scope.schema_name, table_name=table, operation="foreign_keys")
        kcu_table = scope.dataset_information_schema_table("KEY_COLUMN_USAGE")
        rc_table = scope.dataset_information_schema_table("REFERENTIAL_CONSTRAINTS")

        if table is None:
            query_text = self.get_query_text("foreign_keys", "by_schema").format(kcu_table=kcu_table, rc_table=rc_table)
            return driver.select(query_text, schema_name=scope.schema_name, schema_type=ForeignKeyMetadata)

        query_text = self.get_query_text("foreign_keys", "by_table").format(kcu_table=kcu_table, rc_table=rc_table)
        return driver.select(
            query_text, table_name=table, schema_name=scope.schema_name, schema_type=ForeignKeyMetadata
        )

    def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get BigQuery data-dictionary capability profile."""
        _ = driver
        requested_domains = tuple(domains) if domains is not None else _DEFAULT_METADATA_DOMAINS
        capabilities = tuple(_bigquery_capability_for_domain(domain) for domain in requested_domains)
        return MetadataCapabilityProfile(self.dialect, adapter=type(self).__name__, capabilities=capabilities)

    def get_ddl(
        self,
        driver: "BigQueryDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get native BigQuery DDL for a table, view, or routine."""
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
        driver: "BigQueryDriver",
        object_name: str,
        *,
        schema: "str | None" = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Get native BigQuery DDL for a table, view, or routine."""
        _ = include_dependencies, redact
        if object_type.lower() == "table" and (not prefer_native or target_dialect is not None):
            return self._synthesize_table_ddl(driver, object_name, schema=schema, target_dialect=target_dialect)
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        normalized_type = object_type.lower()
        if normalized_type in {"routine", "function", "procedure"}:
            routines_table = scope.dataset_information_schema_table("ROUTINES")
            query_text = _get_domain_query_text("routines", "by_dataset").format(routines_table=routines_table)
            rows = driver.select(query_text, schema_name=scope.schema_name, routine_name=object_name)
        else:
            tables_table = scope.dataset_information_schema_table("TABLES")
            query_text = _get_domain_query_text("ddl", "by_object").format(tables_table=tables_table)
            rows = driver.select(query_text, schema_name=scope.schema_name, table_name=object_name)
        res = _bigquery_ddl_result_from_rows(
            dialect=type(self).dialect,
            object_name=object_name,
            object_type=object_type,
            schema=scope.schema_name,
            rows=rows,
        )
        if target_dialect is not None:
            return self._transpile_ddl_result(res, target_dialect=target_dialect)
        return res

    def get_schema_ddl(
        self,
        driver: "BigQueryDriver",
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
        """Get topologically sorted native BigQuery DDL for all dataset objects."""
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
        resolved_schema = _resolve_bigquery_schema(driver, schema)
        scope = BigQueryMetadataScope.from_schema(resolved_schema)
        schema_prefix = format_bigquery_schema_prefix(resolved_schema)
        query_text = _get_domain_query_text("ddl", "by_schema").format(schema_prefix=schema_prefix)
        rows = driver.select(query_text, schema_name=scope.schema_name)
        foreign_keys = self.get_foreign_keys(driver, schema=schema) if include_dependencies else []
        return _bigquery_schema_ddl_from_rows(
            dialect=type(self).dialect,
            schema=scope.schema_name,
            rows=rows,
            foreign_keys=foreign_keys,
            exclude_tables=exclude_tables,
            direction=direction,
            include_dependencies=include_dependencies,
        )


def _resolve_bigquery_schema(driver: "BigQueryDriver", schema: "str | None") -> "str | None":
    if schema is not None:
        return schema
    default_job_config = getattr(driver, "_default_query_job_config", None)
    if default_job_config is None:
        return None
    default_dataset = getattr(default_job_config, "default_dataset", None)
    if default_dataset is None:
        return None
    if isinstance(default_dataset, str):
        return default_dataset or None
    project = getattr(default_dataset, "project", None)
    dataset_id = getattr(default_dataset, "dataset_id", None)
    if project and dataset_id:
        return f"{project}.{dataset_id}"
    if dataset_id:
        return str(dataset_id)
    return str(default_dataset)


def _clean_bigquery_identifier_part(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if stripped.startswith("`") and stripped.endswith("`"):
        stripped = stripped[1:-1]
    if "`" in stripped:
        msg = "BigQuery metadata identifiers cannot contain backticks."
        raise ValueError(msg)
    return stripped or None


def _normalize_bigquery_region(region: str | None) -> str | None:
    cleaned = _clean_bigquery_identifier_part(region)
    if cleaned is None:
        return None
    if cleaned.startswith("region-"):
        return cleaned
    return f"region-{cleaned}"


def _quote_bigquery_path(*parts: str | None) -> str:
    cleaned_parts = [_clean_bigquery_identifier_part(part) for part in parts]
    present_parts = [part for part in cleaned_parts if part]
    return f"`{'.'.join(present_parts)}`"


def _get_domain_query_text(domain: str, query_name: str) -> str:
    query_text = get_data_dictionary_loader().get_domain_query_text("bigquery", domain, query_name)
    if query_text is None:
        msg = f"Missing BigQuery data-dictionary query {domain}/{query_name}"
        raise RuntimeError(msg)
    return query_text


def _bigquery_capability_for_domain(domain: str) -> "MetadataCapability":
    if domain == "system":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.BILLED, MetadataRisk.EXPENSIVE, MetadataRisk.PRIVILEGED),
            warnings=_BIGQUERY_SYSTEM_WARNINGS,
        )
    if domain == "privileges":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.INFORMATION_SCHEMA,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
            warnings=_BIGQUERY_PRIVILEGE_WARNINGS,
        )
    if domain in _DEFAULT_METADATA_DOMAINS or domain in {"datasets", "nested_columns", "partitions", "storage"}:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.INFORMATION_SCHEMA,
            risks=(MetadataRisk.BILLED,),
            warnings=_BIGQUERY_INFORMATION_SCHEMA_WARNINGS,
        )
    return MetadataCapability.unsupported(domain)


def _bigquery_ddl_result_from_rows(
    *, dialect: str, object_name: str, object_type: str, schema: "str | None", rows: "Sequence[dict[str, Any]]"
) -> DDLResult:
    row = rows[0] if rows else None
    resolved_schema = (
        (row.get("schema_name") or row.get("table_schema") or row.get("routine_schema")) if row is not None else schema
    )
    resolved_catalog = (
        (row.get("catalog_name") or row.get("table_catalog") or row.get("routine_catalog")) if row is not None else None
    )
    resolved_type = str(row.get("object_type") or object_type) if row is not None else object_type
    ddl_value = (row.get("ddl") or row.get("routine_definition")) if row is not None else None
    identity = ObjectIdentity(
        name=object_name,
        object_type=resolved_type,
        catalog=str(resolved_catalog) if resolved_catalog is not None else None,
        schema=str(resolved_schema) if resolved_schema is not None else None,
        dialect=dialect,
        source=MetadataSource.INFORMATION_SCHEMA,
    )
    if ddl_value is None:
        return DDLResult.unsupported(
            identity, source=MetadataSource.INFORMATION_SCHEMA, warnings=_BIGQUERY_INFORMATION_SCHEMA_WARNINGS
        )
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.INFORMATION_SCHEMA,
        ddl=str(ddl_value),
        warnings=_BIGQUERY_INFORMATION_SCHEMA_WARNINGS,
    )


def _bigquery_schema_ddl_from_rows(
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
    filtered_rows: list[tuple[ObjectIdentity, str]] = []
    table_identities: dict[str, ObjectIdentity] = {}
    view_identities: dict[str, ObjectIdentity] = {}

    for row in rows:
        object_name = str(row.get("object_name") or row.get("table_name") or row.get("routine_name") or "")
        raw_type = row.get("object_type") or row.get("table_type") or row.get("routine_type") or "table"
        normalized_type = str(raw_type).lower()
        if normalized_type == "base table":
            normalized_type = "table"
        elif normalized_type == "materialized view":
            normalized_type = "materialized_view"
        ddl_value = row.get("ddl") or row.get("routine_definition")
        if not object_name or ddl_value is None:
            continue
        if object_name.lower() in excluded:
            continue
        row_schema = row.get("schema_name") or row.get("table_schema") or row.get("routine_schema") or schema
        row_catalog = row.get("catalog_name") or row.get("table_catalog") or row.get("routine_catalog")
        identity = ObjectIdentity(
            name=object_name,
            object_type=normalized_type,
            catalog=str(row_catalog) if row_catalog is not None else None,
            schema=str(row_schema) if row_schema is not None else None,
            dialect=dialect,
            source=MetadataSource.INFORMATION_SCHEMA,
        )
        filtered_rows.append((identity, str(ddl_value)))
        if normalized_type == "table":
            table_identities[object_name.lower()] = identity
        elif normalized_type in {"view", "materialized_view"}:
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
    for identity, ddl_text in filtered_rows:
        edges: list[DependencyEdge] = []
        if include_dependencies:
            obj_type = identity.object_type.lower()
            if obj_type == "table":
                edges.extend(
                    DependencyEdge(
                        identity, ref_identity, DependencyEdgeKind.FOREIGN_KEY, source=MetadataSource.INFORMATION_SCHEMA
                    )
                    for ref_identity in fk_targets_by_table.get(identity.name.lower(), ())
                )
            elif obj_type in {"view", "materialized_view"}:
                for ref_key, ref_identity in rel_identities.items():
                    if ref_key == identity.name.lower():
                        continue
                    if re.search(rf"\b{re.escape(ref_identity.name)}\b", ddl_text, re.IGNORECASE):
                        edges.append(
                            DependencyEdge(
                                identity,
                                ref_identity,
                                DependencyEdgeKind.VIEW_REFERENCE,
                                source=MetadataSource.INFORMATION_SCHEMA,
                            )
                        )
        results.append(
            DDLResult(
                identity=identity,
                status=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.NATIVE,
                source=MetadataSource.INFORMATION_SCHEMA,
                ddl=ddl_text,
                dependencies=tuple(edges),
                warnings=_BIGQUERY_INFORMATION_SCHEMA_WARNINGS,
            )
        )

    ordered = (
        sort_ddl_results(results, order=direction, raise_on_cycles=False) if include_dependencies else tuple(results)
    )
    capability = _bigquery_capability_for_domain("ddl")
    return MetadataResult("ddl", capability=capability, items=ordered, warnings=capability.warnings)
