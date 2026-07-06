"""BigQuery-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.data_dictionary.dialects.bigquery import (
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Sequence

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

    @property
    def schema_name(self) -> str | None:
        """Return the dataset name used by schema bind parameters."""
        return self.dataset

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
        scope = BigQueryMetadataScope.from_schema(schema)
        self._log_schema_introspect(driver, schema_name=scope.schema_name, table_name=None, operation="tables")
        tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables(schema)

        query_text = self.get_query_text("tables_by_schema").format(
            tables_table=tables_table, kcu_table=kcu_table, rc_table=rc_table
        )
        return driver.select(query_text, schema_name=scope.schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        scope = BigQueryMetadataScope.from_schema(schema)
        schema_prefix = format_bigquery_schema_prefix(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=scope.schema_name, table_name=None, operation="columns")
            query_text = self.get_query_text("columns_by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_name=scope.schema_name, schema_type=ColumnMetadata)

        self._log_table_describe(driver, schema_name=scope.schema_name, table_name=table, operation="columns")
        query_text = self.get_query_text("columns_by_table").format(schema_prefix=schema_prefix)
        return driver.select(query_text, table_name=table, schema_name=scope.schema_name, schema_type=ColumnMetadata)

    def get_indexes(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        scope = BigQueryMetadataScope.from_schema(schema)
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
        scope = BigQueryMetadataScope.from_schema(schema)
        if table is None:
            self._log_schema_introspect(
                driver, schema_name=scope.schema_name, table_name=None, operation="foreign_keys"
            )
        else:
            self._log_table_describe(driver, schema_name=scope.schema_name, table_name=table, operation="foreign_keys")
        kcu_table = scope.dataset_information_schema_table("KEY_COLUMN_USAGE")
        rc_table = scope.dataset_information_schema_table("REFERENTIAL_CONSTRAINTS")

        if table is None:
            query_text = self.get_query_text("foreign_keys_by_schema").format(kcu_table=kcu_table, rc_table=rc_table)
            return driver.select(query_text, schema_name=scope.schema_name, schema_type=ForeignKeyMetadata)

        query_text = self.get_query_text("foreign_keys_by_table").format(kcu_table=kcu_table, rc_table=rc_table)
        return driver.select(
            query_text, table_name=table, schema_name=scope.schema_name, schema_type=ForeignKeyMetadata
        )

    def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get BigQuery replacement data-dictionary capability profile."""
        _ = driver
        requested_domains = tuple(domains) if domains is not None else _DEFAULT_METADATA_DOMAINS
        capabilities = tuple(_bigquery_capability_for_domain(domain) for domain in requested_domains)
        return MetadataCapabilityProfile(self.dialect, adapter=type(self).__name__, capabilities=capabilities)


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
