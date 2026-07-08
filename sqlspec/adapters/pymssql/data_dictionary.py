"""pymssql data dictionary."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataSupport,
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    VersionInfo,
    ensure_system_metadata_request,
    get_data_dictionary_loader,
    get_dialect_config,
    system_metadata_gated_result,
)
from sqlspec.data_dictionary.dialects.mssql import (
    build_mssql_metadata_capability_profile,
    build_mssql_system_metadata_capability,
    build_mssql_system_metadata_result,
    build_mssql_table_ddl_result,
    extract_mssql_version_value,
    get_mssql_data_dictionary_options,
    is_mssql_azure_sql,
    list_mssql_available_features,
    merge_mssql_table_lists,
    mssql_supports_native_json,
    parse_mssql_engine_edition,
    parse_mssql_version_components,
    resolve_mssql_feature_flag,
    validate_mssql_system_metadata_options,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.pymssql.driver import PymssqlDriver
    from sqlspec.core import SQL
    from sqlspec.data_dictionary._types import DialectConfig, MetadataCapabilityProfile

__all__ = ("MssqlVersionInfo", "PymssqlSyncDataDictionary")

logger = get_logger("sqlspec.adapters.pymssql.data_dictionary")


class MssqlVersionInfo(VersionInfo):
    """MSSQL database version info with build, revision, and Azure SQL detection."""

    def __init__(
        self,
        major: int,
        minor: int = 0,
        build: int = 0,
        revision: int = 0,
        edition: str | None = None,
        engine_edition: int | None = None,
    ) -> None:
        super().__init__(major, minor, 0)
        self.build = build
        self.revision = revision
        self.edition = edition
        self.engine_edition = engine_edition
        self.is_azure_sql = is_mssql_azure_sql(engine_edition)

    def supports_native_json(self) -> bool:
        """Return whether this server supports the native JSON type."""
        return mssql_supports_native_json(self.major, is_azure_sql=self.is_azure_sql)

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version tuple using the MSSQL build number as the third component."""
        return (self.major, self.minor, self.build)

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.build}.{self.revision}"
        if self.edition:
            version_str += f" ({self.edition})"
        if self.is_azure_sql:
            version_str += " [Azure]"
        return version_str


class _MssqlDataDictionaryMixin:
    """Shared helpers for MSSQL data dictionaries."""

    dialect: ClassVar[str] = "mssql"

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def list_available_features(self) -> list[str]:
        """List available feature flags for this dialect."""
        return list_mssql_available_features(self.get_dialect_config())

    def get_domain_query(self, domain: str, name: str) -> "SQL":
        """Return a SQL Server domain query."""
        query = get_data_dictionary_loader().get_domain_query(type(self).dialect, domain, name)
        return cast("SQL", query.sql)

    def _build_version_info(
        self, version_value: str | None, edition: str | None, engine_edition_value: Any
    ) -> MssqlVersionInfo | None:
        if not version_value:
            return None
        major, minor, build, revision = parse_mssql_version_components(version_value)
        return MssqlVersionInfo(
            major,
            minor,
            build,
            revision,
            edition=edition,
            engine_edition=parse_mssql_engine_edition(engine_edition_value),
        )

    def _get_optimal_type_from_version(self, version_info: MssqlVersionInfo | None, type_category: str) -> str:
        if type_category in {"json", "jsonb"} and version_info is not None and version_info.supports_native_json():
            return "JSON"
        return self.get_dialect_config().get_optimal_type(type_category)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PymssqlSyncDataDictionary(_MssqlDataDictionaryMixin, SyncDataDictionaryBase):
    """MSSQL sync data dictionary."""

    dialect: ClassVar[str] = "mssql"

    def __init__(self) -> None:
        super().__init__()

    def get_metadata_capabilities(
        self, driver: "PymssqlDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get SQL Server replacement data-dictionary capability profile."""
        return build_mssql_metadata_capability_profile(type(self).__name__, domains)

    def get_system_metadata_capabilities(
        self, driver: "PymssqlDriver", domains: "Sequence[str] | None" = None
    ) -> tuple[SystemMetadataCapability, ...]:
        """Get SQL Server opt-in system metadata capability disclosures."""
        _ = driver
        requested_domains = ("dmv_exec_requests", "query_store_runtime") if domains is None else tuple(domains)
        return tuple(build_mssql_system_metadata_capability(domain) for domain in requested_domains)

    def get_version(self, driver: "PymssqlDriver") -> MssqlVersionInfo | None:
        """Get SQL Server version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("MssqlVersionInfo | None", self._version_cache.get(driver_id))

        row = driver.select_one_or_none(self.get_query_text("version"))
        if not row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = extract_mssql_version_value(
            _row_value(row, "product_version") or _row_value(row, "version_string", "version")
        )
        edition_value = _row_value(row, "edition")
        edition = str(edition_value) if edition_value is not None else None
        version_info = self._build_version_info(version_value, edition, _row_value(row, "engine_edition"))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "PymssqlDriver", feature: str) -> bool:
        """Check whether SQL Server supports a feature."""
        version_info = self.get_version(driver)
        return resolve_mssql_feature_flag(
            feature,
            major=version_info.major if version_info is not None else 0,
            is_azure_sql=bool(version_info and version_info.is_azure_sql),
            config=self.get_dialect_config(),
            version_info=version_info,
        )

    def get_optimal_type(self, driver: "PymssqlDriver", type_category: str) -> str:
        """Get optimal SQL Server type for a category."""
        return self._get_optimal_type_from_version(self.get_version(driver), type_category)

    def get_tables(self, driver: "PymssqlDriver", schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_domain_query("tables", "by_schema"), schema_name=schema_name, schema_type=TableMetadata
            ),
        )
        all_rows = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_domain_query("tables", "all_by_schema"), schema_name=schema_name, schema_type=TableMetadata
            ),
        )
        return merge_mssql_table_lists(ordered, all_rows)

    def get_columns(
        self, driver: "PymssqlDriver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                driver.select(
                    self.get_domain_query("columns", "by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            driver.select(
                self.get_domain_query("columns", "by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            ),
        )

    def get_indexes(
        self, driver: "PymssqlDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return cast(
                "list[IndexMetadata]",
                driver.select(
                    self.get_domain_query("indexes", "by_schema"), schema_name=schema_name, schema_type=IndexMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return cast(
            "list[IndexMetadata]",
            driver.select(
                self.get_domain_query("indexes", "by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=IndexMetadata,
            ),
        )

    def get_foreign_keys(
        self, driver: "PymssqlDriver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return cast(
                "list[ForeignKeyMetadata]",
                driver.select(
                    self.get_domain_query("constraints", "foreign_keys_by_schema"),
                    schema_name=schema_name,
                    schema_type=ForeignKeyMetadata,
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return cast(
            "list[ForeignKeyMetadata]",
            driver.select(
                self.get_domain_query("constraints", "foreign_keys_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            ),
        )

    def get_ddl(
        self,
        driver: "PymssqlDriver",
        object_name: str,
        schema: str | None = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Generate SQL Server table DDL from sys catalog rows."""
        _ = include_dependencies, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        columns = driver.select(
            self.get_domain_query("ddl", "table_inputs_by_table"), schema_name=schema_name, table_name=object_name
        )
        indexes = driver.select(
            self.get_domain_query("ddl", "index_inputs_by_table"), schema_name=schema_name, table_name=object_name
        )
        return build_mssql_table_ddl_result(schema_name, object_name, columns, indexes, object_type=object_type)

    def get_system_metadata(
        self, driver: "PymssqlDriver", request: SystemMetadataRequest | str | None = None, **kwargs: Any
    ) -> SystemMetadataResult:
        """Get opt-in SQL Server system metadata with sensitive columns redacted by default."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = build_mssql_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        options = get_mssql_data_dictionary_options(driver)
        denied = validate_mssql_system_metadata_options(metadata_request, options)
        if denied is not None:
            return denied
        query = self.get_domain_query("system", metadata_request.domain)
        rows = driver.select(query)
        return build_mssql_system_metadata_result(metadata_request, rows)


def _row_value(row: object, *names: str) -> Any:
    """Return the first named value from a row-like object."""
    if isinstance(row, dict):
        for name in names:
            if name in row:
                return row[name]
            upper_name = name.upper()
            if upper_name in row:
                return row[upper_name]
        return None
    return getattr(row, names[0], None) if names else None
