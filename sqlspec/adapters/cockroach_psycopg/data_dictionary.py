"""CockroachDB-specific data dictionary for metadata queries."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.data_dictionary.dialects.cockroachdb import resolve_cockroachdb_json_type
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_psycopg.driver import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
    from sqlspec.core import SQL

__all__ = ("CockroachPsycopgAsyncDataDictionary", "CockroachPsycopgSyncDataDictionary")


_COCKROACH_METADATA_DOMAINS = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "sequences",
    "comments",
    "privileges",
    "dependencies",
    "ddl",
    "crdb_internal",
    "system",
)
_COCKROACH_SUPPORTED_DOMAINS = frozenset(_COCKROACH_METADATA_DOMAINS) - {"crdb_internal", "system"}


def _cockroach_metadata_capability(domain: str) -> MetadataCapability:
    if domain == "ddl":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.LOSSY,
            source=MetadataSource.INFORMATION_SCHEMA,
            warnings=(
                "CockroachDB DDL metadata is lossy unless SHOW-derived SQL is requested with quoted identifiers.",
            ),
        )
    if domain == "crdb_internal":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.VERSION_GATED, MetadataRisk.PRIVILEGED),
            warnings=("crdb_internal metadata is disabled by default.",),
        )
    if domain == "system":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.EXPENSIVE, MetadataRisk.PRIVILEGED),
            warnings=("System metadata is opt-in and disabled by default.",),
        )
    if domain in _COCKROACH_SUPPORTED_DOMAINS:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.INFORMATION_SCHEMA,
        )
    return MetadataCapability.unsupported(domain)


def _cockroach_metadata_profile(adapter: str, domains: Sequence[str] | None) -> MetadataCapabilityProfile:
    requested_domains = _COCKROACH_METADATA_DOMAINS if domains is None else tuple(domains)
    return MetadataCapabilityProfile(
        "cockroachdb",
        adapter=adapter,
        capabilities=tuple(_cockroach_metadata_capability(domain) for domain in requested_domains),
    )


def _metadata_result(
    domain: str, capability: MetadataCapability, rows: list[Any] | tuple[Any, ...] = ()
) -> MetadataResult:
    return MetadataResult(domain, capability=capability, items=tuple(rows), warnings=capability.warnings)


def _cockroach_domain_sql(domain: str, query_name: str) -> "SQL":
    query = get_data_dictionary_loader().get_domain_query("cockroachdb", domain, query_name)
    if query.sql is None:
        msg = f"Missing CockroachDB data-dictionary query: {domain}/{query_name}"
        raise RuntimeError(msg)
    return query.sql


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachPsycopgSyncDataDictionary(SyncDataDictionaryBase):
    """CockroachDB sync data dictionary."""

    dialect: ClassVar[str] = "cockroachdb"

    def __init__(self) -> None:
        super().__init__()

    def get_metadata_capabilities(
        self, driver: "CockroachPsycopgSyncDriver", domains: Sequence[str] | None = None
    ) -> MetadataCapabilityProfile:
        """Get CockroachDB replacement data-dictionary capability profile."""
        return _cockroach_metadata_profile(type(self).__name__, domains)

    def _select_domain(
        self, driver: "CockroachPsycopgSyncDriver", domain: str, query_name: str, **parameters: Any
    ) -> MetadataResult:
        query = get_data_dictionary_loader().get_domain_query(type(self).dialect, domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult(domain, capability=query.capability, warnings=query.warnings)
        rows = driver.select(query.sql, **parameters)
        return _metadata_result(domain, _cockroach_metadata_capability(domain), rows)

    def get_schemas(self, driver: "CockroachPsycopgSyncDriver") -> MetadataResult:
        """Get schema metadata."""
        return self._select_domain(driver, "schemas", "list")

    def get_objects(self, driver: "CockroachPsycopgSyncDriver", schema: str | None = None) -> MetadataResult:
        """Get database object metadata."""
        return self._select_domain(driver, "objects", "by_schema", schema_name=self.resolve_schema(schema))

    def get_table_details(
        self, driver: "CockroachPsycopgSyncDriver", table: str, schema: str | None = None
    ) -> MetadataResult:
        """Get rich table metadata."""
        return self._select_domain(
            driver,
            "tables",
            "by_schema",
            schema_name=self.resolve_schema(schema),
            table_name=self.resolve_identifier(table),
        )

    def get_constraints(
        self, driver: "CockroachPsycopgSyncDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get constraint metadata."""
        table_name = self.resolve_identifier(table) if table is not None else None
        return self._select_domain(
            driver, "constraints", "by_schema", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    def get_views(self, driver: "CockroachPsycopgSyncDriver", schema: str | None = None) -> MetadataResult:
        """Get view metadata."""
        return self._select_domain(driver, "views", "by_schema", schema_name=self.resolve_schema(schema))

    def get_routines(self, driver: "CockroachPsycopgSyncDriver", schema: str | None = None) -> MetadataResult:
        """Get routine metadata."""
        return _metadata_result("routines", MetadataCapability.unsupported("routines"))

    def get_privileges(
        self, driver: "CockroachPsycopgSyncDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get privilege metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return self._select_domain(
            driver, "privileges", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    def get_dependencies(
        self, driver: "CockroachPsycopgSyncDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get stable dependency metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return self._select_domain(
            driver, "dependencies", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    def get_ddl(
        self,
        driver: "CockroachPsycopgSyncDriver",
        object_name: str,
        schema: str | None = None,
        object_type: str | None = None,
    ) -> MetadataResult:
        """Get lossy CockroachDB DDL status without parameterized identifiers."""
        return self._select_domain(
            driver,
            "ddl",
            "by_object",
            schema_name=self.resolve_schema(schema),
            object_name=self.resolve_identifier(object_name),
            object_type=object_type,
        )

    def get_system_metadata(
        self, driver: "CockroachPsycopgSyncDriver", domain: str, *, include_sensitive: bool = False
    ) -> MetadataResult:
        """Get opt-in CockroachDB system metadata."""
        return _metadata_result("system", _cockroach_metadata_capability("system"))

    def get_version(self, driver: "CockroachPsycopgSyncDriver") -> "VersionInfo | None":
        """Get CockroachDB version information."""
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

    def get_feature_flag(self, driver: "CockroachPsycopgSyncDriver", feature: str) -> bool:
        """Check if CockroachDB supports a specific feature."""
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "CockroachPsycopgSyncDriver", type_category: str) -> str:
        """Get optimal CockroachDB type for a category."""
        config = self.get_dialect_config()
        if type_category == "json":
            return resolve_cockroachdb_json_type(self.get_version(driver))
        return config.get_optimal_type(type_category)

    def get_tables(self, driver: "CockroachPsycopgSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by dependency order."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return driver.select(
            _cockroach_domain_sql("tables", "by_schema"),
            schema_name=schema_name,
            table_name=None,
            schema_type=TableMetadata,
        )

    def get_columns(
        self, driver: "CockroachPsycopgSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                _cockroach_domain_sql("columns", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=ColumnMetadata,
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
        return driver.select(
            _cockroach_domain_sql("columns", "by_schema"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ColumnMetadata,
        )

    def get_indexes(
        self, driver: "CockroachPsycopgSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                _cockroach_domain_sql("indexes", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=IndexMetadata,
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
        return driver.select(
            _cockroach_domain_sql("indexes", "by_schema"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=IndexMetadata,
        )

    def get_foreign_keys(
        self, driver: "CockroachPsycopgSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table_name,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachPsycopgAsyncDataDictionary(AsyncDataDictionaryBase):
    """CockroachDB async data dictionary."""

    dialect: ClassVar[str] = "cockroachdb"

    def __init__(self) -> None:
        super().__init__()

    async def get_metadata_capabilities(
        self, driver: "CockroachPsycopgAsyncDriver", domains: Sequence[str] | None = None
    ) -> MetadataCapabilityProfile:
        """Get CockroachDB replacement data-dictionary capability profile."""
        return _cockroach_metadata_profile(type(self).__name__, domains)

    async def _select_domain(
        self, driver: "CockroachPsycopgAsyncDriver", domain: str, query_name: str, **parameters: Any
    ) -> MetadataResult:
        query = get_data_dictionary_loader().get_domain_query(type(self).dialect, domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult(domain, capability=query.capability, warnings=query.warnings)
        rows = await driver.select(query.sql, **parameters)
        return _metadata_result(domain, _cockroach_metadata_capability(domain), rows)

    async def get_schemas(self, driver: "CockroachPsycopgAsyncDriver") -> MetadataResult:
        """Get schema metadata."""
        return await self._select_domain(driver, "schemas", "list")

    async def get_objects(self, driver: "CockroachPsycopgAsyncDriver", schema: str | None = None) -> MetadataResult:
        """Get database object metadata."""
        return await self._select_domain(driver, "objects", "by_schema", schema_name=self.resolve_schema(schema))

    async def get_table_details(
        self, driver: "CockroachPsycopgAsyncDriver", table: str, schema: str | None = None
    ) -> MetadataResult:
        """Get rich table metadata."""
        return await self._select_domain(
            driver,
            "tables",
            "by_schema",
            schema_name=self.resolve_schema(schema),
            table_name=self.resolve_identifier(table),
        )

    async def get_constraints(
        self, driver: "CockroachPsycopgAsyncDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get constraint metadata."""
        table_name = self.resolve_identifier(table) if table is not None else None
        return await self._select_domain(
            driver, "constraints", "by_schema", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    async def get_views(self, driver: "CockroachPsycopgAsyncDriver", schema: str | None = None) -> MetadataResult:
        """Get view metadata."""
        return await self._select_domain(driver, "views", "by_schema", schema_name=self.resolve_schema(schema))

    async def get_routines(self, driver: "CockroachPsycopgAsyncDriver", schema: str | None = None) -> MetadataResult:
        """Get routine metadata."""
        return _metadata_result("routines", MetadataCapability.unsupported("routines"))

    async def get_privileges(
        self, driver: "CockroachPsycopgAsyncDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get privilege metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return await self._select_domain(
            driver, "privileges", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    async def get_dependencies(
        self, driver: "CockroachPsycopgAsyncDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get stable dependency metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return await self._select_domain(
            driver, "dependencies", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    async def get_ddl(
        self,
        driver: "CockroachPsycopgAsyncDriver",
        object_name: str,
        schema: str | None = None,
        object_type: str | None = None,
    ) -> MetadataResult:
        """Get lossy CockroachDB DDL status without parameterized identifiers."""
        return await self._select_domain(
            driver,
            "ddl",
            "by_object",
            schema_name=self.resolve_schema(schema),
            object_name=self.resolve_identifier(object_name),
            object_type=object_type,
        )

    async def get_system_metadata(
        self, driver: "CockroachPsycopgAsyncDriver", domain: str, *, include_sensitive: bool = False
    ) -> MetadataResult:
        """Get opt-in CockroachDB system metadata."""
        return _metadata_result("system", _cockroach_metadata_capability("system"))

    async def get_version(self, driver: "CockroachPsycopgAsyncDriver") -> "VersionInfo | None":
        """Get CockroachDB version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version"))
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

    async def get_feature_flag(self, driver: "CockroachPsycopgAsyncDriver", feature: str) -> bool:
        """Check if CockroachDB supports a specific feature."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "CockroachPsycopgAsyncDriver", type_category: str) -> str:
        """Get optimal CockroachDB type for a category."""
        config = self.get_dialect_config()
        if type_category == "json":
            return resolve_cockroachdb_json_type(await self.get_version(driver))
        return config.get_optimal_type(type_category)

    async def get_tables(
        self, driver: "CockroachPsycopgAsyncDriver", schema: "str | None" = None
    ) -> "list[TableMetadata]":
        """Get tables sorted by dependency order."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            _cockroach_domain_sql("tables", "by_schema"),
            schema_name=schema_name,
            table_name=None,
            schema_type=TableMetadata,
        )

    async def get_columns(
        self, driver: "CockroachPsycopgAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                _cockroach_domain_sql("columns", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=ColumnMetadata,
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
        return await driver.select(
            _cockroach_domain_sql("columns", "by_schema"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ColumnMetadata,
        )

    async def get_indexes(
        self, driver: "CockroachPsycopgAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                _cockroach_domain_sql("indexes", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=IndexMetadata,
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
        return await driver.select(
            _cockroach_domain_sql("indexes", "by_schema"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=IndexMetadata,
        )

    async def get_foreign_keys(
        self, driver: "CockroachPsycopgAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table_name,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )
