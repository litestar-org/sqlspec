"""CockroachDB AsyncPG data dictionary."""

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
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    VersionInfo,
    ensure_system_metadata_request,
    get_data_dictionary_loader,
    sort_ddl_results,
    system_metadata_gated_result,
    unsupported_system_metadata_capability,
)
from sqlspec.data_dictionary.dialects.cockroachdb import resolve_cockroachdb_json_type
from sqlspec.driver import AsyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver
    from sqlspec.core import SQL

__all__ = ("CockroachAsyncpgDataDictionary",)


_COCKROACH_METADATA_DOMAINS: tuple[str, ...] = (
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


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachAsyncpgDataDictionary(AsyncDataDictionaryBase):
    """CockroachDB async data dictionary (AsyncPG)."""

    dialect: ClassVar[str] = "cockroachdb"

    async def get_metadata_capabilities(
        self, driver: "CockroachAsyncpgDriver", domains: Sequence[str] | None = None
    ) -> MetadataCapabilityProfile:
        """Get CockroachDB data-dictionary capability profile."""
        return _cockroach_metadata_profile(type(self).__name__, domains)

    async def get_system_metadata_capabilities(
        self, driver: "CockroachAsyncpgDriver", domains: Sequence[str] | None = None
    ) -> tuple[SystemMetadataCapability, ...]:
        """Get CockroachDB opt-in system metadata capability disclosures."""
        _ = driver
        requested_domains = ("system",) if domains is None else tuple(domains)
        return tuple(unsupported_system_metadata_capability(domain) for domain in requested_domains)

    async def get_schemas(self, driver: "CockroachAsyncpgDriver") -> MetadataResult:
        """Get schema metadata."""
        return await self._select_domain(driver, "schemas", "list")

    async def get_objects(self, driver: "CockroachAsyncpgDriver", schema: str | None = None) -> MetadataResult:
        """Get database object metadata."""
        return await self._select_domain(driver, "objects", "by_schema", schema_name=self.resolve_schema(schema))

    async def get_table_details(
        self, driver: "CockroachAsyncpgDriver", table: str, schema: str | None = None
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
        self, driver: "CockroachAsyncpgDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get constraint metadata."""
        table_name = self.resolve_identifier(table) if table is not None else None
        return await self._select_domain(
            driver, "constraints", "by_schema", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    async def get_views(self, driver: "CockroachAsyncpgDriver", schema: str | None = None) -> MetadataResult:
        """Get view metadata."""
        return await self._select_domain(driver, "views", "by_schema", schema_name=self.resolve_schema(schema))

    async def get_routines(self, driver: "CockroachAsyncpgDriver", schema: str | None = None) -> MetadataResult:
        """Get routine metadata."""
        return _metadata_result("routines", MetadataCapability.unsupported("routines"))

    async def get_privileges(
        self, driver: "CockroachAsyncpgDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get privilege metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return await self._select_domain(
            driver, "privileges", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    async def get_dependencies(
        self, driver: "CockroachAsyncpgDriver", object_name: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get stable dependency metadata."""
        resolved_object = self.resolve_identifier(object_name) if object_name is not None else None
        return await self._select_domain(
            driver, "dependencies", "by_schema", schema_name=self.resolve_schema(schema), object_name=resolved_object
        )

    async def get_ddl(
        self,
        driver: "CockroachAsyncpgDriver",
        object_name: str,
        schema: str | None = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Get CockroachDB DDL from crdb_internal.tables and pg_catalog helpers."""
        return await self.get_object_ddl(
            driver,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
        )

    async def get_object_ddl(
        self,
        driver: "CockroachAsyncpgDriver",
        object_name: str,
        *,
        schema: str | None = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Get CockroachDB DDL for one object."""
        _ = include_dependencies, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        resolved_object = self.resolve_identifier(object_name)
        result = await self._select_domain(
            driver, "ddl", "by_object", schema_name=schema_name, object_name=resolved_object, object_type=object_type
        )
        return _cockroach_ddl_result_from_rows(
            dialect=type(self).dialect,
            object_name=resolved_object,
            object_type=object_type,
            schema=schema_name,
            rows=result.items,
            warnings=result.warnings,
        )

    async def get_schema_ddl(
        self,
        driver: "CockroachAsyncpgDriver",
        schema: str | None = None,
        *,
        include_domains: Sequence[str] | None = None,
        exclude_domains: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = ("ddl_migrations",),
        direction: Literal["create", "drop"] = "create",
        target_dialect: str | None = None,
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> MetadataResult:
        """Get topologically sorted CockroachDB DDL for all schema objects."""
        _ = include_domains, exclude_domains, target_dialect, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        result = await self._select_domain(driver, "ddl", "by_schema", schema_name=schema_name)
        foreign_keys = await self.get_foreign_keys(driver, schema=schema) if include_dependencies else []
        return _cockroach_schema_ddl_from_rows(
            dialect=type(self).dialect,
            schema=schema_name,
            rows=result.items,
            foreign_keys=foreign_keys,
            exclude_tables=exclude_tables,
            direction=direction,
            include_dependencies=include_dependencies,
        )

    async def get_system_metadata(
        self, driver: "CockroachAsyncpgDriver", request: SystemMetadataRequest | str | None = None, **kwargs: Any
    ) -> SystemMetadataResult:
        """Get opt-in CockroachDB system metadata."""
        _ = driver
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = unsupported_system_metadata_capability(metadata_request.domain)
        return system_metadata_gated_result(metadata_request, capability)

    async def get_version(self, driver: "CockroachAsyncpgDriver") -> "VersionInfo | None":
        driver_id = id(getattr(driver, "connection", None) or driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version", "current"))
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

    async def get_feature_flag(self, driver: "CockroachAsyncpgDriver", feature: str) -> bool:
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "CockroachAsyncpgDriver", type_category: str) -> str:
        config = self.get_dialect_config()
        if type_category == "json":
            return resolve_cockroachdb_json_type(await self.get_version(driver))
        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "CockroachAsyncpgDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            _cockroach_domain_sql("tables", "by_schema"),
            schema_name=schema_name,
            table_name=None,
            schema_type=TableMetadata,
        )

    async def get_columns(
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
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
        table_schema_name = None if schema is None else schema_name
        self._log_table_describe(driver, schema_name=table_schema_name, table_name=table_name, operation="columns")
        return await driver.select(
            _cockroach_domain_sql("columns", "by_schema"),
            schema_name=table_schema_name,
            table_name=table_name,
            schema_type=ColumnMetadata,
        )

    async def get_indexes(
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
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
        self, driver: "CockroachAsyncpgDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys", "by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys", "by_table"),
            table_name=table_name,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )

    async def _select_domain(
        self, driver: "CockroachAsyncpgDriver", domain: str, query_name: str, **parameters: Any
    ) -> MetadataResult:
        query = get_data_dictionary_loader().get_domain_query(type(self).dialect, domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult(domain, capability=query.capability, warnings=query.warnings)
        rows = await driver.select(query.sql, **parameters)
        return _metadata_result(domain, _cockroach_metadata_capability(domain), rows)


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


def _row_value(row: object, key: str) -> object | None:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _cockroach_ddl_result_from_rows(
    *,
    dialect: str,
    object_name: str,
    object_type: str,
    schema: str | None,
    rows: tuple[object, ...],
    warnings: tuple[str, ...] = (),
) -> DDLResult:
    row = rows[0] if rows else None
    resolved_schema = _row_value(row, "schema_name") if row is not None else schema
    ddl = _row_value(row, "ddl") if row is not None else None
    row_warning = _row_value(row, "warning") if row is not None else None
    result_warnings = warnings + ((str(row_warning),) if row_warning else ())
    identity = ObjectIdentity(
        name=object_name,
        object_type=object_type,
        schema=str(resolved_schema) if resolved_schema is not None else None,
        dialect=dialect,
        source=MetadataSource.INFORMATION_SCHEMA,
    )
    if ddl is not None:
        row_fidelity = _row_value(row, "fidelity") if row is not None else None
        return DDLResult(
            identity=identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=str(row_fidelity) if row_fidelity is not None else MetadataFidelity.NATIVE,
            source=MetadataSource.INFORMATION_SCHEMA,
            ddl=str(ddl),
            warnings=result_warnings,
        )
    return DDLResult.lossy(identity, ddl=None, source=MetadataSource.INFORMATION_SCHEMA, warnings=result_warnings)


def _cockroach_schema_ddl_from_rows(
    *,
    dialect: str,
    schema: str | None,
    rows: tuple[object, ...],
    foreign_keys: Sequence[ForeignKeyMetadata],
    exclude_tables: Sequence[str] | None,
    direction: Literal["create", "drop"],
    include_dependencies: bool,
) -> MetadataResult:
    excluded = {name.lower() for name in (exclude_tables or ())}
    filtered_rows: list[tuple[ObjectIdentity, str, str | None, str]] = []
    table_identities: dict[str, ObjectIdentity] = {}
    view_identities: dict[str, ObjectIdentity] = {}
    sequence_identities: dict[str, ObjectIdentity] = {}

    for row in rows:
        object_name_val = _row_value(row, "object_name")
        object_type_val = _row_value(row, "object_type")
        table_name_val = _row_value(row, "table_name")
        ddl_val = _row_value(row, "ddl")
        fidelity_val = _row_value(row, "fidelity")
        object_name = str(object_name_val) if object_name_val is not None else ""
        object_type = str(object_type_val or "table").lower()
        table_name = str(table_name_val) if table_name_val is not None else None
        if not object_name or ddl_val is None:
            continue
        if object_name.lower() in excluded:
            continue
        if table_name is not None and table_name.lower() in excluded:
            continue
        row_schema = _row_value(row, "schema_name")
        resolved_schema = str(row_schema) if row_schema is not None else schema
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=resolved_schema,
            dialect=dialect,
            source=MetadataSource.INFORMATION_SCHEMA,
        )
        fidelity = str(fidelity_val) if fidelity_val is not None else MetadataFidelity.NATIVE.value
        filtered_rows.append((identity, str(ddl_val), table_name, fidelity))
        if object_type == "table":
            table_identities[object_name.lower()] = identity
        elif object_type in {"view", "materialized_view"}:
            view_identities[object_name.lower()] = identity
        elif object_type == "sequence":
            sequence_identities[object_name.lower()] = identity

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
    for identity, ddl_text, table_name, fidelity in filtered_rows:
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
                edges.extend(
                    DependencyEdge(
                        identity,
                        seq_identity,
                        DependencyEdgeKind.SEQUENCE_USE,
                        source=MetadataSource.INFORMATION_SCHEMA,
                    )
                    for seq_identity in sequence_identities.values()
                    if re.search(rf"\b{re.escape(seq_identity.name)}\b", ddl_text, re.IGNORECASE)
                )
            elif obj_type in {"index", "trigger"} and table_name is not None:
                parent_identity = table_identities.get(table_name.lower())
                if parent_identity is not None:
                    edge_kind = (
                        DependencyEdgeKind.TRIGGER_TARGET
                        if obj_type == "trigger"
                        else DependencyEdgeKind.INDEX_EXPRESSION
                    )
                    edges.append(
                        DependencyEdge(identity, parent_identity, edge_kind, source=MetadataSource.INFORMATION_SCHEMA)
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
                fidelity=fidelity,
                source=MetadataSource.INFORMATION_SCHEMA,
                ddl=ddl_text,
                dependencies=tuple(edges),
            )
        )

    ordered = (
        sort_ddl_results(results, order=direction, raise_on_cycles=False) if include_dependencies else tuple(results)
    )
    capability = MetadataCapability(
        domain="ddl",
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.INFORMATION_SCHEMA,
    )
    return MetadataResult("ddl", capability=capability, items=ordered)


def _cockroach_domain_sql(domain: str, query_name: str) -> "SQL":
    query = get_data_dictionary_loader().get_domain_query("cockroachdb", domain, query_name)
    if query.sql is None:
        msg = f"Missing CockroachDB data-dictionary query: {domain}/{query_name}"
        raise RuntimeError(msg)
    return query.sql
