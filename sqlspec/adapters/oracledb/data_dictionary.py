"""Oracle-specific data dictionary for metadata queries."""

from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
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
    get_dialect_config,
    system_metadata_gated_result,
)
from sqlspec.data_dictionary.dialects.oracle import (
    extract_oracle_version_value,
    list_oracle_available_features,
    merge_oracle_table_lists,
    oracle_supports_json_blob,
    oracle_supports_native_json,
    oracle_supports_oson_blob,
    parse_oracle_compatible_major,
    parse_oracle_version_components,
    resolve_oracle_feature_flag,
    resolve_oracle_json_storage,
    resolve_oracle_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = (
    "JSONStorageType",
    "OracleVersionCache",
    "OracleVersionInfo",
    "OracledbAsyncDataDictionary",
    "OracledbSyncDataDictionary",
    "storage_type_from_version",
)

logger = get_logger("sqlspec.adapters.oracledb.data_dictionary")

ORACLE_DEFAULT_CAPABILITY_DOMAINS = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "materialized_views",
    "sequences",
    "routines",
    "arguments",
    "source",
    "triggers",
    "comments",
    "grants",
    "dependencies",
    "partitions",
    "lob_storage",
    "ddl",
    "system",
    "scope:user",
    "scope:all",
    "scope:dba",
    "scope:cdb",
)
ORACLE_PARTIAL_VISIBILITY_WARNING = "ALL_* views can omit inaccessible objects."
ORACLE_PRIVILEGED_SCOPE_WARNING = "DBA_* and CDB_* views require explicit privileged opt-in."
ORACLE_DIAGNOSTICS_WARNING = (
    "Oracle diagnostics, AWR/ASH/ADDM, Statspack, and DBA_HIST views require explicit opt-in, "
    "privileges, and license acknowledgement."
)
ORACLE_DDL_WARNINGS = (
    "DBMS_METADATA output depends on caller privileges and session transform settings.",
    "Native DDL can expose source text, comments, grants, storage details, and security-sensitive metadata.",
)
ORACLE_SYSTEM_METADATA_DOMAINS = ("system", "diagnostics", "awr", "ash", "addm", "statspack")
ORACLE_DISABLED_SYSTEM_METADATA_DOMAINS = ("no_diagnostics", "disabled")
ORACLE_SYSTEM_REDACTION_FIELDS = ("query_text", "sql_text", "username", "user_name", "session_user")


def _oracle_quoted_name(owner: "str | None", object_name: str) -> str:
    if owner is None:
        return quote_identifier(object_name)
    return f"{quote_identifier(owner)}.{quote_identifier(object_name)}"


def _oracle_scope_capability(domain: str, *, include_privileged: bool) -> MetadataCapability:
    if domain == "scope:user":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            warnings=("USER_* views are limited to the current schema.",),
        )
    if domain == "scope:all":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.CATALOG,
            risks=(MetadataRisk.PRIVILEGED,),
            warnings=(ORACLE_PARTIAL_VISIBILITY_WARNING,),
        )
    if domain in {"scope:dba", "scope:cdb"}:
        support = MetadataSupport.SUPPORTED if include_privileged else MetadataSupport.UNSUPPORTED
        return MetadataCapability(
            domain=domain,
            support=support,
            fidelity=MetadataFidelity.NATIVE if include_privileged else MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.CATALOG,
            risks=(MetadataRisk.PRIVILEGED,),
            warnings=(ORACLE_PRIVILEGED_SCOPE_WARNING,),
        )
    return MetadataCapability.unsupported(domain)


def _oracle_capability_for_domain(
    domain: str,
    *,
    include_privileged: bool = False,
    include_diagnostics: bool = False,
    acknowledge_diagnostics_license: bool = False,
) -> MetadataCapability:
    if domain.startswith("scope:"):
        return _oracle_scope_capability(domain, include_privileged=include_privileged)
    if domain == "ddl":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.NATIVE_API,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
            warnings=ORACLE_DDL_WARNINGS,
        )
    if domain == "system":
        diagnostics_enabled = include_diagnostics and acknowledge_diagnostics_license
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED if diagnostics_enabled else MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.NATIVE if diagnostics_enabled else MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.LICENSE_GATED, MetadataRisk.REDACTED),
            warnings=(ORACLE_DIAGNOSTICS_WARNING,),
        )
    risks: tuple[MetadataRisk, ...] = ()
    warnings: tuple[str, ...] = ()
    fidelity = MetadataFidelity.NATIVE
    if domain in {"objects", "tables", "columns", "constraints", "indexes", "views", "materialized_views"}:
        fidelity = MetadataFidelity.PARTIAL
        warnings = (ORACLE_PARTIAL_VISIBILITY_WARNING,)
    if domain in {"source", "comments", "grants"}:
        risks = (MetadataRisk.REDACTED,)
        warnings = ("Metadata can expose application source text, comments, grants, or security-sensitive details.",)
    if domain in {"partitions", "lob_storage"}:
        risks = (MetadataRisk.PRIVILEGED,)
        warnings = ("Storage metadata can be partial when segment or tablespace privileges are missing.",)
    return MetadataCapability(
        domain=domain,
        support=MetadataSupport.SUPPORTED,
        fidelity=fidelity,
        source=MetadataSource.CATALOG,
        risks=risks,
        warnings=warnings,
    )


def _oracle_capability_profile(
    adapter: str,
    domains: "Sequence[str] | None" = None,
    *,
    include_privileged: bool = False,
    include_diagnostics: bool = False,
    acknowledge_diagnostics_license: bool = False,
) -> MetadataCapabilityProfile:
    requested_domains = ORACLE_DEFAULT_CAPABILITY_DOMAINS if domains is None else tuple(domains)
    capabilities = tuple(
        _oracle_capability_for_domain(
            domain,
            include_privileged=include_privileged,
            include_diagnostics=include_diagnostics,
            acknowledge_diagnostics_license=acknowledge_diagnostics_license,
        )
        for domain in requested_domains
    )
    return MetadataCapabilityProfile(dialect="oracle", adapter=adapter, capabilities=capabilities)


def _oracle_domain_metadata_result(domain: str, rows: "list[object]") -> MetadataResult:
    capability = _oracle_capability_for_domain(domain)
    return MetadataResult(domain, capability=capability, items=tuple(rows), warnings=capability.warnings)


def _coerce_oracle_ddl_text(value: object) -> "str | None":
    if value is None:
        return None
    read = getattr(value, "read", None)
    if callable(read):
        return str(read())
    return str(value)


def _oracle_ddl_metadata_result(
    *, object_type: str, object_name: str, owner: "str | None", ddl_text: "str | None"
) -> DDLResult:
    identity = ObjectIdentity(
        name=object_name,
        object_type=object_type.lower(),
        schema=owner,
        dialect="oracle",
        quoted_name=_oracle_quoted_name(owner, object_name),
        source=MetadataSource.NATIVE_API,
    )
    if ddl_text is None:
        return DDLResult.unsupported(identity, source=MetadataSource.NATIVE_API, warnings=ORACLE_DDL_WARNINGS)
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.NATIVE_API,
        ddl=ddl_text,
        warnings=ORACLE_DDL_WARNINGS,
    )


def _oracle_system_metadata_capability(domain: str) -> SystemMetadataCapability:
    normalized_domain = domain.lower()
    if normalized_domain in ORACLE_DISABLED_SYSTEM_METADATA_DOMAINS:
        return SystemMetadataCapability(
            normalized_domain,
            MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.SYSTEM_VIEW,
            warnings=("Diagnostics are disabled by default.",),
        )
    if normalized_domain not in ORACLE_SYSTEM_METADATA_DOMAINS:
        return SystemMetadataCapability.unsupported(normalized_domain, source=MetadataSource.SYSTEM_VIEW)
    return SystemMetadataCapability(
        normalized_domain,
        MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.LICENSE_GATED, MetadataRisk.REDACTED),
        required_privileges=("DBA_HIST access", "Oracle Diagnostics Pack entitlement"),
        license_gate=ORACLE_DIAGNOSTICS_WARNING,
        redaction_fields=ORACLE_SYSTEM_REDACTION_FIELDS,
        warnings=(ORACLE_DIAGNOSTICS_WARNING,),
    )


def _oracle_system_metadata_request(
    request: SystemMetadataRequest | str | None = None, **kwargs: Any
) -> SystemMetadataRequest:
    acknowledge_license = bool(kwargs.pop("acknowledge_license", False))
    if acknowledge_license:
        kwargs.setdefault("allow_license_gated_diagnostics", True)
        kwargs.setdefault("include_performance", True)
    return ensure_system_metadata_request(request, **kwargs)


class OracleVersionInfo(VersionInfo):
    """Oracle database version information."""

    def __init__(
        self, major: int, minor: int = 0, patch: int = 0, compatible: "str | None" = None, is_autonomous: bool = False
    ) -> None:
        """Initialize Oracle version info.

        Args:
            major: Major version number.
            minor: Minor version number.
            patch: Patch version number.
            compatible: Compatible parameter value.
            is_autonomous: Whether this is an Autonomous Database.
        """
        super().__init__(major, minor, patch)
        self.compatible = compatible
        self.is_autonomous = is_autonomous

    @property
    def compatible_major(self) -> "int | None":
        """Get major version from compatible parameter."""
        return parse_oracle_compatible_major(self.compatible)

    def supports_native_json(self) -> bool:
        """Check if database supports native JSON data type."""
        return oracle_supports_native_json(self.major, self.compatible_major)

    def supports_oson_blob(self) -> bool:
        """Check if database supports BLOB with OSON format."""
        return oracle_supports_oson_blob(self.major, self.is_autonomous)

    def supports_json_blob(self) -> bool:
        """Check if database supports BLOB with JSON validation."""
        return oracle_supports_json_blob(self.major)

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.patch}"
        if self.compatible:
            version_str += f" (compatible={self.compatible})"
        if self.is_autonomous:
            version_str += " [Autonomous]"
        return version_str


class JSONStorageType(str, Enum):
    """Oracle JSON storage rung selected from the server version.

    The full ladder is a support contract: SQLSpec targets the oldest possible
    Oracle servers with graceful degradation, so every rung is retained.

    * ``JSON_NATIVE`` — native ``JSON`` type / OSON binary (21c+).
    * ``BLOB_JSON`` — ``BLOB`` validated by an ``IS JSON`` check constraint
      (12.1.0.2+).
    * ``BLOB_PLAIN`` — plain ``BLOB``/``CLOB`` fallback for servers older than 12c.
    """

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob_plain"


class OracleVersionCache:
    """Pool-scoped cache for the resolved Oracle server version.

    One instance is created per config and shared with every driver spawned from
    that config's pool, so the server version is resolved once per pool lifetime
    rather than once per acquired session. The cache lives and dies with the
    pool; a server upgrade under a live pool is out of scope and picked up on the
    next pool restart.
    """

    __slots__ = ("resolved", "version")

    def __init__(self) -> None:
        self.resolved: bool = False
        self.version: OracleVersionInfo | None = None

    def reset(self) -> None:
        """Clear the cached version so the next resolution re-queries the server."""
        self.resolved = False
        self.version = None


def _storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    """Determine the JSON storage rung for an Oracle version.

    An undetectable version defaults to ``BLOB_JSON`` — the ``IS JSON`` BLOB rung
    is valid on every 12c+ server, the overwhelmingly common case.
    """
    if version_info is None:
        return JSONStorageType.BLOB_JSON
    return JSONStorageType(resolve_oracle_json_storage(version_info.major, version_info.compatible_major))


def storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    """Public alias for :func:`_storage_type_from_version`."""
    return _storage_type_from_version(version_info)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbSyncDataDictionary(SyncDataDictionaryBase):
    """Oracle-specific sync data dictionary."""

    dialect: ClassVar[str] = "oracle"

    def __init__(self) -> None:
        super().__init__()

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        config = self.get_dialect_config()
        if schema is not None:
            return normalize_identifier(schema, config.name)
        if config.default_schema is None:
            return None
        return normalize_identifier(config.default_schema, config.name)

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = parse_oracle_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def list_available_features(self) -> "list[str]":
        return list_oracle_available_features(self.get_dialect_config())

    def get_metadata_capabilities(
        self,
        driver: object,
        domains: "Sequence[str] | None" = None,
        *,
        include_privileged: bool = False,
        include_diagnostics: bool = False,
        acknowledge_diagnostics_license: bool = False,
    ) -> MetadataCapabilityProfile:
        """Report Oracle replacement metadata capabilities and scope gates."""

        _ = driver
        return _oracle_capability_profile(
            type(self).__name__,
            domains,
            include_privileged=include_privileged,
            include_diagnostics=include_diagnostics,
            acknowledge_diagnostics_license=acknowledge_diagnostics_license,
        )

    def get_system_metadata_capabilities(
        self, driver: "OracleSyncDriver", domains: "Sequence[str] | None" = None
    ) -> tuple[SystemMetadataCapability, ...]:
        """Get Oracle opt-in system metadata capability disclosures."""

        _ = driver
        requested_domains = ORACLE_SYSTEM_METADATA_DOMAINS if domains is None else tuple(domains)
        return tuple(_oracle_system_metadata_capability(domain) for domain in requested_domains)

    def get_ddl(
        self,
        driver: "OracleSyncDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "TABLE",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Get native Oracle DDL using DBMS_METADATA."""

        _ = include_dependencies, prefer_native, redact
        owner = self.resolve_schema(schema)
        normalized_name = self.resolve_identifier(object_name)
        normalized_type = object_type.upper()
        query = get_data_dictionary_loader().get_domain_query("oracle", "ddl", "dbms_metadata")
        if not query.is_supported or query.sql is None:
            return _oracle_ddl_metadata_result(
                object_type=normalized_type, object_name=normalized_name, owner=owner, ddl_text=None
            )
        ddl_value = driver.select_value(
            query.sql, object_type=normalized_type, object_name=normalized_name, owner=owner
        )
        return _oracle_ddl_metadata_result(
            object_type=normalized_type,
            object_name=normalized_name,
            owner=owner,
            ddl_text=_coerce_oracle_ddl_text(ddl_value),
        )

    def get_system_metadata(
        self, driver: "OracleSyncDriver", request: SystemMetadataRequest | str | None = None, **kwargs: Any
    ) -> SystemMetadataResult:
        """Return Oracle system metadata only when diagnostics gates are accepted."""

        _ = driver
        metadata_request = _oracle_system_metadata_request(request, **kwargs)
        capability = _oracle_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        rows = (
            {
                "diagnostics_enabled": metadata_request.domain not in ORACLE_DISABLED_SYSTEM_METADATA_DOMAINS,
                "domain": metadata_request.domain,
            },
        )
        return SystemMetadataResult.from_rows(
            metadata_request, capability, rows=rows, source=MetadataSource.SYSTEM_VIEW
        )

    def _select_domain(
        self, driver: "OracleSyncDriver", domain: str, *, query_name: str = "by_owner", **binds: object
    ) -> MetadataResult:
        query = get_data_dictionary_loader().get_domain_query("oracle", domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult.unsupported(domain, source=query.capability.source)
        select = cast("Any", driver.select)
        rows = select(query.sql, **binds)
        return _oracle_domain_metadata_result(domain, cast("list[object]", rows))

    def get_schemas(self, driver: "OracleSyncDriver") -> MetadataResult:
        """Get Oracle user/schema metadata."""

        return self._select_domain(driver, "schemas", schema_name=None)

    def get_objects(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle object metadata from ALL_OBJECTS."""

        return self._select_domain(driver, "objects", schema_name=self.resolve_schema(schema), object_name=None)

    def get_table_details(self, driver: "OracleSyncDriver", table: str, schema: "str | None" = None) -> MetadataResult:
        """Get rich Oracle table metadata."""

        return self._select_domain(
            driver, "tables", schema_name=self.resolve_schema(schema), table_name=self.resolve_identifier(table)
        )

    def get_constraints(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle constraint metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return self._select_domain(
            driver, "constraints", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    def get_views(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle view metadata."""

        return self._select_domain(driver, "views", schema_name=self.resolve_schema(schema), view_name=None)

    def get_materialized_views(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle materialized view metadata."""

        return self._select_domain(
            driver, "materialized_views", schema_name=self.resolve_schema(schema), mview_name=None
        )

    def get_sequences(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle sequence metadata."""

        return self._select_domain(driver, "sequences", schema_name=self.resolve_schema(schema), sequence_name=None)

    def get_routines(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle routine, package, procedure, and function metadata."""

        return self._select_domain(driver, "routines", schema_name=self.resolve_schema(schema), object_name=None)

    def get_triggers(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle trigger metadata."""

        return self._select_domain(driver, "triggers", schema_name=self.resolve_schema(schema), trigger_name=None)

    def get_privileges(
        self, driver: "OracleSyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle table and column grants."""

        normalized_object = None if object_name is None else self.resolve_identifier(object_name)
        return self._select_domain(
            driver, "grants", schema_name=self.resolve_schema(schema), object_name=normalized_object
        )

    def get_dependencies(
        self, driver: "OracleSyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle dependency metadata."""

        normalized_object = None if object_name is None else self.resolve_identifier(object_name)
        return self._select_domain(
            driver, "dependencies", schema_name=self.resolve_schema(schema), object_name=normalized_object
        )

    def get_partitions(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle partition and storage metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return self._select_domain(driver, "partitions", schema_name=self.resolve_schema(schema), table_name=table_name)

    def get_lob_storage(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle LOB storage metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return self._select_domain(
            driver, "lob_storage", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    def _get_compatible_value(self, driver: "OracleSyncDriver") -> "str | None":
        query_text = self.get_query_text("compatible")
        try:
            value = driver.select_value(query_text)
            if value is None:
                return None
            return str(value)
        except Exception:
            return None

    def _is_autonomous(self, driver: "OracleSyncDriver") -> bool:
        query_text = self.get_query_text("autonomous_service")
        try:
            return bool(driver.select_value_or_none(query_text))
        except Exception:
            return False

    def get_version(self, driver: "OracleSyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information through the pool-scoped cache."""
        holder: OracleVersionCache | None = getattr(driver, "_oracle_version_cache", None)
        driver_id = id(driver)
        if holder is not None:
            if holder.resolved:
                return holder.version
        elif driver_id in self._version_fetch_attempted:
            return cast("OracleVersionInfo | None", self._version_cache.get(driver_id))

        version_info = self._fetch_version(driver)
        if holder is not None:
            holder.resolved = True
            holder.version = version_info
        else:
            self.cache_version(driver_id, version_info)
        return version_info

    def _fetch_version(self, driver: "OracleSyncDriver") -> "OracleVersionInfo | None":
        """Query the server for its version, compatible level, and autonomous flag."""
        version_row = driver.select_one_or_none(self.get_query_text("version"))
        if not version_row:
            self._log_version_unavailable(type(self).dialect, "missing")
            return None

        version_value = extract_oracle_version_value(version_row)
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            return None

        compatible = self._get_compatible_value(driver)
        is_autonomous = self._is_autonomous(driver)
        version_info = self._build_version_info(version_value, compatible, is_autonomous)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            return None

        self._log_version_detected(type(self).dialect, version_info)
        return version_info

    def get_feature_flag(self, driver: "OracleSyncDriver", feature: str) -> bool:
        """Check if Oracle database supports a specific feature."""
        version_info = self.get_version(driver)
        return resolve_oracle_feature_flag(
            self.get_dialect_config(),
            version_info,
            feature,
            compatible_major=version_info.compatible_major if version_info is not None else None,
            is_autonomous=bool(version_info and version_info.is_autonomous),
        )

    def get_optimal_type(self, driver: "OracleSyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            version_info = self.get_version(driver)
            return resolve_oracle_json_type(
                version_info,
                compatible_major=version_info.compatible_major if version_info is not None else None,
                is_autonomous=bool(version_info and version_info.is_autonomous),
            )
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by dependency order with full coverage."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered_rows = driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        all_rows = driver.select(
            self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        return merge_oracle_table_lists(ordered_rows, all_rows)

    def get_columns(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )
        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
        return driver.select(
            self.get_query("columns_by_table"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ColumnMetadata,
        )

    def get_indexes(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )
        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
        return driver.select(
            self.get_query("indexes_by_table"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=IndexMetadata,
        )

    def get_foreign_keys(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
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
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ForeignKeyMetadata,
        )


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbAsyncDataDictionary(AsyncDataDictionaryBase):
    """Oracle-specific async data dictionary."""

    dialect: ClassVar[str] = "oracle"

    def __init__(self) -> None:
        super().__init__()

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        config = self.get_dialect_config()
        if schema is not None:
            return normalize_identifier(schema, config.name)
        if config.default_schema is None:
            return None
        return normalize_identifier(config.default_schema, config.name)

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = parse_oracle_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def list_available_features(self) -> "list[str]":
        return list_oracle_available_features(self.get_dialect_config())

    async def get_metadata_capabilities(
        self,
        driver: object,
        domains: "Sequence[str] | None" = None,
        *,
        include_privileged: bool = False,
        include_diagnostics: bool = False,
        acknowledge_diagnostics_license: bool = False,
    ) -> MetadataCapabilityProfile:
        """Report Oracle replacement metadata capabilities and scope gates."""

        _ = driver
        return _oracle_capability_profile(
            type(self).__name__,
            domains,
            include_privileged=include_privileged,
            include_diagnostics=include_diagnostics,
            acknowledge_diagnostics_license=acknowledge_diagnostics_license,
        )

    async def get_system_metadata_capabilities(
        self, driver: "OracleAsyncDriver", domains: "Sequence[str] | None" = None
    ) -> tuple[SystemMetadataCapability, ...]:
        """Get Oracle opt-in system metadata capability disclosures."""

        _ = driver
        requested_domains = ORACLE_SYSTEM_METADATA_DOMAINS if domains is None else tuple(domains)
        return tuple(_oracle_system_metadata_capability(domain) for domain in requested_domains)

    async def get_ddl(
        self,
        driver: "OracleAsyncDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "TABLE",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Get native Oracle DDL using DBMS_METADATA."""

        _ = include_dependencies, prefer_native, redact
        owner = self.resolve_schema(schema)
        normalized_name = self.resolve_identifier(object_name)
        normalized_type = object_type.upper()
        query = get_data_dictionary_loader().get_domain_query("oracle", "ddl", "dbms_metadata")
        if not query.is_supported or query.sql is None:
            return _oracle_ddl_metadata_result(
                object_type=normalized_type, object_name=normalized_name, owner=owner, ddl_text=None
            )
        ddl_value = await driver.select_value(
            query.sql, object_type=normalized_type, object_name=normalized_name, owner=owner
        )
        return _oracle_ddl_metadata_result(
            object_type=normalized_type,
            object_name=normalized_name,
            owner=owner,
            ddl_text=_coerce_oracle_ddl_text(ddl_value),
        )

    async def get_system_metadata(
        self, driver: "OracleAsyncDriver", request: SystemMetadataRequest | str | None = None, **kwargs: Any
    ) -> SystemMetadataResult:
        """Return Oracle system metadata only when diagnostics gates are accepted."""

        _ = driver
        metadata_request = _oracle_system_metadata_request(request, **kwargs)
        capability = _oracle_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        rows = (
            {
                "diagnostics_enabled": metadata_request.domain not in ORACLE_DISABLED_SYSTEM_METADATA_DOMAINS,
                "domain": metadata_request.domain,
            },
        )
        return SystemMetadataResult.from_rows(
            metadata_request, capability, rows=rows, source=MetadataSource.SYSTEM_VIEW
        )

    async def _select_domain(
        self, driver: "OracleAsyncDriver", domain: str, *, query_name: str = "by_owner", **binds: object
    ) -> MetadataResult:
        query = get_data_dictionary_loader().get_domain_query("oracle", domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult.unsupported(domain, source=query.capability.source)
        select = cast("Any", driver.select)
        rows = await select(query.sql, **binds)
        return _oracle_domain_metadata_result(domain, cast("list[object]", rows))

    async def get_schemas(self, driver: "OracleAsyncDriver") -> MetadataResult:
        """Get Oracle user/schema metadata."""

        return await self._select_domain(driver, "schemas", schema_name=None)

    async def get_objects(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle object metadata from ALL_OBJECTS."""

        return await self._select_domain(driver, "objects", schema_name=self.resolve_schema(schema), object_name=None)

    async def get_table_details(
        self, driver: "OracleAsyncDriver", table: str, schema: "str | None" = None
    ) -> MetadataResult:
        """Get rich Oracle table metadata."""

        return await self._select_domain(
            driver, "tables", schema_name=self.resolve_schema(schema), table_name=self.resolve_identifier(table)
        )

    async def get_constraints(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle constraint metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return await self._select_domain(
            driver, "constraints", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    async def get_views(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle view metadata."""

        return await self._select_domain(driver, "views", schema_name=self.resolve_schema(schema), view_name=None)

    async def get_materialized_views(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle materialized view metadata."""

        return await self._select_domain(
            driver, "materialized_views", schema_name=self.resolve_schema(schema), mview_name=None
        )

    async def get_sequences(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle sequence metadata."""

        return await self._select_domain(
            driver, "sequences", schema_name=self.resolve_schema(schema), sequence_name=None
        )

    async def get_routines(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle routine, package, procedure, and function metadata."""

        return await self._select_domain(driver, "routines", schema_name=self.resolve_schema(schema), object_name=None)

    async def get_triggers(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Oracle trigger metadata."""

        return await self._select_domain(driver, "triggers", schema_name=self.resolve_schema(schema), trigger_name=None)

    async def get_privileges(
        self, driver: "OracleAsyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle table and column grants."""

        normalized_object = None if object_name is None else self.resolve_identifier(object_name)
        return await self._select_domain(
            driver, "grants", schema_name=self.resolve_schema(schema), object_name=normalized_object
        )

    async def get_dependencies(
        self, driver: "OracleAsyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle dependency metadata."""

        normalized_object = None if object_name is None else self.resolve_identifier(object_name)
        return await self._select_domain(
            driver, "dependencies", schema_name=self.resolve_schema(schema), object_name=normalized_object
        )

    async def get_partitions(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle partition and storage metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return await self._select_domain(
            driver, "partitions", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    async def get_lob_storage(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Oracle LOB storage metadata."""

        table_name = None if table is None else self.resolve_identifier(table)
        return await self._select_domain(
            driver, "lob_storage", schema_name=self.resolve_schema(schema), table_name=table_name
        )

    async def _get_compatible_value(self, driver: "OracleAsyncDriver") -> "str | None":
        query_text = self.get_query_text("compatible")
        try:
            value = await driver.select_value(query_text)
            if value is None:
                return None
            return str(value)
        except Exception:
            return None

    async def _is_autonomous(self, driver: "OracleAsyncDriver") -> bool:
        query_text = self.get_query_text("autonomous_service")
        try:
            return bool(await driver.select_value_or_none(query_text))
        except Exception:
            return False

    async def get_version(self, driver: "OracleAsyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information through the pool-scoped cache."""
        holder: OracleVersionCache | None = getattr(driver, "_oracle_version_cache", None)
        driver_id = id(driver)
        if holder is not None:
            if holder.resolved:
                return holder.version
        elif driver_id in self._version_fetch_attempted:
            return cast("OracleVersionInfo | None", self._version_cache.get(driver_id))

        version_info = await self._fetch_version(driver)
        if holder is not None:
            holder.resolved = True
            holder.version = version_info
        else:
            self.cache_version(driver_id, version_info)
        return version_info

    async def _fetch_version(self, driver: "OracleAsyncDriver") -> "OracleVersionInfo | None":
        """Query the server for its version, compatible level, and autonomous flag."""
        version_row = await driver.select_one_or_none(self.get_query_text("version"))
        if not version_row:
            self._log_version_unavailable(type(self).dialect, "missing")
            return None

        version_value = extract_oracle_version_value(version_row)
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            return None

        compatible = await self._get_compatible_value(driver)
        is_autonomous = await self._is_autonomous(driver)
        version_info = self._build_version_info(version_value, compatible, is_autonomous)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            return None

        self._log_version_detected(type(self).dialect, version_info)
        return version_info

    async def get_feature_flag(self, driver: "OracleAsyncDriver", feature: str) -> bool:
        """Check if Oracle database supports a specific feature."""
        version_info = await self.get_version(driver)
        return resolve_oracle_feature_flag(
            self.get_dialect_config(),
            version_info,
            feature,
            compatible_major=version_info.compatible_major if version_info is not None else None,
            is_autonomous=bool(version_info and version_info.is_autonomous),
        )

    async def get_optimal_type(self, driver: "OracleAsyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            version_info = await self.get_version(driver)
            return resolve_oracle_json_type(
                version_info,
                compatible_major=version_info.compatible_major if version_info is not None else None,
                is_autonomous=bool(version_info and version_info.is_autonomous),
            )
        return self.get_dialect_config().get_optimal_type(type_category)

    async def get_tables(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by dependency order with full coverage."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered_rows = await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        all_rows = await driver.select(
            self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        return merge_oracle_table_lists(ordered_rows, all_rows)

    async def get_columns(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )
        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
        return await driver.select(
            self.get_query("columns_by_table"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ColumnMetadata,
        )

    async def get_indexes(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )
        table_name = self.resolve_identifier(table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
        return await driver.select(
            self.get_query("indexes_by_table"),
            schema_name=schema_name,
            table_name=table_name,
            schema_type=IndexMetadata,
        )

    async def get_foreign_keys(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
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
            schema_name=schema_name,
            table_name=table_name,
            schema_type=ForeignKeyMetadata,
        )
