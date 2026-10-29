"""SQL Server dialect static configuration and reflection helpers."""

import re
from typing import TYPE_CHECKING, Any, Final

from sqlspec.builder._ddl import compile_table_ddl_results
from sqlspec.data_dictionary import (
    DDLResult,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    SystemMetadataCapability,
    SystemMetadataRedactionPolicy,
    SystemMetadataRequest,
    SystemMetadataResult,
    VersionInfo,
    register_dialect,
    system_metadata_gated_result,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.data_dictionary import TableMetadata

__all__ = (
    "MSSQL_CONFIG",
    "MSSQL_DDL_WARNING",
    "MSSQL_DYNAMIC_FEATURES",
    "MSSQL_ENGINE_EDITION_AZURE_SET",
    "MSSQL_FEATURE_FLAGS",
    "MSSQL_FEATURE_VERSIONS",
    "MSSQL_MIN_GREATEST_LEAST_VERSION",
    "MSSQL_MIN_JSON_FUNCTIONS_VERSION",
    "MSSQL_MIN_NATIVE_JSON_VERSION",
    "MSSQL_MIN_STRING_AGG_VERSION",
    "MSSQL_PERMISSIONS_WARNING",
    "MSSQL_PRODUCT_VERSION_PATTERN",
    "MSSQL_QUERY_STORE_PERMISSION_WARNING",
    "MSSQL_REDACTED_VALUE",
    "MSSQL_REPLACEMENT_DOMAINS",
    "MSSQL_SENSITIVE_SYSTEM_KEYS",
    "MSSQL_SYSTEM_METADATA_DOMAINS",
    "MSSQL_SYSTEM_OPT_IN_WARNING",
    "MSSQL_SYSTEM_PERMISSION_WARNING",
    "MSSQL_SYSTEM_WARNING",
    "MSSQL_TYPE_MAPPINGS",
    "MSSQL_VERSION_PARTS_COUNT",
    "MSSQL_VERSION_PATTERN",
    "MssqlVersionInfo",
    "build_mssql_metadata_capability_profile",
    "build_mssql_system_metadata_capability",
    "build_mssql_system_metadata_result",
    "build_mssql_table_ddl_result",
    "extract_mssql_version_value",
    "get_mssql_data_dictionary_options",
    "is_mssql_azure_sql",
    "list_mssql_available_features",
    "merge_mssql_table_lists",
    "mssql_supports_greatest_least",
    "mssql_supports_json_functions",
    "mssql_supports_native_json",
    "mssql_supports_string_agg",
    "mssql_system_metadata_denied",
    "parse_mssql_engine_edition",
    "parse_mssql_version_components",
    "resolve_mssql_feature_flag",
    "validate_mssql_system_metadata_options",
)

MSSQL_VERSION_PATTERN = re.compile(r"(\d+)")
MSSQL_PRODUCT_VERSION_PATTERN = re.compile(r"\b(\d+)\.(\d+)\.(\d+)\.(\d+)\b")
MSSQL_VERSION_PARTS_COUNT: Final[int] = 4
MSSQL_MIN_JSON_FUNCTIONS_VERSION: Final[int] = 13
MSSQL_MIN_STRING_AGG_VERSION: Final[int] = 14
MSSQL_MIN_GREATEST_LEAST_VERSION: Final[int] = 16
MSSQL_MIN_NATIVE_JSON_VERSION: Final[int] = 17
MSSQL_ENGINE_EDITION_AZURE_SET: Final[frozenset[int]] = frozenset({5, 8, 11})

MSSQL_DYNAMIC_FEATURES: Final[tuple[str, ...]] = (
    "is_azure_sql",
    "supports_json_functions",
    "supports_string_agg",
    "supports_greatest_least",
    "supports_native_json",
)

MSSQL_REPLACEMENT_DOMAINS: Final[tuple[str, ...]] = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "modules",
    "routines",
    "triggers",
    "sequences",
    "extended_properties",
    "comments",
    "permissions",
    "privileges",
    "dependencies",
    "storage",
    "ddl",
    "system",
)
MSSQL_SYSTEM_METADATA_DOMAINS: Final[tuple[str, ...]] = ("dmv_exec_requests", "query_store_runtime")

MSSQL_DDL_WARNING: Final[str] = "SQL Server table DDL is reconstructed from sys catalog views."
MSSQL_PERMISSIONS_WARNING: Final[str] = (
    "Permission metadata is visibility-scoped and may require VIEW DEFINITION on secured objects."
)
MSSQL_SYSTEM_WARNING: Final[str] = (
    "System and DMV metadata is opt-in, may expose operational SQL text or principals, and may require "
    "VIEW SERVER STATE, SQL Server 2022 VIEW SERVER PERFORMANCE STATE, VIEW SERVER SECURITY STATE, "
    "Query Store access, or Azure SQL equivalents."
)
MSSQL_SYSTEM_OPT_IN_WARNING: Final[str] = (
    "SQL Server system metadata requires driver_features['data_dictionary']['enable_system_metadata']."
)
MSSQL_SYSTEM_PERMISSION_WARNING: Final[str] = (
    "SQL Server DMV metadata requires VIEW SERVER STATE or the SQL Server 2022 VIEW SERVER PERFORMANCE STATE "
    "equivalent."
)
MSSQL_QUERY_STORE_PERMISSION_WARNING: Final[str] = "Query Store metadata requires explicit query_store access."
MSSQL_REDACTED_VALUE: Final[str] = "[redacted]"
MSSQL_SENSITIVE_SYSTEM_KEYS: Final[frozenset[str]] = frozenset({
    "client_interface_name",
    "host_name",
    "login_name",
    "program_name",
    "query_sql_text",
    "sql_text",
    "user_name",
})

MSSQL_FEATURE_VERSIONS: "FeatureVersions" = {}

MSSQL_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_in_memory": True,
    "supports_for_update": False,
    "supports_skip_locked": False,
    "supports_on_conflict": False,
    "supports_update_from": True,
}

MSSQL_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UNIQUEIDENTIFIER",
    "boolean": "BIT",
    "text": "NVARCHAR(MAX)",
    "json": "NVARCHAR(MAX)",
    "jsonb": "NVARCHAR(MAX)",
    "timestamp": "DATETIME2(6)",
    "timestamptz": "DATETIMEOFFSET(6)",
    "bytea": "VARBINARY(MAX)",
    "blob": "VARBINARY(MAX)",
    "integer": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "varchar": "NVARCHAR({length})",
}

MSSQL_CONFIG = DialectConfig(
    name="mssql",
    feature_versions=MSSQL_FEATURE_VERSIONS,
    feature_flags=MSSQL_FEATURE_FLAGS,
    type_mappings=MSSQL_TYPE_MAPPINGS,
    version_pattern=MSSQL_VERSION_PATTERN,
    default_schema="dbo",
    parameter_style="qmark",
)

register_dialect(MSSQL_CONFIG)


class MssqlVersionInfo(VersionInfo):
    """MSSQL database version info with build, revision, and Azure SQL detection."""

    __slots__ = ("build", "edition", "engine_edition", "is_azure_sql", "revision")

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

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.build}.{self.revision}"
        if self.edition:
            version_str += f" ({self.edition})"
        if self.is_azure_sql:
            version_str += " [Azure]"
        return version_str

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version tuple using the MSSQL build number as the third component."""
        return (self.major, self.minor, self.build)

    def supports_native_json(self) -> bool:
        """Return whether this server supports the native JSON type."""
        return mssql_supports_native_json(self.major, is_azure_sql=self.is_azure_sql)


def extract_mssql_version_value(row: object) -> "str | None":
    """Extract a SQL Server version string from a row-like object."""
    if isinstance(row, dict):
        for key in ("version", "VERSION", "Version", "product_version", "ProductVersion"):
            value = row.get(key)
            if value:
                return str(value)
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    if row is not None:
        return str(row)
    return None


def parse_mssql_version_components(version_string: str) -> tuple[int, int, int, int]:
    """Parse MSSQL version text into major, minor, build, revision components."""
    product_version = MSSQL_PRODUCT_VERSION_PATTERN.search(version_string)
    if product_version is not None:
        groups = product_version.groups()
        return int(groups[0]), int(groups[1]), int(groups[2]), int(groups[3])

    parts = [int(value) for value in MSSQL_VERSION_PATTERN.findall(version_string)]
    if not parts:
        return 0, 0, 0, 0
    while len(parts) < MSSQL_VERSION_PARTS_COUNT:
        parts.append(0)
    return parts[0], parts[1], parts[2], parts[3]


def parse_mssql_engine_edition(value: Any) -> int | None:
    """Parse a SQL Server EngineEdition value into an integer."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode(errors="ignore")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_mssql_azure_sql(engine_edition: int | None) -> bool:
    """Return whether an EngineEdition value represents Azure SQL."""
    return engine_edition in MSSQL_ENGINE_EDITION_AZURE_SET


def mssql_supports_json_functions(major: int) -> bool:
    """Return whether the SQL Server version supports JSON functions."""
    return major >= MSSQL_MIN_JSON_FUNCTIONS_VERSION


def mssql_supports_string_agg(major: int) -> bool:
    """Return whether the SQL Server version supports STRING_AGG."""
    return major >= MSSQL_MIN_STRING_AGG_VERSION


def mssql_supports_greatest_least(major: int) -> bool:
    """Return whether the SQL Server version supports GREATEST and LEAST."""
    return major >= MSSQL_MIN_GREATEST_LEAST_VERSION


def mssql_supports_native_json(major: int, is_azure_sql: bool = False) -> bool:
    """Return whether the SQL Server version supports the native JSON type."""
    return is_azure_sql or major >= MSSQL_MIN_NATIVE_JSON_VERSION


def resolve_mssql_feature_flag(
    feature: str,
    *,
    major: int,
    is_azure_sql: bool = False,
    config: "DialectConfig | None" = None,
    version_info: "VersionInfo | None" = None,
) -> bool:
    """Resolve an MSSQL feature flag from static config and version details."""
    if version_info is not None:
        major = version_info.major
    if feature == "is_azure_sql":
        return is_azure_sql
    if feature == "supports_json_functions":
        return mssql_supports_json_functions(major)
    if feature == "supports_string_agg":
        return mssql_supports_string_agg(major)
    if feature == "supports_greatest_least":
        return mssql_supports_greatest_least(major)
    if feature == "supports_native_json":
        return mssql_supports_native_json(major, is_azure_sql=is_azure_sql)

    dialect_config = config or MSSQL_CONFIG
    flag = dialect_config.get_feature_flag(feature)
    if flag is not None:
        return flag
    required_version = dialect_config.get_feature_version(feature)
    if required_version is None or version_info is None:
        return False
    return bool(version_info >= required_version)


def list_mssql_available_features(config: "DialectConfig | None" = None) -> list[str]:
    """List static and dynamic MSSQL data-dictionary feature flags."""
    dialect_config = config or MSSQL_CONFIG
    features: set[str] = set()
    features.update(dialect_config.feature_flags.keys())
    features.update(dialect_config.feature_versions.keys())
    features.update(MSSQL_DYNAMIC_FEATURES)
    return sorted(features)


def build_mssql_metadata_capability_profile(
    adapter: str | None, domains: "Sequence[str] | None" = None
) -> MetadataCapabilityProfile:
    """Build SQL Server data-dictionary capability metadata."""
    requested_domains = tuple(domains) if domains is not None else MSSQL_REPLACEMENT_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested_domains:
        if domain == "ddl":
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.GENERATED,
                    source=MetadataSource.GENERATED,
                    warnings=(MSSQL_DDL_WARNING,),
                )
            )
            continue
        if domain == "system":
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.PARTIAL,
                    source=MetadataSource.SYSTEM_VIEW,
                    risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
                    warnings=(MSSQL_SYSTEM_WARNING,),
                )
            )
            continue
        if domain in {"permissions", "privileges"}:
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.NATIVE,
                    source=MetadataSource.CATALOG,
                    risks=(MetadataRisk.PRIVILEGED,),
                    warnings=(MSSQL_PERMISSIONS_WARNING,),
                )
            )
            continue
        if domain in MSSQL_REPLACEMENT_DOMAINS:
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.NATIVE,
                    source=MetadataSource.CATALOG,
                )
            )
            continue
        capabilities.append(MetadataCapability.unsupported(domain))
    return MetadataCapabilityProfile("mssql", adapter=adapter, capabilities=tuple(capabilities))


def get_mssql_data_dictionary_options(driver: Any) -> dict[str, Any]:
    """Return SQL Server data-dictionary feature options from a driver."""
    driver_features = getattr(driver, "driver_features", {})
    if not isinstance(driver_features, dict):
        return {}
    options = driver_features.get("data_dictionary", {})
    if not isinstance(options, dict):
        return {}
    return options


def build_mssql_system_metadata_capability(domain: str) -> SystemMetadataCapability:
    """Build a SQL Server system metadata capability disclosure."""
    if domain not in MSSQL_SYSTEM_METADATA_DOMAINS:
        return SystemMetadataCapability.unsupported(domain, source=MetadataSource.SYSTEM_VIEW)
    return SystemMetadataCapability(
        domain=domain,
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.PARTIAL,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
        required_privileges=("VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE", "Query Store access"),
        redaction_fields=SystemMetadataRedactionPolicy.sensitive_field_names(),
        warnings=(MSSQL_SYSTEM_WARNING,),
    )


def mssql_system_metadata_denied(request: SystemMetadataRequest, *warnings: str) -> SystemMetadataResult:
    """Return a permission-aware unsupported system metadata result."""
    capability = SystemMetadataCapability(
        domain=request.domain,
        support=MetadataSupport.UNSUPPORTED,
        fidelity=MetadataFidelity.UNSUPPORTED,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
        redaction_fields=SystemMetadataRedactionPolicy.sensitive_field_names(),
        warnings=warnings,
    )
    return SystemMetadataResult(request, capability, source=MetadataSource.SYSTEM_VIEW)


def validate_mssql_system_metadata_options(
    request: SystemMetadataRequest, options: dict[str, Any]
) -> SystemMetadataResult | None:
    """Return an unsupported result when SQL Server system metadata gates fail."""
    if not options.get("enable_system_metadata"):
        return mssql_system_metadata_denied(request, MSSQL_SYSTEM_OPT_IN_WARNING)
    if request.domain.startswith("query_store") and not options.get("query_store"):
        return mssql_system_metadata_denied(request, MSSQL_QUERY_STORE_PERMISSION_WARNING)
    if request.domain.startswith("dmv") and not (
        options.get("view_server_state") or options.get("view_server_performance_state")
    ):
        return mssql_system_metadata_denied(request, MSSQL_SYSTEM_PERMISSION_WARNING)
    return None


def build_mssql_system_metadata_result(request: SystemMetadataRequest, rows: "Sequence[Any]") -> SystemMetadataResult:
    """Build redacted SQL Server system metadata rows."""
    capability = build_mssql_system_metadata_capability(request.domain)
    gate_result = system_metadata_gated_result(request, capability)
    if gate_result.capability.support != MetadataSupport.SUPPORTED:
        return gate_result
    return SystemMetadataResult.from_rows(
        request, capability, rows=tuple(_row_to_dict(row) for row in rows), source=MetadataSource.SYSTEM_VIEW
    )


def build_mssql_table_ddl_result(
    schema_name: str | None,
    table_name: str,
    column_rows: "Sequence[Any]",
    index_rows: "Sequence[Any] | None" = None,
    *,
    constraint_rows: "Sequence[Any] | None" = None,
    foreign_key_rows: "Sequence[Any] | None" = None,
    target_dialect: str | None = None,
    object_type: str = "table",
) -> DDLResult:
    """Generate SQL Server table DDL from structured catalog rows."""
    schema = schema_name or MSSQL_CONFIG.default_schema
    identity = ObjectIdentity(
        name=table_name,
        object_type=object_type,
        schema=schema,
        dialect=target_dialect or "mssql",
        quoted_name=_mssql_qualified_name(schema, table_name) if not target_dialect else None,
        source=MetadataSource.GENERATED,
    )
    if not column_rows:
        return DDLResult.unsupported(
            identity,
            source=MetadataSource.GENERATED,
            warnings=(f"No SQL Server catalog columns found for {schema or '<default>'}.{table_name}",),
        )

    effective_dialect = target_dialect or "tsql"
    compiled = compile_table_ddl_results(
        table_name,
        column_rows,
        schema=schema,
        constraints=constraint_rows,
        foreign_keys=foreign_key_rows,
        indexes=index_rows,
        dialect=effective_dialect,
        object_type=object_type,
        warnings=(MSSQL_DDL_WARNING,),
    )
    ddl_text = compiled.ddl
    if ddl_text is not None and effective_dialect in {"tsql", "mssql"}:
        ddl_text = re.sub(
            r"(\[[^\]]+\])\s+INTEGER\s+NOT NULL\s+IDENTITY\((\d+),\s*(\d+)\)",
            r"\1 int IDENTITY(\2,\3) NOT NULL",
            ddl_text,
        )
    return DDLResult(
        identity,
        compiled.status,
        fidelity=compiled.fidelity,
        source=compiled.source,
        ddl=ddl_text,
        dependencies=compiled.dependencies,
        redactions=compiled.redactions,
        context=compiled.context,
        warnings=compiled.warnings,
    )


def merge_mssql_table_lists(ordered: "list[TableMetadata]", all_rows: "list[TableMetadata]") -> "list[TableMetadata]":
    """Merge dependency-ordered table rows with catalog rows not in the dependency tree."""
    merged: list[TableMetadata] = []
    seen: set[tuple[str | None, str | None]] = set()

    for row in ordered:
        key = (row.get("schema_name") or row.get("table_schema"), row.get("table_name"))
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    for row in all_rows:
        key = (row.get("schema_name") or row.get("table_schema"), row.get("table_name"))
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    return merged


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    return {key: getattr(row, key) for key in dir(row) if not key.startswith("_") and not callable(getattr(row, key))}


def _quote_mssql_identifier(value: str) -> str:
    return f"[{value.replace(']', ']]')}]"


def _mssql_qualified_name(schema: str | None, name: str) -> str:
    if schema:
        return f"{_quote_mssql_identifier(schema)}.{_quote_mssql_identifier(name)}"
    return _quote_mssql_identifier(name)
