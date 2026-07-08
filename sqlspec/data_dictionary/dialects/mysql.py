import re

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
    VersionInfo,
    register_dialect,
)
from sqlspec.utils.text import quote_backtick_identifier, split_qualified_identifier

__all__ = (
    "MARIADB_CONFIG",
    "MYSQL_CONFIG",
    "MySQLEngineVersion",
    "build_mysql_metadata_capability_profile",
    "build_mysql_show_create_statement",
    "build_mysql_system_metadata_capability",
    "format_mysql_identifier",
    "make_mysql_ddl_result",
    "mysql_system_metadata_query_name",
    "parse_mysql_engine_version",
    "resolve_mysql_json_type",
)


MYSQL_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

MYSQL_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(5, 7, 8),
    "supports_cte": VersionInfo(8, 0, 1),
    "supports_window_functions": VersionInfo(8, 0, 2),
    "supports_skip_locked": VersionInfo(8, 0, 1),
    "supports_generated_columns": VersionInfo(5, 7, 0),
    "supports_check_constraints": VersionInfo(8, 0, 16),
    "supports_invisible_columns": VersionInfo(8, 0, 23),
    "supports_invisible_indexes": VersionInfo(8, 0, 0),
    "supports_roles": VersionInfo(8, 0, 0),
    "supports_events": VersionInfo(5, 1, 6),
    "supports_resource_groups": VersionInfo(8, 0, 3),
    "supports_histograms": VersionInfo(8, 0, 2),
}

MYSQL_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_returning": False,
    "supports_upsert": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_arrays": False,
    "supports_uuid": False,
    "supports_for_update": True,
    "supports_sequences": False,
    "supports_system_versioned_tables": False,
}

MYSQL_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "VARCHAR(36)",
    "boolean": "TINYINT(1)",
    "timestamp": "TIMESTAMP",
    "text": "TEXT",
    "blob": "BLOB",
    "json": "JSON",
}


MYSQL_CONFIG = DialectConfig(
    name="mysql",
    feature_versions=MYSQL_FEATURE_VERSIONS,
    feature_flags=MYSQL_FEATURE_FLAGS,
    type_mappings=MYSQL_TYPE_MAPPINGS,
    version_pattern=MYSQL_VERSION_PATTERN,
)

register_dialect(MYSQL_CONFIG)


MARIADB_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(10, 2, 7),
    "supports_cte": VersionInfo(10, 2, 1),
    "supports_window_functions": VersionInfo(10, 2, 0),
    "supports_skip_locked": VersionInfo(10, 6, 0),
    "supports_generated_columns": VersionInfo(10, 2, 0),
    "supports_check_constraints": VersionInfo(10, 2, 1),
    "supports_roles": VersionInfo(10, 0, 5),
    "supports_events": VersionInfo(5, 1, 6),
    "supports_sequences": VersionInfo(10, 3, 0),
    "supports_system_versioned_tables": VersionInfo(10, 3, 0),
}

MARIADB_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_returning": True,
    "supports_upsert": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_arrays": False,
    "supports_uuid": False,
    "supports_for_update": True,
    "supports_invisible_columns": False,
    "supports_invisible_indexes": False,
    "supports_resource_groups": False,
}

MARIADB_CONFIG = DialectConfig(
    name="mariadb",
    feature_versions=MARIADB_FEATURE_VERSIONS,
    feature_flags=MARIADB_FEATURE_FLAGS,
    type_mappings=MYSQL_TYPE_MAPPINGS,
    version_pattern=MYSQL_VERSION_PATTERN,
)

register_dialect(MARIADB_CONFIG)


class MySQLEngineVersion:
    """Parsed MySQL-family server version with engine and vendor markers."""

    __slots__ = ("engine_family", "raw_version", "variant_markers", "version")

    def __init__(
        self, *, engine_family: str, version: VersionInfo, raw_version: str, variant_markers: "tuple[str, ...]" = ()
    ) -> None:
        self.engine_family = engine_family
        self.version = version
        self.raw_version = raw_version
        self.variant_markers = variant_markers

    def __repr__(self) -> str:
        return (
            "MySQLEngineVersion("
            f"engine_family={self.engine_family!r}, version={self.version!r}, "
            f"raw_version={self.raw_version!r}, variant_markers={self.variant_markers!r})"
        )


MYSQL_METADATA_DOMAINS: "tuple[str, ...]" = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "triggers",
    "events",
    "privileges",
    "plugins",
    "partitions",
    "ddl",
    "system",
)


def parse_mysql_engine_version(version_text: str) -> MySQLEngineVersion | None:
    """Parse a MySQL-family version string into engine, version, and variant markers."""
    raw_version = version_text.strip()
    if not raw_version:
        return None

    engine_family = "mariadb" if "mariadb" in raw_version.lower() else "mysql"
    match_iter = MYSQL_VERSION_PATTERN.finditer(raw_version)
    match = next(match_iter, None)
    if match is None:
        return None
    if engine_family == "mariadb" and raw_version.startswith("5.5.5-"):
        match = next(match_iter, match)
    version = VersionInfo(int(match.group(1)), int(match.group(2)), int(match.group(3)))

    suffix = raw_version[match.end() :]
    markers = tuple(marker for marker in re.split(r"[-+~\s]+", suffix) if marker)
    return MySQLEngineVersion(
        engine_family=engine_family, version=version, raw_version=raw_version, variant_markers=markers
    )


def build_mysql_metadata_capability_profile(
    dialect: str, adapter: str | None, domains: "tuple[str, ...] | None" = None
) -> MetadataCapabilityProfile:
    """Build the replacement metadata capability profile for MySQL-family adapters."""
    requested_domains = MYSQL_METADATA_DOMAINS if domains is None else domains
    capabilities = tuple(_mysql_metadata_capability(domain) for domain in requested_domains)
    return MetadataCapabilityProfile(dialect=dialect, adapter=adapter, capabilities=capabilities)


def format_mysql_identifier(identifier: str) -> str:
    """Format a possibly-qualified MySQL-family identifier with backtick quoting."""
    cleaned = identifier.strip()
    parts = split_qualified_identifier(cleaned, quote_chars="`", allow_bracket_quotes=False)
    formatted = ".".join(quote_backtick_identifier(part) for part in parts)
    return formatted or quote_backtick_identifier(cleaned)


def build_mysql_show_create_statement(object_name: str, schema: str | None = None, object_type: str = "TABLE") -> str:
    """Build a SHOW CREATE statement using quoted identifiers for object names."""
    qualified_name = f"{schema}.{object_name}" if schema else object_name
    return f"SHOW CREATE {object_type.upper()} {format_mysql_identifier(qualified_name)}"


def make_mysql_ddl_result(
    object_name: str,
    schema: str | None,
    object_type: str,
    ddl_row: "dict[str, object]",
    raw_version: str | None,
    sql_mode: str | None,
    sql_quote_show_create: str | None,
) -> DDLResult:
    """Create a DDLResult from a SHOW CREATE row and replay-sensitive session context."""
    parsed_version = parse_mysql_engine_version(raw_version or "")
    engine_family = parsed_version.engine_family if parsed_version is not None else "mysql"
    server_version = str(parsed_version.version) if parsed_version is not None else raw_version
    ddl_text = _extract_show_create_text(ddl_row, object_type)
    identity = ObjectIdentity(
        name=object_name,
        object_type=object_type.lower(),
        schema=schema,
        dialect=engine_family,
        quoted_name=format_mysql_identifier(f"{schema}.{object_name}" if schema else object_name),
        source=MetadataSource.NATIVE_API,
    )
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.NATIVE_API,
        ddl=ddl_text,
        context={
            "engine_family": engine_family,
            "server_version": server_version,
            "sql_mode": sql_mode,
            "sql_quote_show_create": sql_quote_show_create,
        },
        warnings=("DDL output is environment-sensitive; replay with captured SQL mode and quote settings.",),
    )


def _extract_show_create_text(ddl_row: "dict[str, object]", object_type: str) -> str | None:
    expected_key = f"Create {object_type.title()}"
    value = ddl_row.get(expected_key)
    if value is not None:
        return str(value)
    for key, candidate in ddl_row.items():
        if str(key).lower().startswith("create "):
            return str(candidate)
    return None


def _mysql_metadata_capability(domain: str) -> MetadataCapability:
    if domain in {
        "schemas",
        "objects",
        "tables",
        "columns",
        "constraints",
        "indexes",
        "views",
        "routines",
        "triggers",
        "events",
        "privileges",
        "plugins",
        "partitions",
    }:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.INFORMATION_SCHEMA,
        )
    if domain == "ddl":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.NATIVE_API,
            warnings=("SHOW CREATE output depends on SQL mode and quote settings.",),
        )
    if domain == "system":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
            warnings=("Sensitive system metadata requires explicit opt-in.",),
        )
    return MetadataCapability.unsupported(domain)


def build_mysql_system_metadata_capability(domain: str) -> SystemMetadataCapability:
    """Build system metadata capability disclosures for MySQL-family adapters."""
    if domain in {"performance_schema_tables", "sys_schema_table_statistics", "table_statistics"}:
        return SystemMetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
            redaction_fields=("user", "host", "setting", "sql_text"),
            warnings=("Sensitive system metadata requires explicit opt-in.",),
        )
    return SystemMetadataCapability.unsupported(domain, source=MetadataSource.SYSTEM_VIEW)


def mysql_system_metadata_query_name(domain: str) -> str | None:
    """Map public MySQL system metadata domains to query-pack names."""
    if domain in {"table_statistics", "sys_schema_table_statistics"}:
        return "sys_schema_table_statistics"
    if domain == "performance_schema_tables":
        return "performance_schema_tables"
    return None


def resolve_mysql_json_type(version_info: "VersionInfo | None") -> str:
    """Resolve the best MySQL JSON storage type for a database version."""
    json_version = MYSQL_CONFIG.get_feature_version("supports_json")
    if version_info and json_version and version_info >= json_version:
        return "JSON"
    return "TEXT"
