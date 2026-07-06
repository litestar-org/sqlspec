import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.data_dictionary import (
    DDLResult,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    SystemMetadata,
    register_dialect,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary import TableMetadata, VersionInfo

__all__ = (
    "build_mssql_metadata_capability_profile",
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
    """Build SQL Server replacement data-dictionary capability metadata."""
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


def mssql_system_metadata_denied(*warnings: str) -> MetadataResult:
    """Return a permission-aware unsupported system metadata result."""
    capability = MetadataCapability(
        domain="system",
        support=MetadataSupport.UNSUPPORTED,
        fidelity=MetadataFidelity.UNSUPPORTED,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
        warnings=warnings,
    )
    return MetadataResult(domain="system", capability=capability, warnings=warnings)


def validate_mssql_system_metadata_options(domain: str, options: dict[str, Any]) -> MetadataResult | None:
    """Return an unsupported result when SQL Server system metadata gates fail."""
    if not options.get("enable_system_metadata"):
        return mssql_system_metadata_denied(MSSQL_SYSTEM_OPT_IN_WARNING)
    if domain.startswith("query_store") and not options.get("query_store"):
        return mssql_system_metadata_denied(MSSQL_QUERY_STORE_PERMISSION_WARNING)
    if domain.startswith("dmv") and not (
        options.get("view_server_state") or options.get("view_server_performance_state")
    ):
        return mssql_system_metadata_denied(MSSQL_SYSTEM_PERMISSION_WARNING)
    return None


def build_mssql_system_metadata_result(
    domain: str, rows: "Sequence[Any]", *, include_sensitive: bool = False
) -> MetadataResult:
    """Build redacted SQL Server system metadata rows."""
    capability = MetadataCapability(
        domain="system",
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.PARTIAL,
        source=MetadataSource.SYSTEM_VIEW,
        risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
        warnings=(MSSQL_SYSTEM_WARNING,),
    )
    items = []
    for position, row in enumerate(rows):
        attributes: dict[str, object] = dict(_row_to_dict(row))
        if not include_sensitive:
            redacted: dict[str, object] = {}
            for key, value in attributes.items():
                redacted[key] = MSSQL_REDACTED_VALUE if key.lower() in MSSQL_SENSITIVE_SYSTEM_KEYS else value
            attributes = redacted
        identity = ObjectIdentity(
            name=f"{domain}:{position + 1}",
            object_type="system_metadata",
            dialect="mssql",
            source=MetadataSource.SYSTEM_VIEW,
        )
        items.append(SystemMetadata(identity, source=MetadataSource.SYSTEM_VIEW, attributes=attributes))
    return MetadataResult(domain="system", capability=capability, items=tuple(items), warnings=capability.warnings)


def build_mssql_table_ddl_result(
    schema_name: str | None, table_name: str, column_rows: "Sequence[Any]", index_rows: "Sequence[Any] | None" = None
) -> MetadataResult:
    """Generate SQL Server table DDL from structured catalog rows."""
    schema = schema_name or MSSQL_CONFIG.default_schema
    identity = ObjectIdentity(
        name=table_name,
        object_type="table",
        schema=schema,
        dialect="mssql",
        quoted_name=_mssql_qualified_name(schema, table_name),
        source=MetadataSource.GENERATED,
    )
    if not column_rows:
        capability = MetadataCapability(
            domain="ddl",
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.GENERATED,
            warnings=(f"No SQL Server catalog columns found for {schema or '<default>'}.{table_name}",),
        )
        ddl = DDLResult(
            identity, MetadataSupport.UNSUPPORTED, source=MetadataSource.GENERATED, warnings=capability.warnings
        )
        return MetadataResult(domain="ddl", capability=capability, items=(ddl,), warnings=capability.warnings)

    rows = [_row_to_dict(row) for row in column_rows]
    primary_key_name = _first_non_empty(row.get("primary_key_name") for row in rows)
    primary_columns = [
        row for row in rows if row.get("primary_key_ordinal") is not None and str(row.get("primary_key_ordinal")) != "0"
    ]
    primary_columns.sort(key=lambda row: int(cast("Any", row.get("primary_key_ordinal") or 0)))

    ddl_lines = [_render_mssql_column_definition(row) for row in sorted(rows, key=_column_ordinal)]
    if primary_key_name and primary_columns:
        columns = ", ".join(_quote_mssql_identifier(str(row["column_name"])) for row in primary_columns)
        ddl_lines.append(f"CONSTRAINT {_quote_mssql_identifier(str(primary_key_name))} PRIMARY KEY ({columns})")

    table_ddl = "CREATE TABLE " + _mssql_qualified_name(schema, table_name) + " (\n"
    table_ddl += ",\n".join(f"    {line}" for line in ddl_lines)
    table_ddl += "\n);"

    index_statements = [
        _render_mssql_index_definition(_row_to_dict(row), schema, table_name) for row in index_rows or ()
    ]
    ddl_text = "\n\n".join(statement for statement in (table_ddl, *index_statements) if statement)
    capability = MetadataCapability(
        domain="ddl",
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.GENERATED,
        source=MetadataSource.GENERATED,
        warnings=(MSSQL_DDL_WARNING,),
    )
    ddl = DDLResult(
        identity,
        MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.GENERATED,
        source=MetadataSource.GENERATED,
        ddl=ddl_text,
        warnings=capability.warnings,
    )
    return MetadataResult(domain="ddl", capability=capability, items=(ddl,), warnings=capability.warnings)


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def _column_ordinal(row: dict[str, Any]) -> int:
    try:
        return int(cast("Any", row.get("ordinal_position") or 0))
    except (TypeError, ValueError):
        return 0


def _quote_mssql_identifier(value: str) -> str:
    return f"[{value.replace(']', ']]')}]"


def _mssql_qualified_name(schema: str | None, name: str) -> str:
    if schema:
        return f"{_quote_mssql_identifier(schema)}.{_quote_mssql_identifier(name)}"
    return _quote_mssql_identifier(name)


def _first_non_empty(values: Any) -> Any:
    for value in values:
        if value:
            return value
    return None


def _render_mssql_column_definition(row: dict[str, Any]) -> str:
    column_name = _quote_mssql_identifier(str(row["column_name"]))
    if _truthy(row.get("is_computed")) and row.get("computed_definition"):
        definition = f"{column_name} AS {row['computed_definition']}"
        if _truthy(row.get("is_persisted_computed")):
            definition += " PERSISTED"
        return definition

    parts = [column_name, _render_mssql_type(row)]
    if _truthy(row.get("is_identity")):
        seed = row.get("identity_seed") or 1
        increment = row.get("identity_increment") or 1
        parts.append(f"IDENTITY({seed},{increment})")
    if row.get("column_default"):
        parts.append(f"DEFAULT {row['column_default']}")
    parts.append("NULL" if _truthy(row.get("is_nullable")) else "NOT NULL")
    return " ".join(parts)


def _render_mssql_type(row: dict[str, Any]) -> str:
    data_type = str(row.get("data_type") or "sql_variant")
    normalized = data_type.lower()
    max_length = row.get("max_length")
    precision = row.get("numeric_precision")
    scale = row.get("numeric_scale")
    if normalized in {"nchar", "nvarchar"}:
        if max_length in {-1, "-1"}:
            return f"{data_type}(MAX)"
        if max_length is not None:
            return f"{data_type}({int(cast('Any', max_length)) // 2})"
    if normalized in {"binary", "char", "varbinary", "varchar"}:
        if max_length in {-1, "-1"}:
            return f"{data_type}(MAX)"
        if max_length is not None:
            return f"{data_type}({max_length})"
    if normalized in {"decimal", "numeric"} and precision is not None and scale is not None:
        return f"{data_type}({precision},{scale})"
    if normalized in {"datetime2", "datetimeoffset", "time"} and scale is not None:
        return f"{data_type}({scale})"
    return data_type


def _render_mssql_index_definition(row: dict[str, Any], schema: str | None, table_name: str) -> str | None:
    index_name = row.get("index_name")
    columns = _split_mssql_column_list(row.get("columns"))
    if not index_name or not columns:
        return None
    unique = "UNIQUE " if _truthy(row.get("is_unique")) else ""
    index_type = str(row.get("type_desc") or "NONCLUSTERED").replace("_", " ")
    statement = (
        f"CREATE {unique}{index_type} INDEX {_quote_mssql_identifier(str(index_name))} "
        f"ON {_mssql_qualified_name(schema, table_name)} ({', '.join(_quote_mssql_identifier(column) for column in columns)})"
    )
    included = _split_mssql_column_list(row.get("included_columns"))
    if included:
        statement += f" INCLUDE ({', '.join(_quote_mssql_identifier(column) for column in included)})"
    if _truthy(row.get("has_filter")) and row.get("filter_definition"):
        statement += f" WHERE {row['filter_definition']}"
    return statement + ";"


def _split_mssql_column_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, Sequence):
        return [str(part) for part in value if str(part)]
    return [str(value)]
