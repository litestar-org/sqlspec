from enum import Enum
from typing import TYPE_CHECKING, Any, TypedDict, cast

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from collections.abc import Mapping
    from re import Pattern

    from sqlspec.core.statement import SQL
    from sqlspec.data_dictionary._dependencies import DependencyEdge

__all__ = (
    "ColumnDetails",
    "ColumnMetadata",
    "CommentMetadata",
    "ConstraintMetadata",
    "DDLResult",
    "DependencyMetadata",
    "DialectConfig",
    "FeatureFlags",
    "FeatureVersions",
    "ForeignKeyMetadata",
    "IndexDetails",
    "IndexMetadata",
    "MetadataCapability",
    "MetadataCapabilityProfile",
    "MetadataFidelity",
    "MetadataQuery",
    "MetadataResult",
    "MetadataRisk",
    "MetadataSource",
    "MetadataSupport",
    "ObjectIdentity",
    "ObjectMetadata",
    "PartitionMetadata",
    "PrivilegeMetadata",
    "RoutineMetadata",
    "SchemaMetadata",
    "SystemMetadata",
    "SystemMetadataCapability",
    "SystemMetadataRedactionPolicy",
    "SystemMetadataRequest",
    "SystemMetadataResult",
    "TableDetails",
    "TableMetadata",
    "TableStatisticsMetadata",
    "TriggerMetadata",
    "VersionCacheResult",
    "VersionInfo",
    "ViewMetadata",
)


class MetadataSupport(str, Enum):
    """Support status for a metadata domain."""

    GATED = "gated"
    NOT_IMPLEMENTED = "not_implemented"
    SUPPORTED = "supported"
    UNKNOWN = "unknown"
    UNSUPPORTED = "unsupported"


class MetadataFidelity(str, Enum):
    """Fidelity of a metadata response."""

    GENERATED = "generated"
    HYBRID = "hybrid"
    LOSSY = "lossy"
    NATIVE = "native"
    PARTIAL = "partial"
    TRANSPORT_FALLBACK = "transport_fallback"
    UNSUPPORTED = "unsupported"


class MetadataRisk(str, Enum):
    """Risk or access gate attached to a metadata domain."""

    BILLED = "billed"
    EXPENSIVE = "expensive"
    EXTENSION_REQUIRED = "extension_required"
    LICENSE_GATED = "license_gated"
    MANAGED_SERVICE_LIMITED = "managed_service_limited"
    PRIVILEGED = "privileged"
    REDACTED = "redacted"
    VERSION_GATED = "version_gated"


class MetadataSource(str, Enum):
    """Source used to produce a metadata response."""

    CATALOG = "catalog"
    DRIVER_METADATA = "driver_metadata"
    GENERATED = "generated"
    INFORMATION_SCHEMA = "information_schema"
    NATIVE_API = "native_api"
    PARSED_SQL = "parsed_sql"
    SYSTEM_VIEW = "system_view"
    UNKNOWN = "unknown"


class ObjectIdentity:
    """Stable identity for a database object."""

    __slots__ = ("catalog", "dialect", "name", "object_type", "quoted_name", "schema", "source")

    def __init__(
        self,
        name: str,
        object_type: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        dialect: str | None = None,
        quoted_name: str | None = None,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.object_type = object_type
        self.dialect = dialect
        self.quoted_name = quoted_name
        self.source = _coerce_source(source)

    def to_dict(self) -> "dict[str, str | None]":
        """Serialize the identity with stable string enum values."""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "name": self.name,
            "object_type": self.object_type,
            "dialect": self.dialect,
            "quoted_name": self.quoted_name,
            "source": self.source.value,
        }

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "ObjectIdentity":
        """Create an identity from a serialized payload."""
        return cls(
            name=str(data["name"]),
            object_type=str(data["object_type"]),
            catalog=cast("str | None", data.get("catalog")),
            schema=cast("str | None", data.get("schema")),
            dialect=cast("str | None", data.get("dialect")),
            quoted_name=cast("str | None", data.get("quoted_name")),
            source=cast("str", data.get("source", MetadataSource.UNKNOWN.value)),
        )

    def __repr__(self) -> str:
        return (
            f"ObjectIdentity(name={self.name!r}, object_type={self.object_type!r}, catalog={self.catalog!r}, "
            f"schema={self.schema!r}, dialect={self.dialect!r}, quoted_name={self.quoted_name!r}, "
            f"source={self.source!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectIdentity):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((
            self.catalog,
            self.schema,
            self.name,
            self.object_type,
            self.dialect,
            self.quoted_name,
            self.source,
        ))

    def __reduce__(self) -> "tuple[type[ObjectIdentity], tuple[str, str], dict[str, str | None | MetadataSource]]":
        return (
            self.__class__,
            (self.name, self.object_type),
            {
                "catalog": self.catalog,
                "schema": self.schema,
                "dialect": self.dialect,
                "quoted_name": self.quoted_name,
                "source": self.source,
            },
        )


class MetadataCapability:
    """Capability report for one metadata domain."""

    __slots__ = ("domain", "fidelity", "risks", "source", "support", "warnings")

    def __init__(
        self,
        domain: str,
        support: "MetadataSupport | str",
        *,
        fidelity: "MetadataFidelity | str" = MetadataFidelity.UNSUPPORTED,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        risks: "tuple[MetadataRisk | str, ...]" = (),
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.domain = domain
        self.support = _coerce_support(support)
        self.fidelity = _coerce_fidelity(fidelity)
        self.source = _coerce_source(source)
        self.risks = tuple(_coerce_risk(risk) for risk in risks)
        self.warnings = warnings

    def to_dict(self) -> "dict[str, str | tuple[str, ...]]":
        """Serialize the capability with stable string enum values."""
        return {
            "domain": self.domain,
            "support": self.support.value,
            "fidelity": self.fidelity.value,
            "source": self.source.value,
            "risks": tuple(risk.value for risk in self.risks),
            "warnings": self.warnings,
        }

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "MetadataCapability":
        """Create a capability from a serialized payload."""
        return cls(
            domain=str(data["domain"]),
            support=cast("str", data["support"]),
            fidelity=cast("str", data.get("fidelity", MetadataFidelity.UNSUPPORTED.value)),
            source=cast("str", data.get("source", MetadataSource.UNKNOWN.value)),
            risks=tuple(cast("tuple[str, ...]", data.get("risks", ()))),
            warnings=tuple(cast("tuple[str, ...]", data.get("warnings", ()))),
        )

    @classmethod
    def unsupported(
        cls, domain: str, *, source: "MetadataSource | str" = MetadataSource.UNKNOWN
    ) -> "MetadataCapability":
        """Create a standard unsupported-domain capability."""
        return cls(domain=domain, support=MetadataSupport.UNSUPPORTED, source=source)

    def __repr__(self) -> str:
        return (
            f"MetadataCapability(domain={self.domain!r}, support={self.support!r}, "
            f"fidelity={self.fidelity!r}, source={self.source!r}, risks={self.risks!r}, "
            f"warnings={self.warnings!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetadataCapability):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.domain, self.support, self.fidelity, self.source, self.risks, self.warnings))


class MetadataCapabilityProfile:
    """Capability report for one adapter and dialect."""

    __slots__ = ("adapter", "capabilities", "dialect")

    def __init__(
        self, dialect: str, *, adapter: str | None = None, capabilities: "tuple[MetadataCapability, ...]" = ()
    ) -> None:
        self.dialect = dialect
        self.adapter = adapter
        self.capabilities = capabilities

    def get(self, domain: str) -> MetadataCapability:
        """Return capability for a domain, or unknown when it has not been reported."""
        for capability in self.capabilities:
            if capability.domain == domain:
                return capability
        return MetadataCapability(domain=domain, support=MetadataSupport.UNKNOWN)

    def to_dict(self) -> "dict[str, str | None | tuple[dict[str, str | tuple[str, ...]], ...]]":
        """Serialize the profile with stable string enum values."""
        return {
            "dialect": self.dialect,
            "adapter": self.adapter,
            "capabilities": tuple(capability.to_dict() for capability in self.capabilities),
        }

    @classmethod
    def from_domains(cls, dialect: str, adapter: str | None, domains: "tuple[str, ...]") -> "MetadataCapabilityProfile":
        """Create an unsupported profile for domains without implementation."""
        return cls(
            dialect=dialect,
            adapter=adapter,
            capabilities=tuple(MetadataCapability.unsupported(domain) for domain in domains),
        )

    def __repr__(self) -> str:
        return (
            f"MetadataCapabilityProfile(dialect={self.dialect!r}, adapter={self.adapter!r}, "
            f"capabilities={self.capabilities!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetadataCapabilityProfile):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.dialect, self.adapter, self.capabilities))


class MetadataResult:
    """Uniform result envelope for metadata domain lookups."""

    __slots__ = ("capability", "domain", "items", "warnings")

    def __init__(
        self,
        domain: str,
        *,
        capability: MetadataCapability | None = None,
        items: "tuple[object, ...]" = (),
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.domain = domain
        self.capability = capability or MetadataCapability(
            domain=domain, support=MetadataSupport.SUPPORTED, fidelity=MetadataFidelity.NATIVE
        )
        self.items = items
        self.warnings = warnings

    @classmethod
    def unsupported(cls, domain: str, *, source: "MetadataSource | str" = MetadataSource.UNKNOWN) -> "MetadataResult":
        """Create a standard unsupported-domain result."""
        capability = MetadataCapability.unsupported(domain, source=source)
        return cls(domain=domain, capability=capability, warnings=capability.warnings)

    def to_dict(self) -> "dict[str, object]":
        """Serialize the result envelope."""
        return {
            "domain": self.domain,
            "capability": self.capability.to_dict(),
            "items": tuple(_serialize_metadata_item(item) for item in self.items),
            "warnings": self.warnings,
        }

    def __repr__(self) -> str:
        return (
            f"MetadataResult(domain={self.domain!r}, capability={self.capability!r}, "
            f"items={self.items!r}, warnings={self.warnings!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetadataResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.domain, self.capability, self.items, self.warnings))


class MetadataQuery:
    """Loaded data-dictionary query plus capability status."""

    __slots__ = ("capability", "dialect", "domain", "mode", "name", "sql", "warnings")

    def __init__(
        self,
        dialect: str,
        domain: str,
        name: str,
        *,
        sql: "SQL | None" = None,
        mode: str | None = None,
        capability: MetadataCapability | None = None,
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.dialect = dialect
        self.domain = domain
        self.name = name
        self.mode = mode
        self.sql = sql
        self.capability = capability or MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
        )
        self.warnings = warnings or self.capability.warnings

    @property
    def is_supported(self) -> bool:
        """Return whether the query is available and executable."""
        return self.sql is not None and self.capability.support == MetadataSupport.SUPPORTED

    @property
    def query_text(self) -> str | None:
        """Return raw SQL text when the query is supported."""
        if self.sql is None:
            return None
        return self.sql.raw_sql

    @classmethod
    def unsupported(
        cls,
        dialect: str,
        domain: str,
        name: str,
        *,
        mode: str | None = None,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        risks: "tuple[MetadataRisk | str, ...]" = (),
        warnings: "tuple[str, ...]" = (),
    ) -> "MetadataQuery":
        """Create a structured unsupported query result."""
        capability = MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=source,
            risks=risks,
            warnings=warnings,
        )
        return cls(dialect=dialect, domain=domain, name=name, mode=mode, capability=capability, warnings=warnings)

    def to_dict(self) -> "dict[str, object]":
        """Serialize the query status."""
        return {
            "dialect": self.dialect,
            "domain": self.domain,
            "name": self.name,
            "mode": self.mode,
            "capability": self.capability.to_dict(),
            "query_text": self.query_text,
            "warnings": self.warnings,
        }

    def __repr__(self) -> str:
        return (
            f"MetadataQuery(dialect={self.dialect!r}, domain={self.domain!r}, name={self.name!r}, "
            f"mode={self.mode!r}, capability={self.capability!r}, sql={self.sql!r}, warnings={self.warnings!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetadataQuery):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.dialect, self.domain, self.name, self.mode, self.capability, self.query_text, self.warnings))


class DDLResult:
    """DDL text and fidelity status for one database object."""

    __slots__ = ("context", "ddl", "dependencies", "fidelity", "identity", "redactions", "source", "status", "warnings")

    def __init__(
        self,
        identity: ObjectIdentity,
        status: "MetadataSupport | str",
        *,
        fidelity: "MetadataFidelity | str" = MetadataFidelity.UNSUPPORTED,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        ddl: str | None = None,
        dependencies: "tuple[DependencyEdge, ...]" = (),
        redactions: "tuple[str, ...]" = (),
        context: "dict[str, object] | None" = None,
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.identity = identity
        self.status = _coerce_support(status)
        self.fidelity = _coerce_fidelity(fidelity)
        self.source = _coerce_source(source)
        self.ddl = ddl
        self.dependencies = dependencies
        self.redactions = redactions
        self.context = {} if context is None else context
        self.warnings = warnings

    @classmethod
    def unsupported(
        cls,
        identity: ObjectIdentity,
        *,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        warnings: "tuple[str, ...]" = (),
        context: "dict[str, object] | None" = None,
    ) -> "DDLResult":
        """Create an explicit unsupported DDL result."""
        return cls(
            identity,
            status=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=source,
            ddl=None,
            context=context,
            warnings=warnings,
        )

    @classmethod
    def lossy(
        cls,
        identity: ObjectIdentity,
        *,
        ddl: str | None,
        source: "MetadataSource | str",
        dependencies: "tuple[DependencyEdge, ...]" = (),
        redactions: "tuple[str, ...]" = (),
        context: "dict[str, object] | None" = None,
        warnings: "tuple[str, ...]" = (),
    ) -> "DDLResult":
        """Create an explicit lossy DDL result."""
        return cls(
            identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.LOSSY,
            source=source,
            ddl=ddl,
            dependencies=dependencies,
            redactions=redactions,
            context=context,
            warnings=warnings,
        )

    def to_dict(self) -> "dict[str, object]":
        """Serialize the DDL result."""
        return {
            "identity": self.identity.to_dict(),
            "status": self.status.value,
            "fidelity": self.fidelity.value,
            "source": self.source.value,
            "ddl": self.ddl,
            "dependencies": tuple(_serialize_metadata_item(dependency) for dependency in self.dependencies),
            "redactions": self.redactions,
            "context": self.context,
            "warnings": self.warnings,
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DDLResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((
            self.identity,
            self.status,
            self.fidelity,
            self.source,
            self.ddl,
            self.dependencies,
            self.redactions,
            _hashable_mapping(self.context),
            self.warnings,
        ))


class ObjectMetadata:
    """Generic rich metadata for a database object."""

    __slots__ = ("attributes", "identity", "source")

    def __init__(
        self,
        identity: ObjectIdentity,
        *,
        source: "MetadataSource | str | None" = None,
        attributes: "dict[str, object] | None" = None,
    ) -> None:
        self.identity = identity
        self.source = identity.source if source is None else _coerce_source(source)
        self.attributes = {} if attributes is None else attributes

    def to_dict(self) -> "dict[str, object]":
        """Serialize generic object metadata."""
        return {"identity": self.identity.to_dict(), "source": self.source.value, "attributes": self.attributes}


class SchemaMetadata(ObjectMetadata):
    """Metadata for a schema, database, catalog, project, or dataset."""

    __slots__ = ()


class ColumnDetails(ObjectMetadata):
    """Rich metadata for a column."""

    __slots__ = ()


class ConstraintMetadata(ObjectMetadata):
    """Rich metadata for a constraint."""

    __slots__ = ()


class IndexDetails(ObjectMetadata):
    """Rich metadata for an index."""

    __slots__ = ()


class ViewMetadata(ObjectMetadata):
    """Rich metadata for a view or materialized view."""

    __slots__ = ()


class RoutineMetadata(ObjectMetadata):
    """Rich metadata for a routine, function, procedure, or package member."""

    __slots__ = ()


class TriggerMetadata(ObjectMetadata):
    """Rich metadata for a trigger or database event."""

    __slots__ = ()


class CommentMetadata(ObjectMetadata):
    """Rich metadata for comments, labels, tags, or extended properties."""

    __slots__ = ()


class PrivilegeMetadata(ObjectMetadata):
    """Rich metadata for a grant, role edge, or privilege."""

    __slots__ = ()


class DependencyMetadata(ObjectMetadata):
    """Rich metadata for an object dependency edge."""

    __slots__ = ()


class PartitionMetadata(ObjectMetadata):
    """Rich metadata for partition, clustering, storage, or table options."""

    __slots__ = ()


_REDACTED_VALUE = "[REDACTED]"
_SQL_TEXT_FIELDS = {
    "current_query",
    "query",
    "query_text",
    "sql",
    "sql_fulltext",
    "sql_text",
    "statement",
    "statement_text",
}
_USER_FIELDS = {
    "current_user",
    "login",
    "login_name",
    "owner",
    "principal",
    "principal_name",
    "session_user",
    "user",
    "user_name",
    "username",
    "usename",
}
_HOST_FIELDS = {
    "client_addr",
    "client_host",
    "client_hostname",
    "host",
    "hostname",
    "ip_address",
    "machine",
    "program_host",
    "remote_addr",
}
_SETTING_VALUE_FIELDS = {"option_value", "parameter_value", "setting_value", "variable_value"}
_SETTING_NAME_FIELDS = {"name", "option_name", "parameter_name", "setting_name", "variable_name"}
_CONNECTION_STRING_FIELDS = {"connection_string", "database_url", "dsn", "jdbc_url", "url", "uri"}
_GRANT_FIELDS = {"grant", "grant_sql", "grant_statement", "grantee", "grantor", "grants", "permission", "permissions"}
_SENSITIVE_NAME_FRAGMENTS = {
    "api_key",
    "auth",
    "credential",
    "database_url",
    "dsn",
    "jwt",
    "key",
    "oauth",
    "passwd",
    "password",
    "secret",
    "token",
}


def _normalize_redaction_key(key: str) -> str:
    return key.strip().lower().replace("-", "_")


def _is_sensitive_name(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = _normalize_redaction_key(value)
    return any(fragment in normalized for fragment in _SENSITIVE_NAME_FRAGMENTS)


def _row_setting_name(row: "Mapping[str, object]") -> object | None:
    for key, value in row.items():
        if _normalize_redaction_key(str(key)) in _SETTING_NAME_FIELDS:
            return value
    return None


def _looks_like_connection_string(value: object) -> bool:
    if not isinstance(value, str):
        return False
    lowered = value.lower()
    if "://" in value and ("@" in value or "password" in lowered or "token" in lowered):
        return True
    if "password=" in lowered or "pwd=" in lowered:
        return True
    return "user id=" in lowered and ("server=" in lowered or "host=" in lowered)


class SystemMetadataRedactionPolicy:
    """Redaction policy for system and performance metadata rows.

    Args:
        redact_sql_text: Redact SQL, query, and statement text fields.
        redact_users: Redact user, owner, login, and principal fields.
        redact_hosts: Redact host, address, and machine fields.
        redact_settings: Redact settings likely to contain secrets.
        redact_connection_strings: Redact connection string and URL values.
        redact_grants: Redact grant and permission fields.
    """

    __slots__ = (
        "redact_connection_strings",
        "redact_grants",
        "redact_hosts",
        "redact_settings",
        "redact_sql_text",
        "redact_users",
    )

    def __init__(
        self,
        *,
        redact_sql_text: bool = True,
        redact_users: bool = True,
        redact_hosts: bool = True,
        redact_settings: bool = True,
        redact_connection_strings: bool = True,
        redact_grants: bool = True,
    ) -> None:
        self.redact_sql_text = redact_sql_text
        self.redact_users = redact_users
        self.redact_hosts = redact_hosts
        self.redact_settings = redact_settings
        self.redact_connection_strings = redact_connection_strings
        self.redact_grants = redact_grants

    @classmethod
    def unredacted(cls) -> "SystemMetadataRedactionPolicy":
        """Create a policy that leaves system metadata rows unchanged."""
        return cls(
            redact_sql_text=False,
            redact_users=False,
            redact_hosts=False,
            redact_settings=False,
            redact_connection_strings=False,
            redact_grants=False,
        )

    @classmethod
    def sensitive_field_names(cls) -> "tuple[str, ...]":
        """Return the canonical sensitive field categories covered by the default policy."""
        return ("sql_text", "user", "host", "setting", "connection_string", "grant")

    def redact_row(self, row: "Mapping[str, object]") -> "tuple[dict[str, object], tuple[str, ...]]":
        """Return a redacted row and the fields that were changed.

        Args:
            row: Metadata row to redact.

        Returns:
            A tuple of the redacted row and sorted redacted field names.
        """
        redacted: dict[str, object] = {}
        fields: set[str] = set()
        for key, value in row.items():
            field_name = str(key)
            if self._should_redact_field(field_name, value, row):
                redacted[field_name] = _REDACTED_VALUE
                fields.add(field_name)
                continue
            redacted[field_name] = value
        return redacted, tuple(sorted(fields))

    def _should_redact_field(self, key: str, value: object, row: "Mapping[str, object]") -> bool:
        normalized = _normalize_redaction_key(key)
        if self.redact_sql_text and (
            normalized in _SQL_TEXT_FIELDS or normalized.endswith(("_sql", "_query", "_statement"))
        ):
            return True
        if self.redact_users and normalized in _USER_FIELDS:
            return True
        if self.redact_hosts and normalized in _HOST_FIELDS:
            return True
        if self.redact_connection_strings and (
            normalized in _CONNECTION_STRING_FIELDS or _looks_like_connection_string(value)
        ):
            return True
        if self.redact_settings and (
            _is_sensitive_name(normalized)
            or normalized in _SETTING_VALUE_FIELDS
            or (normalized == "value" and _is_sensitive_name(_row_setting_name(row)))
        ):
            return True
        return self.redact_grants and normalized in _GRANT_FIELDS

    def to_dict(self) -> "dict[str, bool]":
        """Serialize redaction settings."""
        return {
            "redact_sql_text": self.redact_sql_text,
            "redact_users": self.redact_users,
            "redact_hosts": self.redact_hosts,
            "redact_settings": self.redact_settings,
            "redact_connection_strings": self.redact_connection_strings,
            "redact_grants": self.redact_grants,
        }


class SystemMetadataRequest:
    """Opt-in request for system and performance metadata.

    Args:
        domain: System metadata domain to query.
        include_system: Enable system metadata domains such as settings or sessions.
        include_performance: Enable performance metadata domains such as statistics or history.
        allow_billed_metadata: Permit domains that may incur cloud or service billing.
        allow_license_gated_diagnostics: Permit license-gated diagnostics such as AWR/ASH.
        include_sensitive: Disable default row redaction for this request.
        table: Optional table target for table-scoped metadata.
        schema: Optional schema target for table-scoped metadata.
        redaction_policy: Optional explicit row redaction policy.
    """

    __slots__ = (
        "allow_billed_metadata",
        "allow_license_gated_diagnostics",
        "domain",
        "include_performance",
        "include_sensitive",
        "include_system",
        "redaction_policy",
        "schema",
        "table",
    )

    def __init__(
        self,
        domain: str,
        *,
        include_system: bool = False,
        include_performance: bool = False,
        allow_billed_metadata: bool = False,
        allow_license_gated_diagnostics: bool = False,
        include_sensitive: bool = False,
        table: str | None = None,
        schema: str | None = None,
        redaction_policy: SystemMetadataRedactionPolicy | None = None,
        redact_sql_text: bool = True,
        redact_users: bool = True,
        redact_hosts: bool = True,
        redact_settings: bool = True,
        redact_connection_strings: bool = True,
        redact_grants: bool = True,
    ) -> None:
        self.domain = domain
        self.include_system = include_system
        self.include_performance = include_performance
        self.allow_billed_metadata = allow_billed_metadata
        self.allow_license_gated_diagnostics = allow_license_gated_diagnostics
        self.include_sensitive = include_sensitive
        self.table = table
        self.schema = schema
        if redaction_policy is not None:
            self.redaction_policy = redaction_policy
        elif include_sensitive:
            self.redaction_policy = SystemMetadataRedactionPolicy.unredacted()
        else:
            self.redaction_policy = SystemMetadataRedactionPolicy(
                redact_sql_text=redact_sql_text,
                redact_users=redact_users,
                redact_hosts=redact_hosts,
                redact_settings=redact_settings,
                redact_connection_strings=redact_connection_strings,
                redact_grants=redact_grants,
            )

    @property
    def is_enabled(self) -> bool:
        """Return whether either system or performance metadata was explicitly enabled."""
        return self.include_system or self.include_performance

    def to_dict(self) -> "dict[str, object]":
        """Serialize the request."""
        return {
            "domain": self.domain,
            "include_system": self.include_system,
            "include_performance": self.include_performance,
            "allow_billed_metadata": self.allow_billed_metadata,
            "allow_license_gated_diagnostics": self.allow_license_gated_diagnostics,
            "include_sensitive": self.include_sensitive,
            "table": self.table,
            "schema": self.schema,
            "redaction_policy": self.redaction_policy.to_dict(),
        }


class SystemMetadataCapability:
    """Capability disclosure for one system or performance metadata domain.

    Args:
        domain: System metadata domain name.
        support: Support status for the domain.
        fidelity: Fidelity of the domain response.
        source: Metadata source used for the domain.
        risks: Risk and gate categories attached to the domain.
        required_privileges: Required database privileges or roles.
        cost_implications: Billing or expensive-query disclosure.
        license_gate: License requirement disclosure.
        managed_service_restricted: Whether managed services commonly restrict this domain.
        redaction_fields: Sensitive fields that may appear in rows.
        warnings: Additional capability warnings.
    """

    __slots__ = (
        "cost_implications",
        "domain",
        "fidelity",
        "license_gate",
        "managed_service_restricted",
        "redaction_fields",
        "required_privileges",
        "risks",
        "source",
        "support",
        "warnings",
    )

    def __init__(
        self,
        domain: str,
        support: "MetadataSupport | str",
        *,
        fidelity: "MetadataFidelity | str" = MetadataFidelity.UNSUPPORTED,
        source: "MetadataSource | str" = MetadataSource.SYSTEM_VIEW,
        risks: "tuple[MetadataRisk | str, ...]" = (),
        required_privileges: "tuple[str, ...]" = (),
        cost_implications: str | None = None,
        license_gate: str | None = None,
        managed_service_restricted: bool = False,
        redaction_fields: "tuple[str, ...]" = (),
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.domain = domain
        self.support = _coerce_support(support)
        self.fidelity = _coerce_fidelity(fidelity)
        self.source = _coerce_source(source)
        self.risks = tuple(_coerce_risk(risk) for risk in risks)
        self.required_privileges = required_privileges
        self.cost_implications = cost_implications
        self.license_gate = license_gate
        self.managed_service_restricted = managed_service_restricted
        self.redaction_fields = redaction_fields
        self.warnings = warnings

    @classmethod
    def unsupported(
        cls, domain: str, *, source: "MetadataSource | str" = MetadataSource.SYSTEM_VIEW
    ) -> "SystemMetadataCapability":
        """Create a safe unsupported system-domain capability."""
        return cls(
            domain,
            MetadataSupport.UNSUPPORTED,
            source=source,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.REDACTED),
            redaction_fields=SystemMetadataRedactionPolicy.sensitive_field_names(),
            warnings=("No system metadata query pack is registered for this domain.",),
        )

    @property
    def requires_billed_opt_in(self) -> bool:
        """Return whether this domain requires billed metadata opt-in."""
        return MetadataRisk.BILLED in self.risks

    @property
    def requires_license_opt_in(self) -> bool:
        """Return whether this domain requires license-gated diagnostics opt-in."""
        return MetadataRisk.LICENSE_GATED in self.risks

    def with_support(
        self,
        support: "MetadataSupport | str",
        *,
        warnings: "tuple[str, ...]" = (),
        fidelity: "MetadataFidelity | str | None" = None,
    ) -> "SystemMetadataCapability":
        """Return a copy with a different support status and additional warnings."""
        return SystemMetadataCapability(
            self.domain,
            support,
            fidelity=self.fidelity if fidelity is None else fidelity,
            source=self.source,
            risks=self.risks,
            required_privileges=self.required_privileges,
            cost_implications=self.cost_implications,
            license_gate=self.license_gate,
            managed_service_restricted=self.managed_service_restricted,
            redaction_fields=self.redaction_fields,
            warnings=self.warnings + warnings,
        )

    def to_dict(self) -> "dict[str, object]":
        """Serialize the capability with stable enum values."""
        return {
            "domain": self.domain,
            "support": self.support.value,
            "fidelity": self.fidelity.value,
            "source": self.source.value,
            "risks": tuple(risk.value for risk in self.risks),
            "required_privileges": self.required_privileges,
            "cost_implications": self.cost_implications,
            "license_gate": self.license_gate,
            "managed_service_restricted": self.managed_service_restricted,
            "redaction_fields": self.redaction_fields,
            "warnings": self.warnings,
        }

    def __repr__(self) -> str:
        return (
            f"SystemMetadataCapability(domain={self.domain!r}, support={self.support!r}, "
            f"source={self.source!r}, risks={self.risks!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SystemMetadataCapability):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((
            self.domain,
            self.support,
            self.fidelity,
            self.source,
            self.risks,
            self.required_privileges,
            self.cost_implications,
            self.license_gate,
            self.managed_service_restricted,
            self.redaction_fields,
            self.warnings,
        ))


class SystemMetadataResult:
    """Result envelope for system and performance metadata.

    Args:
        request: Request that produced the result.
        capability: Capability status for the request domain.
        rows: Redacted metadata rows.
        source: Metadata source override.
        warnings: Additional result warnings.
        redactions: Redacted fields.
    """

    __slots__ = ("capability", "redactions", "request", "rows", "source", "warnings")

    def __init__(
        self,
        request: SystemMetadataRequest,
        capability: SystemMetadataCapability,
        *,
        rows: "tuple[Mapping[str, object], ...]" = (),
        source: "MetadataSource | str | None" = None,
        warnings: "tuple[str, ...]" = (),
        redactions: "tuple[str, ...]" = (),
    ) -> None:
        self.request = request
        self.capability = capability
        self.rows = tuple(dict(row) for row in rows)
        self.source = capability.source if source is None else _coerce_source(source)
        self.warnings = capability.warnings + warnings
        self.redactions = redactions

    @classmethod
    def from_rows(
        cls,
        request: SystemMetadataRequest,
        capability: SystemMetadataCapability,
        *,
        rows: "tuple[Mapping[str, object], ...]",
        source: "MetadataSource | str | None" = None,
        warnings: "tuple[str, ...]" = (),
    ) -> "SystemMetadataResult":
        """Create a result from raw rows, applying request redaction first."""
        redacted_rows: list[dict[str, object]] = []
        redactions: set[str] = set()
        for row in rows:
            redacted, fields = request.redaction_policy.redact_row(row)
            redacted_rows.append(redacted)
            redactions.update(fields)
        return cls(
            request,
            capability,
            rows=tuple(redacted_rows),
            source=source,
            warnings=warnings,
            redactions=tuple(sorted(redactions)),
        )

    def to_dict(self) -> "dict[str, object]":
        """Serialize the system metadata result."""
        return {
            "request": self.request.to_dict(),
            "capability": self.capability.to_dict(),
            "rows": self.rows,
            "source": self.source.value,
            "warnings": self.warnings,
            "redactions": self.redactions,
        }

    def __repr__(self) -> str:
        return (
            f"SystemMetadataResult(request={self.request!r}, capability={self.capability!r}, "
            f"rows={self.rows!r}, warnings={self.warnings!r}, redactions={self.redactions!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SystemMetadataResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        rows_hash = tuple(tuple(sorted((key, repr(value)) for key, value in row.items())) for row in self.rows)
        return hash((
            repr(self.request.to_dict()),
            self.capability,
            rows_hash,
            self.source,
            self.warnings,
            self.redactions,
        ))


class SystemMetadata(ObjectMetadata):
    """Opt-in system or performance metadata."""

    __slots__ = ()


class TableDetails(ObjectMetadata):
    """Rich table metadata including optional DDL status."""

    __slots__ = ("ddl", "table_type")

    def __init__(
        self,
        identity: ObjectIdentity,
        *,
        table_type: str | None = None,
        source: "MetadataSource | str | None" = None,
        ddl: DDLResult | None = None,
        attributes: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(identity, source=source, attributes=attributes)
        self.table_type = table_type
        self.ddl = ddl

    def to_dict(self) -> "dict[str, object]":
        """Serialize table details."""
        payload = super().to_dict()
        payload["table_type"] = self.table_type
        payload["ddl"] = None if self.ddl is None else self.ddl.to_dict()
        return payload


class ColumnMetadata(TypedDict, total=False):
    """Metadata for a database column."""

    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: str | bool | None
    column_default: str | None
    ordinal_position: int
    max_length: int
    numeric_precision: int
    numeric_scale: int
    is_primary: bool | int
    is_unique: bool | int
    extra: str


class TableMetadata(TypedDict, total=False):
    """Metadata for a database table."""

    schema_name: str
    table_name: str
    table_type: str
    table_catalog: str
    table_schema: str
    dependency_level: int
    level: int


class IndexMetadata(TypedDict, total=False):
    """Metadata for a database index."""

    schema_name: str
    table_name: str
    index_name: str
    columns: list[str] | str | None
    is_unique: bool | int
    is_primary: bool | int


@mypyc_attr(allow_interpreted_subclasses=False)
class ForeignKeyMetadata:
    """Metadata for a foreign key constraint."""

    __slots__ = (
        "column_name",
        "constraint_name",
        "referenced_column",
        "referenced_schema",
        "referenced_table",
        "schema",
        "table_name",
    )

    def __init__(
        self,
        table_name: str,
        column_name: str,
        referenced_table: str,
        referenced_column: str,
        constraint_name: str | None = None,
        schema: str | None = None,
        referenced_schema: str | None = None,
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.referenced_table = referenced_table
        self.referenced_column = referenced_column
        self.constraint_name = constraint_name
        self.schema = schema
        self.referenced_schema = referenced_schema

    def __repr__(self) -> str:
        return (
            f"ForeignKeyMetadata(table_name={self.table_name!r}, column_name={self.column_name!r}, "
            f"referenced_table={self.referenced_table!r}, referenced_column={self.referenced_column!r}, "
            f"constraint_name={self.constraint_name!r}, schema={self.schema!r}, "
            f"referenced_schema={self.referenced_schema!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ForeignKeyMetadata):
            return NotImplemented
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.referenced_table == other.referenced_table
            and self.referenced_column == other.referenced_column
            and self.constraint_name == other.constraint_name
            and self.schema == other.schema
            and self.referenced_schema == other.referenced_schema
        )

    def __hash__(self) -> int:
        return hash((
            self.table_name,
            self.column_name,
            self.referenced_table,
            self.referenced_column,
            self.constraint_name,
            self.schema,
            self.referenced_schema,
        ))

    def __reduce__(
        self,
    ) -> "tuple[type[ForeignKeyMetadata], tuple[str, str, str, str, str | None, str | None, str | None]]":
        return (
            self.__class__,
            (
                self.table_name,
                self.column_name,
                self.referenced_table,
                self.referenced_column,
                self.constraint_name,
                self.schema,
                self.referenced_schema,
            ),
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class VersionInfo:
    """Parsed database version info."""

    __slots__ = ("major", "minor", "patch")

    def __init__(self, major: int, minor: int = 0, patch: int = 0) -> None:
        """Initialize version info.

        Args:
            major: Major version number
            minor: Minor version number
            patch: Patch version number
        """
        self.major = major
        self.minor = minor
        self.patch = patch

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version as tuple for comparison."""
        return (self.major, self.minor, self.patch)

    def __str__(self) -> str:
        """String representation of version info."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"VersionInfo({self.major}, {self.minor}, {self.patch})"

    def __eq__(self, other: object) -> bool:
        """Check version equality."""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return self.version_tuple == other.version_tuple

    def __lt__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than another."""
        return self.version_tuple < other.version_tuple

    def __le__(self, other: "VersionInfo") -> bool:
        """Check if this version is less than or equal to another."""
        return self.version_tuple <= other.version_tuple

    def __gt__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than another."""
        return self.version_tuple > other.version_tuple

    def __ge__(self, other: "VersionInfo") -> bool:
        """Check if this version is greater than or equal to another."""
        return self.version_tuple >= other.version_tuple

    def __hash__(self) -> int:
        """Make VersionInfo hashable based on version tuple."""
        return hash(self.version_tuple)

    def __reduce__(self) -> "tuple[type[VersionInfo], tuple[int, int, int]]":
        return (self.__class__, self.version_tuple)


VersionCacheResult = tuple[bool, VersionInfo | None]
"""Return type for version cache lookup methods."""


class FeatureFlags(TypedDict, total=False):
    """Typed feature flags for data dictionary dialects."""

    supports_arrays: bool
    supports_clustering: bool
    supports_cte: bool
    supports_generators: bool
    supports_for_update: bool
    supports_geography: bool
    supports_in_memory: bool
    supports_index_clustering: bool
    supports_interleaved_tables: bool
    supports_json: bool
    supports_maps: bool
    supports_partitioning: bool
    supports_prepared_statements: bool
    supports_returning: bool
    supports_schemas: bool
    supports_skip_locked: bool
    supports_structs: bool
    supports_transactions: bool
    supports_upsert: bool
    supports_uuid: bool
    supports_window_functions: bool


class FeatureVersions(TypedDict, total=False):
    """Typed feature version requirements for data dictionary dialects."""

    supports_cte: "VersionInfo"
    supports_json: "VersionInfo"
    supports_jsonb: "VersionInfo"
    supports_partitioning: "VersionInfo"
    supports_returning: "VersionInfo"
    supports_skip_locked: "VersionInfo"
    supports_upsert: "VersionInfo"
    supports_window_functions: "VersionInfo"


class TableStatisticsMetadata(TypedDict, total=False):
    """Native driver statistics for a table or column."""

    catalog_name: str
    schema_name: str
    table_name: str
    column_name: str | None
    statistic_key: int
    statistic_name: str
    statistic_value: int | float | str | bytes | None
    is_approximate: bool


@mypyc_attr(allow_interpreted_subclasses=False)
class DialectConfig:
    """Static configuration for a database dialect."""

    __slots__ = (
        "default_schema",
        "feature_flags",
        "feature_versions",
        "name",
        "parameter_style",
        "type_mappings",
        "version_pattern",
    )

    def __init__(
        self,
        name: str,
        feature_versions: "FeatureVersions",
        feature_flags: "FeatureFlags",
        type_mappings: "dict[str, str]",
        version_pattern: "Pattern[str]",
        default_schema: "str | None" = None,
        parameter_style: str = "named",
    ) -> None:
        """Initialize a dialect configuration.

        Args:
            name: Dialect name used for lookups.
            feature_versions: Minimum versions required for features.
            feature_flags: Static boolean feature flags.
            type_mappings: Logical type to dialect type mapping.
            version_pattern: Regex used to parse version strings.
            default_schema: Default schema for dialect.
            parameter_style: Default parameter style for dialect SQL.
        """
        self.name: str = name
        self.feature_versions: FeatureVersions = feature_versions
        self.feature_flags: FeatureFlags = feature_flags
        self.type_mappings: dict[str, str] = type_mappings
        self.version_pattern: Pattern[str] = version_pattern
        self.default_schema: str | None = default_schema
        self.parameter_style: str = parameter_style

    def get_feature_flag(self, feature: str) -> "bool | None":
        """Return a feature flag value if defined.

        Args:
            feature: Feature flag name.

        Returns:
            Feature flag value or None if unknown.
        """
        return cast("bool | None", self.feature_flags.get(feature))

    def get_feature_version(self, feature: str) -> "VersionInfo | None":
        """Return required version for a feature if defined.

        Args:
            feature: Feature version name.

        Returns:
            VersionInfo if defined, otherwise None.
        """
        return cast("VersionInfo | None", self.feature_versions.get(feature))

    def get_optimal_type(self, logical_type: str) -> str:
        """Return the dialect-specific type for a logical type.

        Args:
            logical_type: Logical type name.

        Returns:
            Dialect-specific type string.
        """
        default_type = self.type_mappings.get("text", "TEXT")
        return self.type_mappings.get(logical_type, default_type)


def _coerce_support(value: "MetadataSupport | str") -> MetadataSupport:
    if isinstance(value, MetadataSupport):
        return value
    return MetadataSupport(value)


def _coerce_fidelity(value: "MetadataFidelity | str") -> MetadataFidelity:
    if isinstance(value, MetadataFidelity):
        return value
    return MetadataFidelity(value)


def _coerce_risk(value: "MetadataRisk | str") -> MetadataRisk:
    if isinstance(value, MetadataRisk):
        return value
    return MetadataRisk(value)


def _coerce_source(value: "MetadataSource | str") -> MetadataSource:
    if isinstance(value, MetadataSource):
        return value
    return MetadataSource(value)


def _serialize_metadata_item(item: object) -> object:
    to_dict = getattr(item, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    return item


def _hashable_mapping(value: "dict[str, object]") -> "tuple[tuple[str, str], ...]":
    return tuple(sorted((key, repr(item)) for key, item in value.items()))
