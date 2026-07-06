from enum import Enum
from typing import TYPE_CHECKING, Any, TypedDict, cast

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from re import Pattern

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

    def __reduce__(
        self,
    ) -> "tuple[type[ObjectIdentity], tuple[str, str], dict[str, str | None | MetadataSource]]":
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
    def unsupported(cls, domain: str, *, source: "MetadataSource | str" = MetadataSource.UNKNOWN) -> "MetadataCapability":
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

    def __init__(self, dialect: str, *, adapter: str | None = None, capabilities: "tuple[MetadataCapability, ...]" = ()) -> None:
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
    def from_domains(
        cls, dialect: str, adapter: str | None, domains: "tuple[str, ...]"
    ) -> "MetadataCapabilityProfile":
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
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
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


class DDLResult:
    """DDL text and fidelity status for one database object."""

    __slots__ = ("ddl", "fidelity", "identity", "source", "status", "warnings")

    def __init__(
        self,
        identity: ObjectIdentity,
        status: "MetadataSupport | str",
        *,
        fidelity: "MetadataFidelity | str" = MetadataFidelity.UNSUPPORTED,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        ddl: str | None = None,
        warnings: "tuple[str, ...]" = (),
    ) -> None:
        self.identity = identity
        self.status = _coerce_support(status)
        self.fidelity = _coerce_fidelity(fidelity)
        self.source = _coerce_source(source)
        self.ddl = ddl
        self.warnings = warnings

    def to_dict(self) -> "dict[str, object]":
        """Serialize the DDL result."""
        return {
            "identity": self.identity.to_dict(),
            "status": self.status.value,
            "fidelity": self.fidelity.value,
            "source": self.source.value,
            "ddl": self.ddl,
            "warnings": self.warnings,
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DDLResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((self.identity, self.status, self.fidelity, self.source, self.ddl, self.warnings))


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
