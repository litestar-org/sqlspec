from typing import TYPE_CHECKING, TypedDict, cast

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from re import Pattern

__all__ = (
    "ColumnMetadata",
    "DialectConfig",
    "FeatureFlags",
    "FeatureVersions",
    "ForeignKeyMetadata",
    "IndexMetadata",
    "TableMetadata",
    "VersionCacheResult",
    "VersionInfo",
)


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
    supports_upsert: "VersionInfo"
    supports_window_functions: "VersionInfo"


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
