from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from re import Pattern

    from sqlspec.typing import VersionInfo

__all__ = ("DialectConfig", "FeatureFlags", "FeatureVersions")


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
