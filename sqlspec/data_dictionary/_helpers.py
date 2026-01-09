from typing import TYPE_CHECKING

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary._loader import get_data_dictionary_loader
from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.exceptions import SQLFileNotFoundError

if TYPE_CHECKING:
    from sqlspec.core.statement import SQL
    from sqlspec.data_dictionary._types import DialectConfig
    from sqlspec.driver import VersionInfo

__all__ = ("DialectSQLMixin",)


@mypyc_attr(allow_interpreted_subclasses=True)
class DialectSQLMixin:
    """Mixin for loading dialect-specific data dictionary SQL."""

    __slots__ = ()

    dialect: str

    def get_default_features(self) -> "list[str]":
        """Return default feature flags for the dialect mixin."""
        return []

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(self.dialect)

    def get_query(self, name: str) -> "SQL":
        """Return a named SQL query for this dialect."""
        loader = get_data_dictionary_loader()
        return loader.get_query(self.dialect, name)

    def get_query_text(self, name: str) -> str:
        """Return raw SQL text for a named query for this dialect."""
        loader = get_data_dictionary_loader()
        return loader.get_query_text(self.dialect, name)

    def get_query_text_or_none(self, name: str) -> "str | None":
        """Return raw SQL text for a named query or None if missing."""
        try:
            return self.get_query_text(name)
        except SQLFileNotFoundError:
            return None

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        config = self.get_dialect_config()
        return config.default_schema

    def resolve_feature_flag(self, feature: str, version: "VersionInfo | None") -> bool:
        """Resolve a feature flag using dialect config and version info."""
        config = self.get_dialect_config()
        flag = config.get_feature_flag(feature)
        if flag is not None:
            return flag
        required_version = config.get_feature_version(feature)
        if required_version is None or version is None:
            return False
        return bool(version >= required_version)

    def list_available_features(self) -> "list[str]":
        """List all features available for the dialect."""
        config = self.get_dialect_config()
        features = set(self.get_default_features())
        features.update(config.feature_flags.keys())
        features.update(config.feature_versions.keys())
        return sorted(features)
