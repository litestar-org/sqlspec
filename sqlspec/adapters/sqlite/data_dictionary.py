"""SQLite-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Optional, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.driver import SqliteDriver

logger = get_logger("adapters.sqlite.data_dictionary")

# Compiled regex patterns
SQLITE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

__all__ = ("SqliteSyncDataDictionary",)


class SqliteSyncDataDictionary(SyncDataDictionaryBase):
    """SQLite-specific sync data dictionary."""

    def get_version(self, driver: SyncDriverAdapterBase) -> "Optional[VersionInfo]":
        """Get SQLite database version information.

        Args:
            driver: Sync database driver instance

        Returns:
            SQLite version information or None if detection fails
        """
        version_str = cast("SqliteDriver", driver).select_value("SELECT sqlite_version()")
        if not version_str:
            logger.warning("No SQLite version information found")
            return None

        # Parse version like "3.45.0"
        version_match = SQLITE_VERSION_PATTERN.match(str(version_str))
        if not version_match:
            logger.warning("Could not parse SQLite version: %s", version_str)
            return None

        major, minor, patch = map(int, version_match.groups())
        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected SQLite version: %s", version_info)
        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: SQLite driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = self.get_version(driver)
        if not version_info:
            return False

        feature_checks = {
            "supports_json": lambda v: v >= VersionInfo(3, 38, 0),
            "supports_returning": lambda v: v >= VersionInfo(3, 35, 0),
            "supports_upsert": lambda v: v >= VersionInfo(3, 24, 0),
            "supports_window_functions": lambda v: v >= VersionInfo(3, 25, 0),
            "supports_cte": lambda v: v >= VersionInfo(3, 8, 3),
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_schemas": lambda _: False,  # SQLite has ATTACH but not schemas
            "supports_arrays": lambda _: False,
            "supports_uuid": lambda _: False,
        }

        if feature in feature_checks:
            return feature_checks[feature](version_info)

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal SQLite type for a category.

        Args:
            driver: SQLite driver instance
            type_category: Type category

        Returns:
            SQLite-specific type name
        """
        version_info = self.get_version(driver)

        if type_category == "json":
            if version_info and version_info >= VersionInfo(3, 38, 0):
                return "JSON"
            return "TEXT"

        type_map = {"uuid": "TEXT", "boolean": "INTEGER", "timestamp": "TIMESTAMP", "text": "TEXT", "blob": "BLOB"}
        return type_map.get(type_category, "TEXT")

    def list_available_features(self) -> "list[str]":
        """List available SQLite feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_arrays",
            "supports_uuid",
        ]
