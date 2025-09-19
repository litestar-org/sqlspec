"""MySQL-specific data dictionary for metadata queries via asyncmy."""

import re
from typing import TYPE_CHECKING, Optional, cast

from sqlspec.driver import AsyncDataDictionaryBase, AsyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

logger = get_logger("adapters.asyncmy.data_dictionary")

# Compiled regex patterns
VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

__all__ = ("MySQLAsyncDataDictionary",)


class MySQLAsyncDataDictionary(AsyncDataDictionaryBase):
    """MySQL-specific async data dictionary."""

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "Optional[VersionInfo]":
        """Get MySQL database version information.

        Args:
            driver: Async database driver instance

        Returns:
            MySQL version information or None if detection fails
        """
        result = await cast("AsyncmyDriver", driver).select_value_or_none("SELECT VERSION() as version")
        if not result:
            logger.warning("No MySQL version information found")

        # Parse version like "8.0.33-0ubuntu0.22.04.2" or "5.7.42-log"
        version_match = VERSION_PATTERN.search(str(result))
        if not version_match:
            logger.warning("Could not parse MySQL version: %s", result)
            return None

        major, minor, patch = map(int, version_match.groups())
        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected MySQL version: %s", version_info)
        return version_info

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Check if MySQL database supports a specific feature.

        Args:
            driver: MySQL async driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = await self.get_version(driver)
        if not version_info:
            return False

        feature_checks = {
            "supports_json": lambda v: v >= VersionInfo(5, 7, 8),
            "supports_cte": lambda v: v >= VersionInfo(8, 0, 1),
            "supports_window_functions": lambda v: v >= VersionInfo(8, 0, 2),
            "supports_returning": lambda _: False,  # MySQL doesn't have RETURNING
            "supports_upsert": lambda _: True,  # ON DUPLICATE KEY UPDATE available
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_schemas": lambda _: True,  # MySQL calls them databases
            "supports_arrays": lambda _: False,  # No array types
            "supports_uuid": lambda _: False,  # No native UUID, use VARCHAR(36)
        }

        if feature in feature_checks:
            return feature_checks[feature](version_info)

        return False

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal MySQL type for a category.

        Args:
            driver: MySQL async driver instance
            type_category: Type category

        Returns:
            MySQL-specific type name
        """
        version_info = await self.get_version(driver)

        if type_category == "json":
            if version_info and version_info >= VersionInfo(5, 7, 8):
                return "JSON"
            return "TEXT"

        type_map = {
            "uuid": "VARCHAR(36)",
            "boolean": "TINYINT(1)",
            "timestamp": "TIMESTAMP",
            "text": "TEXT",
            "blob": "BLOB",
        }
        return type_map.get(type_category, "VARCHAR(255)")

    def list_available_features(self) -> "list[str]":
        """List available MySQL feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_cte",
            "supports_window_functions",
            "supports_returning",
            "supports_upsert",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_arrays",
            "supports_uuid",
        ]
