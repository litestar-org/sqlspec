"""PostgreSQL-specific data dictionary for metadata queries via asyncpg."""

import re
from typing import TYPE_CHECKING, Optional, cast

from sqlspec.driver import AsyncDataDictionaryBase, AsyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.driver import AsyncpgDriver

logger = get_logger("adapters.asyncpg.data_dictionary")

# Compiled regex patterns
POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")

__all__ = ("PostgresAsyncDataDictionary",)


class PostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    """PostgreSQL-specific async data dictionary."""

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "Optional[VersionInfo]":
        """Get PostgreSQL database version information.

        Args:
            driver: Async database driver instance

        Returns:
            PostgreSQL version information or None if detection fails
        """
        asyncpg_driver = cast("AsyncpgDriver", driver)
        version_str = await asyncpg_driver.select_value("SELECT version()")
        if not version_str:
            logger.warning("No PostgreSQL version information found")
            return None

        # Parse version like "PostgreSQL 15.3 on x86_64-pc-linux-gnu..."
        version_match = POSTGRES_VERSION_PATTERN.search(str(version_str))
        if not version_match:
            logger.warning("Could not parse PostgreSQL version: %s", version_str)
            return None

        major = int(version_match.group(1))
        minor = int(version_match.group(2))
        patch = int(version_match.group(3)) if version_match.group(3) else 0

        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected PostgreSQL version: %s", version_info)
        return version_info

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Check if PostgreSQL database supports a specific feature.

        Args:
            driver: Async database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = await self.get_version(driver)
        if not version_info:
            return False

        feature_checks = {
            "supports_json": lambda v: v >= VersionInfo(9, 2, 0),
            "supports_jsonb": lambda v: v >= VersionInfo(9, 4, 0),
            "supports_uuid": lambda _: True,  # UUID extension widely available
            "supports_arrays": lambda _: True,  # PostgreSQL has excellent array support
            "supports_returning": lambda v: v >= VersionInfo(8, 2, 0),
            "supports_upsert": lambda v: v >= VersionInfo(9, 5, 0),  # ON CONFLICT
            "supports_window_functions": lambda v: v >= VersionInfo(8, 4, 0),
            "supports_cte": lambda v: v >= VersionInfo(8, 4, 0),
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_schemas": lambda _: True,
            "supports_partitioning": lambda v: v >= VersionInfo(10, 0, 0),
        }

        if feature in feature_checks:
            return feature_checks[feature](version_info)

        return False

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal PostgreSQL type for a category.

        Args:
            driver: Async database driver instance
            type_category: Type category

        Returns:
            PostgreSQL-specific type name
        """
        version_info = await self.get_version(driver)

        if type_category == "json":
            if version_info and version_info >= VersionInfo(9, 4, 0):
                return "JSONB"  # Prefer JSONB over JSON
            if version_info and version_info >= VersionInfo(9, 2, 0):
                return "JSON"
            return "TEXT"

        type_map = {
            "uuid": "UUID",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP WITH TIME ZONE",
            "text": "TEXT",
            "blob": "BYTEA",
            "array": "ARRAY",
        }
        return type_map.get(type_category, "TEXT")

    def list_available_features(self) -> "list[str]":
        """List available PostgreSQL feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_jsonb",
            "supports_uuid",
            "supports_arrays",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_partitioning",
        ]
