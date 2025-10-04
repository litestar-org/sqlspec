"""DuckDB-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.duckdb.driver import DuckDBDriver

logger = get_logger("adapters.duckdb.data_dictionary")

# Compiled regex patterns
DUCKDB_VERSION_PATTERN = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")

__all__ = ("DuckDBSyncDataDictionary",)


class DuckDBSyncDataDictionary(SyncDataDictionaryBase):
    """DuckDB-specific sync data dictionary."""

    def get_version(self, driver: SyncDriverAdapterBase) -> "VersionInfo | None":
        """Get DuckDB database version information.

        Args:
            driver: DuckDB driver instance

        Returns:
            DuckDB version information or None if detection fails
        """
        version_str = cast("DuckDBDriver", driver).select_value("SELECT version()")
        if not version_str:
            logger.warning("No DuckDB version information found")
            return None

        # Parse version like "v0.9.2" or "0.9.2"
        version_match = DUCKDB_VERSION_PATTERN.search(str(version_str))
        if not version_match:
            logger.warning("Could not parse DuckDB version: %s", version_str)
            return None

        major, minor, patch = map(int, version_match.groups())
        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected DuckDB version: %s", version_info)
        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if DuckDB database supports a specific feature.

        Args:
            driver: DuckDB driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = self.get_version(driver)
        if not version_info:
            return False

        feature_checks: dict[str, Callable[..., bool]] = {
            "supports_json": lambda _: True,  # DuckDB has excellent JSON support
            "supports_arrays": lambda _: True,  # LIST type
            "supports_maps": lambda _: True,  # MAP type
            "supports_structs": lambda _: True,  # STRUCT type
            "supports_returning": lambda v: v >= VersionInfo(0, 8, 0),
            "supports_upsert": lambda v: v >= VersionInfo(0, 8, 0),
            "supports_window_functions": lambda _: True,
            "supports_cte": lambda _: True,
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_schemas": lambda _: True,
            "supports_uuid": lambda _: True,
        }

        if feature in feature_checks:
            return bool(feature_checks[feature](version_info))

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:  # pyright: ignore
        """Get optimal DuckDB type for a category.

        Args:
            driver: DuckDB driver instance
            type_category: Type category

        Returns:
            DuckDB-specific type name
        """
        type_map = {
            "json": "JSON",
            "uuid": "UUID",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "text": "TEXT",
            "blob": "BLOB",
            "array": "LIST",
            "map": "MAP",
            "struct": "STRUCT",
        }
        return type_map.get(type_category, "VARCHAR")

    def list_available_features(self) -> "list[str]":
        """List available DuckDB feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_arrays",
            "supports_maps",
            "supports_structs",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_uuid",
        ]
