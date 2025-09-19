"""ADBC multi-dialect data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Optional, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.driver import AdbcDriver

logger = get_logger("adapters.adbc.data_dictionary")

POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")
SQLITE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")
DUCKDB_VERSION_PATTERN = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")
MYSQL_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

__all__ = ("AdbcDataDictionary",)


class AdbcDataDictionary(SyncDataDictionaryBase):
    """ADBC multi-dialect data dictionary.

    Detects the underlying database dialect and delegates to appropriate logic.
    """

    def __init__(self) -> None:
        """Initialize ADBC data dictionary."""
        self._detected_dialect: Optional[str] = None
        self._cached_version: Optional[VersionInfo] = None

    def _detect_dialect(self, driver: SyncDriverAdapterBase) -> str:
        """Detect the underlying database dialect.

        Args:
            driver: ADBC driver instance

        Returns:
            Detected dialect name
        """
        if self._detected_dialect:
            return self._detected_dialect

        self._detected_dialect = (
            str(cast("AdbcDriver", driver).dialect) if cast("AdbcDriver", driver).dialect else "sqlite"
        )
        return self._detected_dialect

    def get_version(self, driver: SyncDriverAdapterBase) -> "Optional[VersionInfo]":
        """Get database version information based on detected dialect.

        Args:
            driver: ADBC driver instance

        Returns:
            Database version information or None if detection fails
        """
        if self._cached_version:
            return self._cached_version

        dialect = self._detect_dialect(driver)

        if dialect == "postgres":
            try:
                version_str = cast("AdbcDriver", driver).select_value("SELECT version()")
                version_match = POSTGRES_VERSION_PATTERN.search(str(version_str))
                if version_match:
                    major = int(version_match.group(1))
                    minor = int(version_match.group(2))
                    patch = int(version_match.group(3)) if version_match.group(3) else 0
                    self._cached_version = VersionInfo(major, minor, patch)
            except Exception:
                logger.warning("Failed to get PostgreSQL version")

        elif dialect == "sqlite":
            try:
                version_str = cast("AdbcDriver", driver).select_value("SELECT sqlite_version()")
                version_match = SQLITE_VERSION_PATTERN.match(str(version_str))
                if version_match:
                    major, minor, patch = map(int, version_match.groups())
                    self._cached_version = VersionInfo(major, minor, patch)
            except Exception:
                logger.warning("Failed to get SQLite version")

        elif dialect == "duckdb":
            try:
                version_str = cast("AdbcDriver", driver).select_value("SELECT version()")
                version_match = DUCKDB_VERSION_PATTERN.search(str(version_str))
                if version_match:
                    major, minor, patch = map(int, version_match.groups())
                    self._cached_version = VersionInfo(major, minor, patch)
            except Exception:
                logger.warning("Failed to get DuckDB version")

        elif dialect == "mysql":
            try:
                version_str = cast("AdbcDriver", driver).select_value("SELECT VERSION()")
                version_match = MYSQL_VERSION_PATTERN.search(str(version_str))
                if version_match:
                    major, minor, patch = map(int, version_match.groups())
                    self._cached_version = VersionInfo(major, minor, patch)
            except Exception:
                logger.warning("Failed to get MySQL version")

        elif dialect == "bigquery":
            # BigQuery is a cloud service
            self._cached_version = VersionInfo(1, 0, 0)

        logger.debug("Detected %s version: %s", dialect, self._cached_version)
        return self._cached_version

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if database supports a specific feature based on detected dialect.

        Args:
            driver: ADBC driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        dialect = self._detect_dialect(driver)
        version_info = self.get_version(driver)

        if dialect == "postgres":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(9, 2, 0),
                "supports_jsonb": lambda v: v and v >= VersionInfo(9, 4, 0),
                "supports_uuid": lambda _: True,
                "supports_arrays": lambda _: True,
                "supports_returning": lambda v: v and v >= VersionInfo(8, 2, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(9, 5, 0),
            }
        elif dialect == "sqlite":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(3, 38, 0),
                "supports_returning": lambda v: v and v >= VersionInfo(3, 35, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(3, 24, 0),
                "supports_uuid": lambda _: False,
                "supports_arrays": lambda _: False,
            }
        elif dialect == "duckdb":
            feature_checks = {
                "supports_json": lambda _: True,
                "supports_arrays": lambda _: True,
                "supports_uuid": lambda _: True,
                "supports_returning": lambda v: v and v >= VersionInfo(0, 8, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(0, 8, 0),
            }
        elif dialect == "mysql":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(5, 7, 8),
                "supports_cte": lambda v: v and v >= VersionInfo(8, 0, 1),
                "supports_returning": lambda _: False,
                "supports_upsert": lambda _: True,
                "supports_uuid": lambda _: False,
                "supports_arrays": lambda _: False,
            }
        elif dialect == "bigquery":
            feature_checks = {
                "supports_json": lambda _: True,
                "supports_arrays": lambda _: True,
                "supports_structs": lambda _: True,
                "supports_returning": lambda _: False,
                "supports_upsert": lambda _: True,
                "supports_uuid": lambda _: False,
            }
        else:
            feature_checks = {}

        # Common features
        common_features = {
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_window_functions": lambda _: True,
            "supports_cte": lambda _: True,
        }

        feature_checks.update(common_features)

        if feature in feature_checks:
            return feature_checks[feature](version_info)

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal database type for a category based on detected dialect.

        Args:
            driver: ADBC driver instance
            type_category: Type category

        Returns:
            Database-specific type name
        """
        dialect = self._detect_dialect(driver)
        version_info = self.get_version(driver)

        if dialect == "postgres":
            if type_category == "json":
                if version_info and version_info >= VersionInfo(9, 4, 0):
                    return "JSONB"
                if version_info and version_info >= VersionInfo(9, 2, 0):
                    return "JSON"
                return "TEXT"
            type_map = {
                "uuid": "UUID",
                "boolean": "BOOLEAN",
                "timestamp": "TIMESTAMP WITH TIME ZONE",
                "text": "TEXT",
                "blob": "BYTEA",
            }

        elif dialect == "sqlite":
            if type_category == "json":
                if version_info and version_info >= VersionInfo(3, 38, 0):
                    return "JSON"
                return "TEXT"
            type_map = {"uuid": "TEXT", "boolean": "INTEGER", "timestamp": "TIMESTAMP", "text": "TEXT", "blob": "BLOB"}

        elif dialect == "duckdb":
            type_map = {
                "json": "JSON",
                "uuid": "UUID",
                "boolean": "BOOLEAN",
                "timestamp": "TIMESTAMP",
                "text": "TEXT",
                "blob": "BLOB",
                "array": "LIST",
            }

        elif dialect == "mysql":
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

        elif dialect == "bigquery":
            type_map = {
                "json": "JSON",
                "uuid": "STRING",
                "boolean": "BOOL",
                "timestamp": "TIMESTAMP",
                "text": "STRING",
                "blob": "BYTES",
                "array": "ARRAY",
            }
        else:
            # Generic fallback
            type_map = {
                "json": "TEXT",
                "uuid": "VARCHAR(36)",
                "boolean": "INTEGER",
                "timestamp": "TIMESTAMP",
                "text": "TEXT",
                "blob": "BLOB",
            }

        return type_map.get(type_category, "TEXT")

    def list_available_features(self) -> "list[str]":
        """List available feature flags across all supported dialects.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_jsonb",
            "supports_uuid",
            "supports_arrays",
            "supports_structs",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
        ]
