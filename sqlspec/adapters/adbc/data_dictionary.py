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

    Delegates to appropriate dialect-specific logic based on the driver's dialect.
    """

    def _get_dialect(self, driver: SyncDriverAdapterBase) -> str:
        """Get dialect from ADBC driver.

        Args:
            driver: ADBC driver instance

        Returns:
            Dialect name
        """
        return str(cast("AdbcDriver", driver).dialect)

    def get_version(self, driver: SyncDriverAdapterBase) -> "Optional[VersionInfo]":
        """Get database version information based on detected dialect.

        Args:
            driver: ADBC driver instance

        Returns:
            Database version information or None if detection fails
        """
        dialect = self._get_dialect(driver)
        adbc_driver = cast("AdbcDriver", driver)

        try:
            if dialect == "postgres":
                version_str = adbc_driver.select_value("SELECT version()")
                if version_str:
                    match = POSTGRES_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major = int(match.group(1))
                        minor = int(match.group(2))
                        patch = int(match.group(3)) if match.group(3) else 0
                        return VersionInfo(major, minor, patch)

            elif dialect == "sqlite":
                version_str = adbc_driver.select_value("SELECT sqlite_version()")
                if version_str:
                    match = SQLITE_VERSION_PATTERN.match(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        return VersionInfo(major, minor, patch)

            elif dialect == "duckdb":
                version_str = adbc_driver.select_value("SELECT version()")
                if version_str:
                    match = DUCKDB_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        return VersionInfo(major, minor, patch)

            elif dialect == "mysql":
                version_str = adbc_driver.select_value("SELECT VERSION()")
                if version_str:
                    match = MYSQL_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        return VersionInfo(major, minor, patch)

            elif dialect == "bigquery":
                return VersionInfo(1, 0, 0)

        except Exception:
            logger.warning("Failed to get %s version", dialect)

        return None

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if database supports a specific feature based on detected dialect.

        Args:
            driver: ADBC driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        dialect = self._get_dialect(driver)
        version_info = self.get_version(driver)

        if dialect == "postgres":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(9, 2, 0),
                "supports_jsonb": lambda v: v and v >= VersionInfo(9, 4, 0),
                "supports_uuid": lambda _: True,
                "supports_arrays": lambda _: True,
                "supports_returning": lambda v: v and v >= VersionInfo(8, 2, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(9, 5, 0),
                "supports_window_functions": lambda v: v and v >= VersionInfo(8, 4, 0),
                "supports_cte": lambda v: v and v >= VersionInfo(8, 4, 0),
                "supports_transactions": lambda _: True,
                "supports_prepared_statements": lambda _: True,
                "supports_schemas": lambda _: True,
            }
        elif dialect == "sqlite":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(3, 38, 0),
                "supports_returning": lambda v: v and v >= VersionInfo(3, 35, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(3, 24, 0),
                "supports_window_functions": lambda v: v and v >= VersionInfo(3, 25, 0),
                "supports_cte": lambda v: v and v >= VersionInfo(3, 8, 3),
                "supports_transactions": lambda _: True,
                "supports_prepared_statements": lambda _: True,
                "supports_schemas": lambda _: False,
                "supports_arrays": lambda _: False,
                "supports_uuid": lambda _: False,
            }
        elif dialect == "duckdb":
            feature_checks = {
                "supports_json": lambda _: True,
                "supports_arrays": lambda _: True,
                "supports_uuid": lambda _: True,
                "supports_returning": lambda v: v and v >= VersionInfo(0, 8, 0),
                "supports_upsert": lambda v: v and v >= VersionInfo(0, 8, 0),
                "supports_window_functions": lambda _: True,
                "supports_cte": lambda _: True,
                "supports_transactions": lambda _: True,
                "supports_prepared_statements": lambda _: True,
                "supports_schemas": lambda _: True,
            }
        elif dialect == "mysql":
            feature_checks = {
                "supports_json": lambda v: v and v >= VersionInfo(5, 7, 8),
                "supports_cte": lambda v: v and v >= VersionInfo(8, 0, 1),
                "supports_returning": lambda _: False,
                "supports_upsert": lambda _: True,
                "supports_window_functions": lambda v: v and v >= VersionInfo(8, 0, 2),
                "supports_transactions": lambda _: True,
                "supports_prepared_statements": lambda _: True,
                "supports_schemas": lambda _: True,
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
                "supports_window_functions": lambda _: True,
                "supports_cte": lambda _: True,
                "supports_transactions": lambda _: False,
                "supports_prepared_statements": lambda _: True,
                "supports_schemas": lambda _: True,
                "supports_uuid": lambda _: False,
            }
        else:
            feature_checks = {
                "supports_transactions": lambda _: True,
                "supports_prepared_statements": lambda _: True,
                "supports_window_functions": lambda _: True,
                "supports_cte": lambda _: True,
            }

        if feature in feature_checks:
            return bool(feature_checks[feature](version_info))

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal database type for a category based on detected dialect.

        Args:
            driver: ADBC driver instance
            type_category: Type category

        Returns:
            Database-specific type name
        """
        dialect = self._get_dialect(driver)
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
                "array": "ARRAY",
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
