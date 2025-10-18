"""Oracle-specific data dictionary for metadata queries."""
# cspell:ignore pdbs

import re
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
    VersionInfo,
)
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver

logger = get_logger("adapters.oracledb.data_dictionary")

# Oracle version constants
ORACLE_MIN_JSON_NATIVE_VERSION = 21
ORACLE_MIN_JSON_NATIVE_COMPATIBLE = 20
ORACLE_MIN_JSON_BLOB_VERSION = 12
ORACLE_MIN_OSON_VERSION = 19

# Compiled regex patterns
ORACLE_VERSION_PATTERN = re.compile(r"Oracle Database (\d+)c?.* Release (\d+)\.(\d+)\.(\d+)")

__all__ = ("OracleAsyncDataDictionary", "OracleSyncDataDictionary", "OracleVersionInfo")


class OracleVersionInfo(VersionInfo):
    """Oracle database version information."""

    def __init__(
        self, major: int, minor: int = 0, patch: int = 0, compatible: "str | None" = None, is_autonomous: bool = False
    ) -> None:
        """Initialize Oracle version info.

        Args:
            major: Major version number (e.g., 19, 21, 23)
            minor: Minor version number
            patch: Patch version number
            compatible: Compatible parameter value
            is_autonomous: Whether this is an Autonomous Database
        """
        super().__init__(major, minor, patch)
        self.compatible = compatible
        self.is_autonomous = is_autonomous

    @property
    def compatible_major(self) -> "int | None":
        """Get major version from compatible parameter."""
        if not self.compatible:
            return None
        parts = self.compatible.split(".")
        if not parts:
            return None
        return int(parts[0])

    def supports_native_json(self) -> bool:
        """Check if database supports native JSON data type.

        Returns:
            True if Oracle 21c+ with compatible >= 20
        """
        return (
            self.major >= ORACLE_MIN_JSON_NATIVE_VERSION
            and (self.compatible_major or 0) >= ORACLE_MIN_JSON_NATIVE_COMPATIBLE
        )

    def supports_oson_blob(self) -> bool:
        """Check if database supports BLOB with OSON format.

        Returns:
            True if Oracle 19c+ (Autonomous) or 21c+
        """
        if self.major >= ORACLE_MIN_JSON_NATIVE_VERSION:
            return True
        return self.major >= ORACLE_MIN_OSON_VERSION and self.is_autonomous

    def supports_json_blob(self) -> bool:
        """Check if database supports BLOB with JSON validation.

        Returns:
            True if Oracle 12c+
        """
        return self.major >= ORACLE_MIN_JSON_BLOB_VERSION

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.patch}"
        if self.compatible:
            version_str += f" (compatible={self.compatible})"
        if self.is_autonomous:
            version_str += " [Autonomous]"
        return version_str


class OracleDataDictionaryMixin:
    """Mixin providing Oracle-specific metadata queries."""

    __slots__ = ()

    def _get_columns_sql(self, table: str, schema: "str | None" = None) -> str:
        """Get SQL to query column metadata from Oracle data dictionary.

        Uses USER_TAB_COLUMNS which returns column names in UPPERCASE.

        Args:
            table: Table name to query columns for
            schema: Schema name (unused for USER_TAB_COLUMNS)

        Returns:
            SQL string for Oracle's USER_TAB_COLUMNS query
        """
        _ = schema
        return f"""
            SELECT column_name, data_type, data_length, nullable
            FROM user_tab_columns
            WHERE table_name = '{table.upper()}'
            ORDER BY column_id
        """

    def _get_oracle_version(self, driver: "OracleAsyncDriver | OracleSyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information.

        Args:
            driver: Database driver instance

        Returns:
            Oracle version information or None if detection fails
        """
        banner = driver.select_value("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'")

        # Parse version from banner like "Oracle Database 21c Enterprise Edition Release 21.0.0.0.0 - Production"
        # or "Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 - Production"
        version_match = ORACLE_VERSION_PATTERN.search(str(banner))

        if not version_match:
            logger.warning("Could not parse Oracle version from banner: %s", banner)
            return None

        major = int(version_match.group(1))
        release_major = int(version_match.group(2))
        minor = int(version_match.group(3))
        patch = int(version_match.group(4))

        # For Oracle 21c+, the major version is in the first group
        # For Oracle 19c and earlier, use the release version
        if major >= ORACLE_MIN_JSON_NATIVE_VERSION:
            version_info = OracleVersionInfo(major, minor, patch)
        else:
            version_info = OracleVersionInfo(release_major, minor, patch)

        logger.debug("Detected Oracle version: %s", version_info)
        return version_info

    def _get_oracle_compatible(self, driver: "OracleAsyncDriver | OracleSyncDriver") -> "str | None":
        """Get Oracle compatible parameter value.

        Args:
            driver: Database driver instance

        Returns:
            Compatible parameter value or None if detection fails
        """
        try:
            compatible = driver.select_value("SELECT value FROM v$parameter WHERE name = 'compatible'")
            logger.debug("Detected Oracle compatible parameter: %s", compatible)
            return str(compatible)
        except Exception:
            logger.warning("Compatible parameter not found")
            return None

    def _get_oracle_json_type(self, version_info: "OracleVersionInfo | None") -> str:
        """Determine the appropriate JSON column type for Oracle.

        Args:
            version_info: Oracle version information

        Returns:
            Appropriate Oracle column type for JSON data
        """
        if not version_info:
            logger.warning("No version info provided, using CLOB fallback")
            return "CLOB"

        # Decision matrix for JSON column type
        if version_info.supports_native_json():
            logger.info("Using native JSON type for Oracle %s", version_info)
            return "JSON"
        if version_info.supports_oson_blob():
            logger.info("Using BLOB with OSON format for Oracle %s", version_info)
            return "BLOB CHECK (data IS JSON FORMAT OSON)"
        if version_info.supports_json_blob():
            logger.info("Using BLOB with JSON validation for Oracle %s", version_info)
            return "BLOB CHECK (data IS JSON)"
        logger.info("Using CLOB fallback for Oracle %s", version_info)
        return "CLOB"


class OracleSyncDataDictionary(OracleDataDictionaryMixin, SyncDataDictionaryBase):
    """Oracle-specific sync data dictionary."""

    def _is_oracle_autonomous(self, driver: "OracleSyncDriver") -> bool:
        """Check if this is an Oracle Autonomous Database.

        Args:
            driver: Database driver instance

        Returns:
            True if this is an Autonomous Database, False otherwise
        """
        result = driver.select_value_or_none("SELECT COUNT(*) as cnt FROM v$pdbs WHERE cloud_identity IS NOT NULL")
        return bool(result and int(result) > 0)

    def get_version(self, driver: SyncDriverAdapterBase) -> "OracleVersionInfo | None":
        """Get Oracle database version information.

        Args:
            driver: Database driver instance

        Returns:
            Oracle version information or None if detection fails
        """
        oracle_driver = cast("OracleSyncDriver", driver)
        version_info = self._get_oracle_version(oracle_driver)
        if version_info:
            # Enhance with additional information
            compatible = self._get_oracle_compatible(oracle_driver)
            is_autonomous = self._is_oracle_autonomous(oracle_driver)

            version_info.compatible = compatible
            version_info.is_autonomous = is_autonomous

        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if Oracle database supports a specific feature.

        Args:
            driver: Database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        if feature == "is_autonomous":
            return self._is_oracle_autonomous(cast("OracleSyncDriver", driver))

        version_info = self.get_version(driver)
        if not version_info:
            return False

        feature_checks: dict[str, Callable[..., bool]] = {
            "supports_native_json": version_info.supports_native_json,
            "supports_oson_blob": version_info.supports_oson_blob,
            "supports_json_blob": version_info.supports_json_blob,
            "supports_json": version_info.supports_json_blob,  # Any JSON support
            "supports_transactions": lambda: True,
            "supports_prepared_statements": lambda: True,
            "supports_schemas": lambda: True,
        }

        if feature in feature_checks:
            return bool(feature_checks[feature]())

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal Oracle type for a category.

        Args:
            driver: Database driver instance
            type_category: Type category

        Returns:
            Oracle-specific type name
        """
        type_map = {
            "json": self._get_oracle_json_type(self.get_version(driver)),
            "uuid": "RAW(16)",
            "boolean": "NUMBER(1)",
            "timestamp": "TIMESTAMP",
            "text": "CLOB",
            "blob": "BLOB",
        }
        return type_map.get(type_category, "VARCHAR2(255)")

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table from Oracle data dictionary.

        Args:
            driver: Database driver instance
            table: Table name to query columns for
            schema: Schema name (ignored for USER_TAB_COLUMNS)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column (UPPERCASE in Oracle)
                - data_type: Oracle data type
                - data_length: Maximum length (for character types)
                - nullable: 'Y' or 'N'
        """

        oracle_driver = cast("OracleSyncDriver", driver)
        result = oracle_driver.execute(self._get_columns_sql(table, schema))
        return result.data or []

    def list_available_features(self) -> "list[str]":
        """List available Oracle feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "is_autonomous",
            "supports_native_json",
            "supports_oson_blob",
            "supports_json_blob",
            "supports_json",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
        ]


class OracleAsyncDataDictionary(OracleDataDictionaryMixin, AsyncDataDictionaryBase):
    """Oracle-specific async data dictionary."""

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "OracleVersionInfo | None":
        """Get Oracle database version information.

        Args:
            driver: Async database driver instance

        Returns:
            Oracle version information or None if detection fails
        """
        banner = await cast("OracleAsyncDriver", driver).select_value(
            "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'"
        )

        version_match = ORACLE_VERSION_PATTERN.search(str(banner))

        if not version_match:
            logger.warning("Could not parse Oracle version from banner: %s", banner)
            return None

        major = int(version_match.group(1))
        release_major = int(version_match.group(2))
        minor = int(version_match.group(3))
        patch = int(version_match.group(4))

        if major >= ORACLE_MIN_JSON_NATIVE_VERSION:
            version_info = OracleVersionInfo(major, minor, patch)
        else:
            version_info = OracleVersionInfo(release_major, minor, patch)

        # Enhance with additional information
        oracle_driver = cast("OracleAsyncDriver", driver)
        compatible = await self._get_oracle_compatible_async(oracle_driver)
        is_autonomous = await self._is_oracle_autonomous_async(oracle_driver)

        version_info.compatible = compatible
        version_info.is_autonomous = is_autonomous

        logger.debug("Detected Oracle version: %s", version_info)
        return version_info

    async def _get_oracle_compatible_async(self, driver: "OracleAsyncDriver") -> "str | None":
        """Get Oracle compatible parameter value (async version).

        Args:
            driver: Async database driver instance

        Returns:
            Compatible parameter value or None if detection fails
        """
        try:
            compatible = await driver.select_value("SELECT value FROM v$parameter WHERE name = 'compatible'")
            logger.debug("Detected Oracle compatible parameter: %s", compatible)
            return str(compatible)
        except Exception:
            logger.warning("Compatible parameter not found")
            return None

    async def _is_oracle_autonomous_async(self, driver: "OracleAsyncDriver") -> bool:
        """Check if this is an Oracle Autonomous Database (async version).

        Args:
            driver: Async database driver instance

        Returns:
            True if this is an Autonomous Database, False otherwise
        """
        # Check for cloud_identity in v$pdbs (most reliable for Autonomous)
        with suppress(Exception):
            result = await driver.execute("SELECT COUNT(*) as cnt FROM v$pdbs WHERE cloud_identity IS NOT NULL")
            if result.data:
                count = result.data[0]["cnt"] if isinstance(result.data[0], dict) else result.data[0][0]
                if int(count) > 0:
                    logger.debug("Detected Oracle Autonomous Database via v$pdbs")
                    return True

        logger.debug("Oracle Autonomous Database not detected")
        return False

    async def get_feature_flag(self, driver: AsyncDriverAdapterBase, feature: str) -> bool:
        """Check if Oracle database supports a specific feature.

        Args:
            driver: Async database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        if feature == "is_autonomous":
            return await self._is_oracle_autonomous_async(cast("OracleAsyncDriver", driver))

        version_info = await self.get_version(driver)
        if not version_info:
            return False

        feature_checks: dict[str, Callable[..., bool]] = {
            "supports_native_json": version_info.supports_native_json,
            "supports_oson_blob": version_info.supports_oson_blob,
            "supports_json_blob": version_info.supports_json_blob,
            "supports_json": version_info.supports_json_blob,  # Any JSON support
            "supports_transactions": lambda: True,
            "supports_prepared_statements": lambda: True,
            "supports_schemas": lambda: True,
        }

        if feature in feature_checks:
            return bool(feature_checks[feature]())

        return False

    async def get_optimal_type(self, driver: AsyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal Oracle type for a category.

        Args:
            driver: Async database driver instance
            type_category: Type category

        Returns:
            Oracle-specific type name
        """
        if type_category == "json":
            version_info = await self.get_version(driver)
            return self._get_oracle_json_type(version_info)

        # Other Oracle-specific type mappings
        type_map = {"uuid": "RAW(16)", "boolean": "NUMBER(1)", "timestamp": "TIMESTAMP", "text": "CLOB", "blob": "BLOB"}
        return type_map.get(type_category, "VARCHAR2(255)")

    async def get_columns(
        self, driver: AsyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table from Oracle data dictionary.

        Args:
            driver: Async database driver instance
            table: Table name to query columns for
            schema: Schema name (ignored for USER_TAB_COLUMNS)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column (UPPERCASE in Oracle)
                - data_type: Oracle data type
                - data_length: Maximum length (for character types)
                - nullable: 'Y' or 'N'
        """

        oracle_driver = cast("OracleAsyncDriver", driver)
        result = await oracle_driver.execute(self._get_columns_sql(table, schema))
        return result.data or []

    def list_available_features(self) -> "list[str]":
        """List available Oracle feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "is_autonomous",
            "supports_native_json",
            "supports_oson_blob",
            "supports_json_blob",
            "supports_json",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
        ]
