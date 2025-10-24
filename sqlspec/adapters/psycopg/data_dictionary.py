"""PostgreSQL-specific data dictionary for metadata queries via psycopg."""

import re
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

    from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver

logger = get_logger("adapters.psycopg.data_dictionary")

# Compiled regex patterns
POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")

__all__ = ("PostgresAsyncDataDictionary", "PostgresSyncDataDictionary")


class PostgresSyncDataDictionary(SyncDataDictionaryBase):
    """PostgreSQL-specific sync data dictionary."""

    def get_version(self, driver: SyncDriverAdapterBase) -> "VersionInfo | None":
        """Get PostgreSQL database version information.

        Args:
            driver: Sync database driver instance

        Returns:
            PostgreSQL version information or None if detection fails
        """
        version_str = cast("PsycopgSyncDriver", driver).select_value("SELECT version()")
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

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if PostgreSQL database supports a specific feature.

        Args:
            driver: Sync database driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = self.get_version(driver)
        if not version_info:
            return False

        feature_checks: dict[str, Callable[[VersionInfo], bool]] = {
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
            return bool(feature_checks[feature](version_info))

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal PostgreSQL type for a category.

        Args:
            driver: Sync database driver instance
            type_category: Type category

        Returns:
            PostgreSQL-specific type name
        """
        version_info = self.get_version(driver)

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

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table using pg_catalog.

        Args:
            driver: Psycopg sync driver instance
            table: Table name to query columns for
            schema: Schema name (None for default 'public')

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: PostgreSQL data type
                - is_nullable: Whether column allows NULL (YES/NO)
                - column_default: Default value if any

        Notes:
            Uses pg_catalog instead of information_schema to avoid potential
            issues with PostgreSQL 'name' type in some drivers.
        """
        psycopg_driver = cast("PsycopgSyncDriver", driver)

        schema_name = schema or "public"
        sql = """
            SELECT
                a.attname::text AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS column_default
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            WHERE c.relname = %s
                AND n.nspname = %s
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY a.attnum
        """

        result = psycopg_driver.execute(sql, (table, schema_name))
        return result.data or []

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


class PostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    """PostgreSQL-specific async data dictionary."""

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "VersionInfo | None":
        """Get PostgreSQL database version information.

        Args:
            driver: Async database driver instance

        Returns:
            PostgreSQL version information or None if detection fails
        """
        version_str = await cast("PsycopgAsyncDriver", driver).select_value("SELECT version()")
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

        feature_checks: dict[str, Callable[[VersionInfo], bool]] = {
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
            return bool(feature_checks[feature](version_info))

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

    async def get_columns(
        self, driver: AsyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table using pg_catalog.

        Args:
            driver: Psycopg async driver instance
            table: Table name to query columns for
            schema: Schema name (None for default 'public')

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: PostgreSQL data type
                - is_nullable: Whether column allows NULL (YES/NO)
                - column_default: Default value if any

        Notes:
            Uses pg_catalog instead of information_schema to avoid potential
            issues with PostgreSQL 'name' type in some drivers.
        """
        psycopg_driver = cast("PsycopgAsyncDriver", driver)

        schema_name = schema or "public"
        sql = """
            SELECT
                a.attname::text AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS column_default
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            WHERE c.relname = %s
                AND n.nspname = %s
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY a.attnum
        """

        result = await psycopg_driver.execute(sql, (table, schema_name))
        return result.data or []

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
