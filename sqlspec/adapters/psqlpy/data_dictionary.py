"""PostgreSQL-specific data dictionary for metadata queries via psqlpy."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import AsyncDataDictionaryBase, AsyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy.driver import PsqlpyDriver

logger = get_logger("adapters.psqlpy.data_dictionary")

# Compiled regex patterns
POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")

__all__ = ("PsqlpyAsyncDataDictionary",)


class PsqlpyAsyncDataDictionary(AsyncDataDictionaryBase):
    """PostgreSQL-specific async data dictionary via psqlpy."""

    __slots__ = ()

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "VersionInfo | None":
        """Get PostgreSQL database version information.

        Uses caching to avoid repeated database queries within the same
        driver session.

        Args:
            driver: Async database driver instance.

        Returns:
            PostgreSQL version information or None if detection fails.
        """
        driver_id = id(driver)
        was_cached, cached_version = self.get_cached_version(driver_id)
        if was_cached:
            return cached_version

        version_str = await cast("PsqlpyDriver", driver).select_value("SELECT version()")
        if not version_str:
            logger.warning("No PostgreSQL version information found")
            self.cache_version(driver_id, None)
            return None

        version_match = POSTGRES_VERSION_PATTERN.search(str(version_str))
        if not version_match:
            logger.warning("Could not parse PostgreSQL version: %s", version_str)
            self.cache_version(driver_id, None)
            return None

        major = int(version_match.group(1))
        minor = int(version_match.group(2))
        patch = int(version_match.group(3)) if version_match.group(3) else 0

        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected PostgreSQL version: %s", version_info)
        self.cache_version(driver_id, version_info)
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

        feature_versions: dict[str, VersionInfo] = {
            "supports_json": VersionInfo(9, 2, 0),
            "supports_jsonb": VersionInfo(9, 4, 0),
            "supports_returning": VersionInfo(8, 2, 0),
            "supports_upsert": VersionInfo(9, 5, 0),
            "supports_window_functions": VersionInfo(8, 4, 0),
            "supports_cte": VersionInfo(8, 4, 0),
            "supports_partitioning": VersionInfo(10, 0, 0),
        }
        feature_flags: dict[str, bool] = {
            "supports_uuid": True,
            "supports_arrays": True,
            "supports_transactions": True,
            "supports_prepared_statements": True,
            "supports_schemas": True,
        }

        if feature in feature_versions:
            return bool(version_info >= feature_versions[feature])
        if feature in feature_flags:
            return feature_flags[feature]

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
            driver: Psqlpy async driver instance
            table: Table name to query columns for
            schema: Schema name (None for default 'public')

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: PostgreSQL data type
                - is_nullable: Whether column allows NULL (YES/NO)
                - column_default: Default value if any

        Notes:
            Uses pg_catalog instead of information_schema to avoid psqlpy's
            inability to handle the PostgreSQL 'name' type returned by information_schema.
        """
        psqlpy_driver = cast("PsqlpyDriver", driver)

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
            WHERE c.relname = $1
                AND n.nspname = $2
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY a.attnum
        """

        result = await psqlpy_driver.execute(sql, (table, schema_name))
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
