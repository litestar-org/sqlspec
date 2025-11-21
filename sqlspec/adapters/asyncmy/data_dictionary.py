"""MySQL-specific data dictionary for metadata queries via asyncmy."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import AsyncDataDictionaryBase, AsyncDriverAdapterBase, ForeignKeyMetadata, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

logger = get_logger("adapters.asyncmy.data_dictionary")

# Compiled regex patterns
VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

__all__ = ("MySQLAsyncDataDictionary",)


class MySQLAsyncDataDictionary(AsyncDataDictionaryBase):
    """MySQL-specific async data dictionary."""

    async def get_version(self, driver: AsyncDriverAdapterBase) -> "VersionInfo | None":
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

        feature_checks: dict[str, Callable[..., bool]] = {
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
            return bool(feature_checks[feature](version_info))

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

    async def get_tables(self, driver: "AsyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using MySQL catalog.

        Requires MySQL 8.0.1+ for recursive CTE support.
        """
        version = await self.get_version(driver)
        asyncmy_driver = cast("AsyncmyDriver", driver)

        if not version or version < VersionInfo(8, 0, 1):
            msg = "get_tables requires MySQL 8.0.1+ for dependency ordering"
            raise RuntimeError(msg)

        schema_clause = f"'{schema}'" if schema else "DATABASE()"

        sql = f"""
        WITH RECURSIVE dependency_tree AS (
            SELECT
                table_name,
                0 AS level,
                CAST(table_name AS CHAR(4000)) AS path
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema = {schema_clause}
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.key_column_usage kcu
                  WHERE kcu.table_name = t.table_name
                    AND kcu.table_schema = t.table_schema
                    AND kcu.referenced_table_name IS NOT NULL
              )

            UNION ALL

            SELECT
                kcu.table_name,
                dt.level + 1,
                CONCAT(dt.path, ',', kcu.table_name)
            FROM information_schema.key_column_usage kcu
            JOIN dependency_tree dt ON kcu.referenced_table_name = dt.table_name
            WHERE kcu.table_schema = {schema_clause}
              AND kcu.referenced_table_name IS NOT NULL
              AND NOT FIND_IN_SET(kcu.table_name, dt.path)
        )
        SELECT DISTINCT table_name
        FROM dependency_tree
        ORDER BY level, table_name
        """
        result = await asyncmy_driver.execute(sql)
        return [row["table_name"] for row in result.get_data()]

    async def get_foreign_keys(
        self, driver: "AsyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        asyncmy_driver = cast("AsyncmyDriver", driver)

        where_clauses = ["referenced_table_name IS NOT NULL"]
        if table:
            where_clauses.append(f"table_name = '{table}'")
        if schema:
            where_clauses.append(f"table_schema = '{schema}'")
        else:
            where_clauses.append("table_schema = DATABASE()")

        where_str = " AND ".join(where_clauses)

        sql = f"""
            SELECT
                table_name,
                column_name,
                referenced_table_name,
                referenced_column_name,
                constraint_name,
                table_schema,
                referenced_table_schema
            FROM information_schema.key_column_usage
            WHERE {where_str}
        """
        result = await asyncmy_driver.execute(sql)

        return [
            ForeignKeyMetadata(
                table_name=row["table_name"],
                column_name=row["column_name"],
                referenced_table=row["referenced_table_name"],
                referenced_column=row["referenced_column_name"],
                constraint_name=row["constraint_name"],
                schema=row["table_schema"],
                referenced_schema=row.get("referenced_table_schema"),
            )
            for row in result.data
        ]

    async def get_indexes(
        self, driver: "AsyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        asyncmy_driver = cast("AsyncmyDriver", driver)
        sql = f"SHOW INDEX FROM {table}" if schema is None else f"SHOW INDEX FROM {table} FROM {schema}"

        result = await asyncmy_driver.execute(sql)
        # Parse SHOW INDEX output
        indexes: dict[str, dict[str, Any]] = {}
        for row in result.data:
            idx_name = row["Key_name"]
            if idx_name not in indexes:
                indexes[idx_name] = {
                    "name": idx_name,
                    "columns": [],
                    "unique": row["Non_unique"] == 0,
                    "primary": idx_name == "PRIMARY",
                    "table_name": table,
                }
            indexes[idx_name]["columns"].append(row["Column_name"])

        return list(indexes.values())

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
