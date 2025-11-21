"""PostgreSQL-specific data dictionary for metadata queries via psycopg."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    ForeignKeyMetadata,
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

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using Recursive CTE."""
        psycopg_driver = cast("PsycopgSyncDriver", driver)
        schema_name = schema or "public"

        sql = """
        WITH RECURSIVE dependency_tree AS (
            SELECT
                t.table_name::text,
                0 AS level,
                ARRAY[t.table_name::text] AS path
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema = %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                  WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = t.table_name
                    AND tc.table_schema = t.table_schema
              )

            UNION ALL

            SELECT
                tc.table_name::text,
                dt.level + 1,
                dt.path || tc.table_name::text
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            JOIN dependency_tree dt
              ON ccu.table_name = dt.table_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND ccu.table_schema = %s
              AND NOT (tc.table_name = ANY(dt.path))
        )
        SELECT DISTINCT table_name, level
        FROM dependency_tree
        ORDER BY level, table_name;
        """
        result = psycopg_driver.execute(sql, (schema_name, schema_name, schema_name))
        return [row["table_name"] for row in result.data]

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

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        psycopg_driver = cast("PsycopgSyncDriver", driver)
        schema_name = schema or "public"

        sql = """
            SELECT
                kcu.table_name,
                kcu.column_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name,
                tc.constraint_name,
                tc.table_schema,
                ccu.table_schema AS referenced_table_schema
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND (%s::text IS NULL OR tc.table_schema = %s)
              AND (%s::text IS NULL OR tc.table_name = %s)
        """

        result = psycopg_driver.execute(sql, (schema_name, schema_name, table, table))

        return [
            ForeignKeyMetadata(
                table_name=row["table_name"],
                column_name=row["column_name"],
                referenced_table=row["referenced_table_name"],
                referenced_column=row["referenced_column_name"],
                constraint_name=row["constraint_name"],
                schema=row["table_schema"],
                referenced_schema=row["referenced_table_schema"],
            )
            for row in result.data
        ]

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        psycopg_driver = cast("PsycopgSyncDriver", driver)
        schema_name = schema or "public"

        sql = """
            SELECT
                i.relname as index_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a,
                pg_namespace n
            WHERE
                t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relnamespace = n.oid
                AND n.nspname = %s
                AND t.relname = %s
            GROUP BY
                i.relname,
                ix.indisunique,
                ix.indisprimary
        """
        result = psycopg_driver.execute(sql, (schema_name, table))

        return [
            {
                "name": row["index_name"],
                "columns": row["columns"],
                "unique": row["is_unique"],
                "primary": row["is_primary"],
                "table_name": table,
            }
            for row in result.data
        ]

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

    async def get_tables(self, driver: "AsyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using Recursive CTE."""
        psycopg_driver = cast("PsycopgAsyncDriver", driver)
        schema_name = schema or "public"

        sql = """
        WITH RECURSIVE dependency_tree AS (
            SELECT
                t.table_name::text,
                0 AS level,
                ARRAY[t.table_name::text] AS path
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema = %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                  WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = t.table_name
                    AND tc.table_schema = t.table_schema
              )

            UNION ALL

            SELECT
                tc.table_name::text,
                dt.level + 1,
                dt.path || tc.table_name::text
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            JOIN dependency_tree dt
              ON ccu.table_name = dt.table_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND ccu.table_schema = %s
              AND NOT (tc.table_name = ANY(dt.path))
        )
        SELECT DISTINCT table_name, level
        FROM dependency_tree
        ORDER BY level, table_name;
        """
        result = await psycopg_driver.execute(sql, (schema_name, schema_name, schema_name))
        return [row["table_name"] for row in result.data]

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

    async def get_foreign_keys(
        self, driver: "AsyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        psycopg_driver = cast("PsycopgAsyncDriver", driver)
        schema_name = schema or "public"

        sql = """
            SELECT
                kcu.table_name,
                kcu.column_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name,
                tc.constraint_name,
                tc.table_schema,
                ccu.table_schema AS referenced_table_schema
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND (%s::text IS NULL OR tc.table_schema = %s)
              AND (%s::text IS NULL OR tc.table_name = %s)
        """

        result = await psycopg_driver.execute(sql, (schema_name, schema_name, table, table))

        return [
            ForeignKeyMetadata(
                table_name=row["table_name"],
                column_name=row["column_name"],
                referenced_table=row["referenced_table_name"],
                referenced_column=row["referenced_column_name"],
                constraint_name=row["constraint_name"],
                schema=row["table_schema"],
                referenced_schema=row["referenced_table_schema"],
            )
            for row in result.data
        ]

    async def get_indexes(
        self, driver: "AsyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        psycopg_driver = cast("PsycopgAsyncDriver", driver)
        schema_name = schema or "public"

        sql = """
            SELECT
                i.relname as index_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a,
                pg_namespace n
            WHERE
                t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relnamespace = n.oid
                AND n.nspname = %s
                AND t.relname = %s
            GROUP BY
                i.relname,
                ix.indisunique,
                ix.indisprimary
        """
        result = await psycopg_driver.execute(sql, (schema_name, table))

        return [
            {
                "name": row["index_name"],
                "columns": row["columns"],
                "unique": row["is_unique"],
                "primary": row["is_primary"],
                "table_name": table,
            }
            for row in result.data
        ]

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
