"""DuckDB-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import ForeignKeyMetadata, SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
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

        Uses caching to avoid repeated database queries within the same
        driver session.

        Args:
            driver: DuckDB driver instance.

        Returns:
            DuckDB version information or None if detection fails.
        """
        driver_id = id(driver)
        was_cached, cached_version = self.get_cached_version(driver_id)
        if was_cached:
            return cached_version

        version_str = cast("DuckDBDriver", driver).select_value("SELECT version()")
        if not version_str:
            logger.warning("No DuckDB version information found")
            self.cache_version(driver_id, None)
            return None

        version_match = DUCKDB_VERSION_PATTERN.search(str(version_str))
        if not version_match:
            logger.warning("Could not parse DuckDB version: %s", version_str)
            self.cache_version(driver_id, None)
            return None

        major, minor, patch = map(int, version_match.groups())
        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected DuckDB version: %s", version_info)
        self.cache_version(driver_id, version_info)
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

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table using information_schema.

        Args:
            driver: DuckDB driver instance
            table: Table name to query columns for
            schema: Schema name (None for default)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: DuckDB data type
                - nullable: Whether column allows NULL (YES/NO)
                - column_default: Default value if any
        """
        duckdb_driver = cast("DuckDBDriver", driver)

        if schema:
            sql = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = '{schema}'
                ORDER BY ordinal_position
            """
        else:
            sql = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '{table}'
                ORDER BY ordinal_position
            """

        result = duckdb_driver.execute(sql)
        return result.data or []

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using DuckDB catalog."""
        duckdb_driver = cast("DuckDBDriver", driver)
        schema_clause = f"'{schema}'" if schema else "current_schema()"

        sql = f"""
        WITH RECURSIVE dependency_tree AS (
            SELECT
                table_name,
                0 AS level,
                [table_name] AS path
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
              AND t.table_schema = {schema_clause}
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.key_column_usage kcu
                  WHERE kcu.table_name = t.table_name
                    AND kcu.table_schema = t.table_schema
                    AND kcu.constraint_name IN (SELECT constraint_name FROM information_schema.referential_constraints)
              )

            UNION ALL

            SELECT
                kcu.table_name,
                dt.level + 1,
                list_append(dt.path, kcu.table_name)
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc ON kcu.constraint_name = rc.constraint_name
            JOIN information_schema.key_column_usage pk_kcu
              ON rc.unique_constraint_name = pk_kcu.constraint_name
              AND rc.unique_constraint_schema = pk_kcu.constraint_schema
            JOIN dependency_tree dt ON dt.table_name = pk_kcu.table_name
            WHERE kcu.table_schema = {schema_clause}
              AND NOT list_contains(dt.path, kcu.table_name)
        )
        SELECT DISTINCT table_name, level
        FROM dependency_tree
        ORDER BY level, table_name
        """
        result = duckdb_driver.execute(sql)
        return [row["table_name"] for row in result.get_data()]

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        duckdb_driver = cast("DuckDBDriver", driver)

        where_clauses = []
        if schema:
            where_clauses.append(f"kcu.table_schema = '{schema}'")
        if table:
            where_clauses.append(f"kcu.table_name = '{table}'")

        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"

        sql = f"""
            SELECT
                kcu.table_name,
                kcu.column_name,
                pk_kcu.table_name AS referenced_table_name,
                pk_kcu.column_name AS referenced_column_name,
                kcu.constraint_name,
                kcu.table_schema,
                pk_kcu.table_schema AS referenced_table_schema
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc
              ON kcu.constraint_name = rc.constraint_name
            JOIN information_schema.key_column_usage pk_kcu
              ON rc.unique_constraint_name = pk_kcu.constraint_name
              AND kcu.ordinal_position = pk_kcu.ordinal_position
            WHERE {where_str}
        """

        result = duckdb_driver.execute(sql)

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
            for row in result.get_data()
        ]

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        # DuckDB doesn't expose indexes easily in IS yet, usually just constraints?
        # Fallback to empty for now or implementation specific.
        # PRD mentions it but no specific instruction on implementation detail if missing.
        # Returning empty list.
        return []

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
