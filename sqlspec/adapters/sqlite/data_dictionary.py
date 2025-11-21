"""SQLite-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import ForeignKeyMetadata, SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.sqlite.driver import SqliteDriver

logger = get_logger("adapters.sqlite.data_dictionary")

# Compiled regex patterns
SQLITE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

__all__ = ("SqliteSyncDataDictionary",)


class SqliteSyncDataDictionary(SyncDataDictionaryBase):
    """SQLite-specific sync data dictionary."""

    def get_version(self, driver: SyncDriverAdapterBase) -> "VersionInfo | None":
        """Get SQLite database version information.

        Args:
            driver: Sync database driver instance

        Returns:
            SQLite version information or None if detection fails
        """
        version_str = cast("SqliteDriver", driver).select_value("SELECT sqlite_version()")
        if not version_str:
            logger.warning("No SQLite version information found")
            return None

        # Parse version like "3.45.0"
        version_match = SQLITE_VERSION_PATTERN.match(str(version_str))
        if not version_match:
            logger.warning("Could not parse SQLite version: %s", version_str)
            return None

        major, minor, patch = map(int, version_match.groups())
        version_info = VersionInfo(major, minor, patch)
        logger.debug("Detected SQLite version: %s", version_info)
        return version_info

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: SQLite driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        version_info = self.get_version(driver)
        if not version_info:
            return False

        feature_checks: dict[str, Callable[[VersionInfo], bool]] = {
            "supports_json": lambda v: v >= VersionInfo(3, 38, 0),
            "supports_returning": lambda v: v >= VersionInfo(3, 35, 0),
            "supports_upsert": lambda v: v >= VersionInfo(3, 24, 0),
            "supports_window_functions": lambda v: v >= VersionInfo(3, 25, 0),
            "supports_cte": lambda v: v >= VersionInfo(3, 8, 3),
            "supports_transactions": lambda _: True,
            "supports_prepared_statements": lambda _: True,
            "supports_schemas": lambda _: False,  # SQLite has ATTACH but not schemas
            "supports_arrays": lambda _: False,
            "supports_uuid": lambda _: False,
        }

        if feature in feature_checks:
            return bool(feature_checks[feature](version_info))

        return False

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal SQLite type for a category.

        Args:
            driver: SQLite driver instance
            type_category: Type category

        Returns:
            SQLite-specific type name
        """
        version_info = self.get_version(driver)

        if type_category == "json":
            if version_info and version_info >= VersionInfo(3, 38, 0):
                return "JSON"
            return "TEXT"

        type_map = {"uuid": "TEXT", "boolean": "INTEGER", "timestamp": "TIMESTAMP", "text": "TEXT", "blob": "BLOB"}
        return type_map.get(type_category, "TEXT")

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table using SQLite PRAGMA.

        Args:
            driver: SQLite driver instance
            table: Table name to query columns for
            schema: Schema name (unused in SQLite)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: SQLite data type
                - nullable: Whether column allows NULL
                - default_value: Default value if any
        """
        sqlite_driver = cast("SqliteDriver", driver)
        result = sqlite_driver.execute(f"PRAGMA table_info({table})")

        return [
            {
                "column_name": row["name"] if isinstance(row, dict) else row[1],
                "data_type": row["type"] if isinstance(row, dict) else row[2],
                "nullable": not (row["notnull"] if isinstance(row, dict) else row[3]),
                "default_value": row["dflt_value"] if isinstance(row, dict) else row[4],
            }
            for row in result.data or []
        ]

    def get_tables_in_topological_order(
        self, driver: "SyncDriverAdapterBase", schema: "str | None" = None
    ) -> "list[str]":
        """Get tables sorted by topological dependency order."""
        sqlite_driver = cast("SqliteDriver", driver)

        sql = """
        WITH RECURSIVE dependency_tree AS (
            SELECT
                m.name as table_name,
                0 as level,
                '/' || m.name || '/' as path
            FROM sqlite_schema m
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND NOT EXISTS (
                  SELECT 1 FROM pragma_foreign_key_list(m.name)
              )

            UNION ALL

            SELECT
                m.name as table_name,
                dt.level + 1,
                dt.path || m.name || '/'
            FROM sqlite_schema m
            JOIN pragma_foreign_key_list(m.name) fk
            JOIN dependency_tree dt ON fk."table" = dt.table_name
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND instr(dt.path, '/' || m.name || '/') = 0
        )
        SELECT DISTINCT table_name, level FROM dependency_tree ORDER BY level, table_name;
        """
        try:
            result = sqlite_driver.execute(sql)
            return [row["table_name"] if isinstance(row, dict) else row[0] for row in result.data]
        except Exception:
            return self.sort_tables_topologically(
                self.get_tables(driver, schema), self.get_foreign_keys(driver, schema=schema)
            )

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""

        sqlite_driver = cast("SqliteDriver", driver)

        if table:
            sql = f"SELECT '{table}' as table_name, fk.* FROM pragma_foreign_key_list('{table}') fk"
            result = sqlite_driver.execute(sql)
        else:
            sql = """
                SELECT m.name as table_name, fk.*
                FROM sqlite_schema m, pragma_foreign_key_list(m.name) fk
                WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
            """
            result = sqlite_driver.execute(sql)

        fks = []
        for row in result.data:
            if isinstance(row, (list, tuple)):
                table_name = row[0]
                ref_table = row[3]
                col = row[4]
                ref_col = row[5]
            else:
                table_name = row["table_name"]
                ref_table = row["table"]
                col = row["from"]
                ref_col = row["to"]

            fks.append(
                ForeignKeyMetadata(
                    table_name=table_name,
                    column_name=col,
                    referenced_table=ref_table,
                    referenced_column=ref_col,
                    constraint_name=None,
                    schema=None,
                    referenced_schema=None,
                )
            )
        return fks

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        sqlite_driver = cast("SqliteDriver", driver)

        index_list_res = sqlite_driver.execute(f"PRAGMA index_list('{table}')")
        indexes = []

        for idx_row in index_list_res.data:
            if isinstance(idx_row, (list, tuple)):
                idx_name = idx_row[1]
                unique = bool(idx_row[2])
            else:
                idx_name = idx_row["name"]
                unique = bool(idx_row["unique"])

            info_res = sqlite_driver.execute(f"PRAGMA index_info('{idx_name}')")
            cols = []
            for col_row in info_res.data:
                if isinstance(col_row, (list, tuple)):
                    cols.append(col_row[2])
                else:
                    cols.append(col_row["name"])

            indexes.append({"name": idx_name, "columns": cols, "unique": unique, "primary": False, "table_name": table})

        return indexes

    def list_available_features(self) -> "list[str]":
        """List available SQLite feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_arrays",
            "supports_uuid",
        ]
