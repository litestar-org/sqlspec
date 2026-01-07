"""ADBC multi-dialect data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import ForeignKeyMetadata, SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
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

    __slots__ = ()

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata based on detected dialect."""

        dialect = self._get_dialect(driver)
        adbc_driver = cast("AdbcDriver", driver)

        if dialect == "sqlite":
            if table:
                # Single table
                result = adbc_driver.execute(f"PRAGMA foreign_key_list('{table}')")
                # SQLite PRAGMA returns: id, seq, table, from, to, on_update, on_delete, match
                # We need 'from' (col) and 'table' (ref_table) and 'to' (ref_col)
                # Note: PRAGMA results from ADBC might be keyed by name or index depending on driver
                return [
                    ForeignKeyMetadata(
                        table_name=table,
                        column_name=row["from"] if isinstance(row, dict) else row[3],
                        referenced_table=row["table"] if isinstance(row, dict) else row[2],
                        referenced_column=row["to"] if isinstance(row, dict) else row[4],
                    )
                    for row in result.data
                ]
            # For all tables in SQLite we'd have to iterate, which base class doesn't do efficiently.
            # We'll just return empty if no table specified for now, or iterate if crucial.
            # Base implementation will call this per-table if needed? No, base implementation expects all if table is None.
            # For SQLite ADBC, iterating tables is expensive. Let's support single table primarily.
            return []

        # SQL-standard compliant databases (Postgres, MySQL, DuckDB, BigQuery)
        # They all support information_schema.key_column_usage roughly the same way

        # Postgres/DuckDB/MySQL query
        params = []

        if dialect == "bigquery":
            dataset = schema
            if not dataset:
                return []  # BigQuery requires dataset for info schema
            kcu = f"`{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
            rc = f"`{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
            # BQ uses named params usually or positional? ADBC usually positional '?'
            # But BQ driver might want named. ADBC standardizes on '?' usually.
            sql = f"""
                SELECT
                    kcu.table_name,
                    kcu.column_name,
                    pk_kcu.table_name AS referenced_table_name,
                    pk_kcu.column_name AS referenced_column_name,
                    kcu.constraint_name,
                    kcu.table_schema,
                    pk_kcu.table_schema AS referenced_table_schema
                FROM {kcu} kcu
                JOIN {rc} rc ON kcu.constraint_name = rc.constraint_name
                JOIN {kcu} pk_kcu
                  ON rc.unique_constraint_name = pk_kcu.constraint_name
                  AND kcu.ordinal_position = pk_kcu.ordinal_position
            """
            if table:
                sql += f" WHERE kcu.table_name = '{table}'"  # Simple string sub for BQ ADBC safety check needed?

            try:
                result = adbc_driver.execute(sql)
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
            except Exception:
                return []

        # Standard ANSI SQL (Postgres, MySQL, DuckDB)
        kcu = "information_schema.key_column_usage"

        if dialect == "postgres":
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
            """
            if schema:
                sql += " AND tc.table_schema = ?"
                params.append(schema)
            if table:
                sql += " AND tc.table_name = ?"
                params.append(table)

        elif dialect == "mysql":
            # MySQL information_schema
            sql = """
                SELECT
                    table_name,
                    column_name,
                    referenced_table_name,
                    referenced_column_name,
                    constraint_name,
                    table_schema,
                    referenced_table_schema
                FROM information_schema.key_column_usage
                WHERE referenced_table_name IS NOT NULL
            """
            if schema:
                sql += " AND table_schema = ?"
                params.append(schema)
            if table:
                sql += " AND table_name = ?"
                params.append(table)

        elif dialect == "duckdb":
            # DuckDB similar to Postgres but sometimes requires referential_constraints join
            sql = """
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
                WHERE 1=1
            """
            if schema:
                sql += " AND kcu.table_schema = ?"
                params.append(schema)
            if table:
                sql += " AND kcu.table_name = ?"
                params.append(table)
        else:
            return []

        try:
            result = adbc_driver.execute(sql, tuple(params))
            return [
                ForeignKeyMetadata(
                    table_name=row["table_name"],
                    column_name=row["column_name"],
                    referenced_table=row["referenced_table_name"],
                    referenced_column=row["referenced_column_name"],
                    constraint_name=row["constraint_name"],
                    schema=row.get("table_schema"),
                    referenced_schema=row.get("referenced_table_schema"),
                )
                for row in result.data
            ]
        except Exception:
            return []

    def _get_dialect(self, driver: SyncDriverAdapterBase) -> str:
        """Get dialect from ADBC driver.

        Args:
            driver: ADBC driver instance

        Returns:
            Dialect name
        """
        return str(cast("AdbcDriver", driver).dialect)

    def get_version(self, driver: SyncDriverAdapterBase) -> "VersionInfo | None":
        """Get database version information based on detected dialect.

        Uses caching to avoid repeated database queries within the same
        driver session.

        Args:
            driver: ADBC driver instance.

        Returns:
            Database version information or None if detection fails.
        """
        driver_id = id(driver)
        was_cached, cached_version = self.get_cached_version(driver_id)
        if was_cached:
            return cached_version

        dialect = self._get_dialect(driver)
        adbc_driver = cast("AdbcDriver", driver)
        version_info: VersionInfo | None = None

        try:
            if dialect == "postgres":
                version_str = adbc_driver.select_value("SELECT version()")
                if version_str:
                    match = POSTGRES_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major = int(match.group(1))
                        minor = int(match.group(2))
                        patch = int(match.group(3)) if match.group(3) else 0
                        version_info = VersionInfo(major, minor, patch)

            elif dialect == "sqlite":
                version_str = adbc_driver.select_value("SELECT sqlite_version()")
                if version_str:
                    match = SQLITE_VERSION_PATTERN.match(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        version_info = VersionInfo(major, minor, patch)

            elif dialect == "duckdb":
                version_str = adbc_driver.select_value("SELECT version()")
                if version_str:
                    match = DUCKDB_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        version_info = VersionInfo(major, minor, patch)

            elif dialect == "mysql":
                version_str = adbc_driver.select_value("SELECT VERSION()")
                if version_str:
                    match = MYSQL_VERSION_PATTERN.search(str(version_str))
                    if match:
                        major, minor, patch = map(int, match.groups())
                        version_info = VersionInfo(major, minor, patch)

            elif dialect == "bigquery":
                version_info = VersionInfo(1, 0, 0)

        except Exception:
            logger.warning("Failed to get %s version", dialect)

        self.cache_version(driver_id, version_info)
        return version_info

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

        feature_versions: dict[str, VersionInfo] = {}
        feature_flags: dict[str, bool] = {}

        if dialect == "postgres":
            feature_versions = {
                "supports_json": VersionInfo(9, 2, 0),
                "supports_jsonb": VersionInfo(9, 4, 0),
                "supports_returning": VersionInfo(8, 2, 0),
                "supports_upsert": VersionInfo(9, 5, 0),
                "supports_window_functions": VersionInfo(8, 4, 0),
                "supports_cte": VersionInfo(8, 4, 0),
            }
            feature_flags = {
                "supports_uuid": True,
                "supports_arrays": True,
                "supports_transactions": True,
                "supports_prepared_statements": True,
                "supports_schemas": True,
            }
        elif dialect == "sqlite":
            feature_versions = {
                "supports_json": VersionInfo(3, 38, 0),
                "supports_returning": VersionInfo(3, 35, 0),
                "supports_upsert": VersionInfo(3, 24, 0),
                "supports_window_functions": VersionInfo(3, 25, 0),
                "supports_cte": VersionInfo(3, 8, 3),
            }
            feature_flags = {
                "supports_transactions": True,
                "supports_prepared_statements": True,
                "supports_schemas": False,
                "supports_arrays": False,
                "supports_uuid": False,
            }
        elif dialect == "duckdb":
            feature_versions = {"supports_returning": VersionInfo(0, 8, 0), "supports_upsert": VersionInfo(0, 8, 0)}
            feature_flags = {
                "supports_json": True,
                "supports_arrays": True,
                "supports_uuid": True,
                "supports_window_functions": True,
                "supports_cte": True,
                "supports_transactions": True,
                "supports_prepared_statements": True,
                "supports_schemas": True,
            }
        elif dialect == "mysql":
            feature_versions = {
                "supports_json": VersionInfo(5, 7, 8),
                "supports_cte": VersionInfo(8, 0, 1),
                "supports_window_functions": VersionInfo(8, 0, 2),
            }
            feature_flags = {
                "supports_returning": False,
                "supports_upsert": True,
                "supports_transactions": True,
                "supports_prepared_statements": True,
                "supports_schemas": True,
                "supports_uuid": False,
                "supports_arrays": False,
            }
        elif dialect == "bigquery":
            feature_flags = {
                "supports_json": True,
                "supports_arrays": True,
                "supports_structs": True,
                "supports_returning": False,
                "supports_upsert": True,
                "supports_window_functions": True,
                "supports_cte": True,
                "supports_transactions": False,
                "supports_prepared_statements": True,
                "supports_schemas": True,
                "supports_uuid": False,
            }
        else:
            feature_flags = {
                "supports_transactions": True,
                "supports_prepared_statements": True,
                "supports_window_functions": True,
                "supports_cte": True,
            }

        if feature in feature_versions:
            if not version_info:
                return False
            return bool(version_info >= feature_versions[feature])
        if feature in feature_flags:
            return feature_flags[feature]

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

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table based on detected dialect.

        Args:
            driver: ADBC driver instance
            table: Table name to query columns for
            schema: Schema name (None for default)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: Database data type
                - is_nullable or nullable: Whether column allows NULL
                - column_default or default_value: Default value if any
        """
        dialect = self._get_dialect(driver)
        adbc_driver = cast("AdbcDriver", driver)

        if dialect == "sqlite":
            result = adbc_driver.execute(f"PRAGMA table_info({table})")
            return [
                {
                    "column_name": row["name"] if isinstance(row, dict) else row[1],
                    "data_type": row["type"] if isinstance(row, dict) else row[2],
                    "nullable": not (row["notnull"] if isinstance(row, dict) else row[3]),
                    "default_value": row["dflt_value"] if isinstance(row, dict) else row[4],
                }
                for row in result.data or []
            ]

        if dialect == "postgres":
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
                WHERE c.relname = ?
                    AND n.nspname = ?
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY a.attnum
            """
            result = adbc_driver.execute(sql, (table, schema_name))
            return result.data or []

        if schema:
            sql = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = ? AND table_schema = ?
                ORDER BY ordinal_position
            """
            result = adbc_driver.execute(sql, (table, schema))
        else:
            sql = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
            """
            result = adbc_driver.execute(sql, (table,))

        return result.data or []

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
