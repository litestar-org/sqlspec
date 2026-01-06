"""Oracle-specific data dictionary for metadata queries."""
# cspell:ignore pdbs

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
    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver

logger = get_logger("adapters.oracledb.data_dictionary")

# Oracle version constants
ORACLE_MIN_JSON_NATIVE_VERSION = 21
ORACLE_MIN_JSON_NATIVE_COMPATIBLE = 20
ORACLE_MIN_JSON_BLOB_VERSION = 12
ORACLE_MIN_OSON_VERSION = 19

# Compiled regex patterns
VERSION_NUMBER_PATTERN = re.compile(r"(\d+)")
VERSION_COMPONENT_COUNT = 3

COMPONENT_VERSION_SQL = (
    'SELECT product AS "product", version AS "version", status AS "status" '
    "FROM product_component_version WHERE product LIKE 'Oracle%' "
    "ORDER BY TO_NUMBER(REGEXP_SUBSTR(version, '^[0-9]+')) DESC, version DESC "
    "FETCH FIRST 1 ROWS ONLY"
)

AUTONOMOUS_SERVICE_SQL = "SELECT sys_context('USERENV','CLOUD_SERVICE') AS \"service\" FROM dual"

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
            SELECT
                column_name AS "column_name",
                data_type AS "data_type",
                data_length AS "data_length",
                nullable AS "nullable"
            FROM user_tab_columns
            WHERE table_name = '{table.upper()}'
            ORDER BY column_id
        """

    def _get_foreign_keys_sql(self, table: "str | None" = None) -> str:
        where_clause = f"AND c.table_name = '{table.upper()}'" if table else ""
        return f"""
            SELECT
                c.table_name,
                cc.column_name,
                r.table_name AS referenced_table_name,
                rcc.column_name AS referenced_column_name,
                c.constraint_name,
                c.owner AS schema,
                r.owner AS referenced_schema
            FROM user_constraints c
            JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
            JOIN user_constraints r ON c.r_constraint_name = r.constraint_name
            JOIN user_cons_columns rcc ON r.constraint_name = rcc.constraint_name
            WHERE c.constraint_type = 'R'
              AND cc.position = rcc.position
              {where_clause}
        """

    def _get_indexes_sql(self, table: "str | None" = None) -> str:
        where_clause = f"AND i.table_name = '{table.upper()}'" if table else ""
        return f"""
            SELECT
                i.index_name AS name,
                i.table_name,
                i.uniqueness,
                LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
            FROM user_indexes i
            JOIN user_ind_columns ic ON i.index_name = ic.index_name
            WHERE 1=1 {where_clause}
            GROUP BY i.index_name, i.table_name, i.uniqueness
        """

    def _get_tables_sql(self) -> str:
        return """
            SELECT table_name, MAX(LEVEL) as lvl
            FROM user_constraints
            START WITH table_name NOT IN (
                SELECT table_name FROM user_constraints WHERE constraint_type = 'R'
            )
            CONNECT BY NOCYCLE PRIOR constraint_name = r_constraint_name
            GROUP BY table_name
            ORDER BY lvl, table_name
        """

    def _select_component_version_row(self, driver: "OracleSyncDriver") -> "dict[str, Any] | None":
        """Fetch the latest Oracle component version row.

        Args:
            driver: Oracle sync driver instance.

        Returns:
            First matching row from product_component_version or None.
        """

        result = driver.execute(COMPONENT_VERSION_SQL)
        data = result.get_data()
        if not data:
            logger.warning("No rows returned from product_component_version")
            return None
        return data[0]

    async def _select_component_version_row_async(self, driver: "OracleAsyncDriver") -> "dict[str, Any] | None":
        """Async helper to fetch the latest Oracle component version row.

        Args:
            driver: Oracle async driver instance.

        Returns:
            First matching row from product_component_version or None.
        """

        result = await driver.execute(COMPONENT_VERSION_SQL)
        data = result.get_data()
        if not data:
            logger.warning("No rows returned from product_component_version")
            return None
        return data[0]

    def _build_version_info_from_row(self, row: "dict[str, Any] | None") -> "OracleVersionInfo | None":
        """Build Oracle version metadata from a component version row.

        Args:
            row: Data dictionary row containing product/version fields.

        Returns:
            OracleVersionInfo if parsing succeeds, otherwise None.
        """

        if not row:
            logger.warning("Unable to determine Oracle version without component data")
            return None

        version_value = row.get("version_full") or row.get("VERSION_FULL") or row.get("version") or row.get("VERSION")

        if version_value is None:
            logger.warning("Component version row missing VERSION column: %s", row)
            return None

        matches = VERSION_NUMBER_PATTERN.findall(str(version_value))
        if not matches:
            logger.warning("Unable to parse Oracle version from value: %s", version_value)
            return None

        numbers = [int(match) for match in matches[:VERSION_COMPONENT_COUNT]]
        while len(numbers) < VERSION_COMPONENT_COUNT:
            numbers.append(0)

        version_info = OracleVersionInfo(numbers[0], numbers[1], numbers[2])
        product_name = row.get("product") or row.get("PRODUCT") or "Oracle Database"
        logger.debug("Detected Oracle component version for %s: %s", product_name, version_info)
        return version_info

    def _get_oracle_version(self, driver: "OracleAsyncDriver | OracleSyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information.

        Args:
            driver: Database driver instance

        Returns:
            Oracle version information or None if detection fails
        """
        row = self._select_component_version_row(cast("OracleSyncDriver", driver))
        return self._build_version_info_from_row(row)

    def _get_oracle_compatible(self, driver: "OracleAsyncDriver | OracleSyncDriver") -> "str | None":
        """Get Oracle compatible parameter value.

        Args:
            driver: Database driver instance

        Returns:
            Compatible parameter value or None if detection fails
        """
        try:
            compatible = driver.select_value("SELECT value AS \"value\" FROM v$parameter WHERE name = 'compatible'")
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
        try:
            service = driver.select_value_or_none(AUTONOMOUS_SERVICE_SQL)
        except Exception:
            logger.debug("Unable to detect Oracle cloud service via sys_context")
            return False
        if service is None:
            return False
        normalized = str(service).strip().upper()
        if not normalized:
            return False
        return "AUTONOMOUS" in normalized or normalized.startswith(("ATP", "ADW"))

    def get_version(self, driver: SyncDriverAdapterBase) -> "OracleVersionInfo | None":
        """Get Oracle database version information.

        Uses caching to avoid repeated database queries within the same
        driver session.

        Args:
            driver: Database driver instance.

        Returns:
            Oracle version information or None if detection fails.
        """
        driver_id = id(driver)
        was_cached, cached_version = self.get_cached_version(driver_id)
        if was_cached:
            return cast("OracleVersionInfo | None", cached_version)

        oracle_driver = cast("OracleSyncDriver", driver)
        version_info = self._get_oracle_version(oracle_driver)
        if version_info:
            compatible = self._get_oracle_compatible(oracle_driver)
            is_autonomous = self._is_oracle_autonomous(oracle_driver)

            version_info.compatible = compatible
            version_info.is_autonomous = is_autonomous

        self.cache_version(driver_id, version_info)
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

        feature_flags: "dict[str", bool] = {
            "supports_native_json": version_info.supports_native_json,
            "supports_oson_blob": version_info.supports_oson_blob,
            "supports_json_blob": version_info.supports_json_blob,
            "supports_json": version_info.supports_json_blob,
            "supports_transactions": True,
            "supports_prepared_statements": True,
            "supports_schemas": True,
        }

        if feature in feature_flags:
            return feature_flags[feature]

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
                - column_name: Name of the column
                - data_type: Oracle data type
                - data_length: Maximum length (for character types)
                - nullable: 'Y' or 'N'
        """

        oracle_driver = cast("OracleSyncDriver", driver)
        result = oracle_driver.execute(self._get_columns_sql(table, schema))
        return result.get_data()

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using Oracle CONNECT BY."""
        oracle_driver = cast("OracleSyncDriver", driver)

        result = oracle_driver.execute(self._get_tables_sql())
        sorted_tables = [row["table_name"] for row in result]

        all_result = oracle_driver.execute("SELECT table_name FROM user_tables")
        all_tables = {row["table_name"] for row in all_result.get_data()}

        sorted_set = set(sorted_tables)
        disconnected = list(all_tables - sorted_set)
        disconnected.sort()

        return disconnected + sorted_tables

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        oracle_driver = cast("OracleSyncDriver", driver)
        result = oracle_driver.execute(self._get_foreign_keys_sql(table))

        return [
            ForeignKeyMetadata(
                table_name=row["table_name"],
                column_name=row["column_name"],
                referenced_table=row["referenced_table_name"],
                referenced_column=row["referenced_column_name"],
                constraint_name=row["constraint_name"],
                schema=row["schema"],
                referenced_schema=row["referenced_schema"],
            )
            for row in result.get_data()
        ]

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        oracle_driver = cast("OracleSyncDriver", driver)
        result = oracle_driver.execute(self._get_indexes_sql(table))

        return [
            {
                "name": row["name"],
                "columns": row["columns"].split(",") if row["columns"] else [],
                "unique": row["uniqueness"] == "UNIQUE",
                "primary": False,
                "table_name": row["table_name"],
            }
            for row in result.get_data()
        ]

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

        Uses caching to avoid repeated database queries within the same
        driver session.

        Args:
            driver: Async database driver instance.

        Returns:
            Oracle version information or None if detection fails.
        """
        driver_id = id(driver)
        was_cached, cached_version = self.get_cached_version(driver_id)
        if was_cached:
            return cast("OracleVersionInfo | None", cached_version)

        oracle_driver = cast("OracleAsyncDriver", driver)
        row = await self._select_component_version_row_async(oracle_driver)
        version_info = self._build_version_info_from_row(row)

        if not version_info:
            self.cache_version(driver_id, None)
            return None

        compatible = await self._get_oracle_compatible_async(oracle_driver)
        is_autonomous = await self._is_oracle_autonomous_async(oracle_driver)

        version_info.compatible = compatible
        version_info.is_autonomous = is_autonomous

        logger.debug("Detected Oracle version: %s", version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def _get_oracle_compatible_async(self, driver: "OracleAsyncDriver") -> "str | None":
        """Get Oracle compatible parameter value (async version).

        Args:
            driver: Async database driver instance

        Returns:
            Compatible parameter value or None if detection fails
        """
        try:
            compatible = await driver.select_value(
                "SELECT value AS \"value\" FROM v$parameter WHERE name = 'compatible'"
            )
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
        try:
            service = await driver.select_value_or_none(AUTONOMOUS_SERVICE_SQL)
        except Exception:
            logger.debug("Unable to detect Oracle cloud service via sys_context (async)")
            return False
        if service is None:
            return False
        normalized = str(service).strip().upper()
        if not normalized:
            return False
        if "AUTONOMOUS" in normalized or normalized.startswith(("ATP", "ADW")):
            logger.debug("Detected Oracle Autonomous Database via USERENV context")
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

        feature_flags: "dict[str", bool] = {
            "supports_native_json": version_info.supports_native_json,
            "supports_oson_blob": version_info.supports_oson_blob,
            "supports_json_blob": version_info.supports_json_blob,
            "supports_json": version_info.supports_json_blob,
            "supports_transactions": True,
            "supports_prepared_statements": True,
            "supports_schemas": True,
        }

        if feature in feature_flags:
            return feature_flags[feature]

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
                - column_name: Name of the column
                - data_type: Oracle data type
                - data_length: Maximum length (for character types)
                - nullable: 'Y' or 'N'
        """

        oracle_driver = cast("OracleAsyncDriver", driver)
        result = await oracle_driver.execute(self._get_columns_sql(table, schema))
        return result.get_data()

    async def get_tables(self, driver: "AsyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get tables sorted by topological dependency order using Oracle CONNECT BY."""
        oracle_driver = cast("OracleAsyncDriver", driver)

        result = await oracle_driver.execute(self._get_tables_sql())
        sorted_tables = [row["table_name"] for row in result.get_data()]

        all_result = await oracle_driver.execute("SELECT table_name FROM user_tables")
        all_tables = {row["table_name"] for row in all_result.get_data()}

        sorted_set = set(sorted_tables)
        disconnected = list(all_tables - sorted_set)
        disconnected.sort()

        return disconnected + sorted_tables

    async def get_foreign_keys(
        self, driver: "AsyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        oracle_driver = cast("OracleAsyncDriver", driver)
        result = await oracle_driver.execute(self._get_foreign_keys_sql(table))

        return [
            ForeignKeyMetadata(
                table_name=row["table_name"],
                column_name=row["column_name"],
                referenced_table=row["referenced_table_name"],
                referenced_column=row["referenced_column_name"],
                constraint_name=row["constraint_name"],
                schema=row["schema"],
                referenced_schema=row["referenced_schema"],
            )
            for row in result.get_data()
        ]

    async def get_indexes(
        self, driver: "AsyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get index information for a table."""
        oracle_driver = cast("OracleAsyncDriver", driver)
        result = await oracle_driver.execute(self._get_indexes_sql(table))

        return [
            {
                "name": row["name"],
                "columns": row["columns"].split(",") if row["columns"] else [],
                "unique": row["uniqueness"] == "UNIQUE",
                "primary": False,
                "table_name": row["table_name"],
            }
            for row in result.get_data()
        ]

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
