"""Oracle-specific data dictionary for metadata queries."""

import re
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import get_dialect_config
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
    from sqlspec.data_dictionary._types import DialectConfig

ORACLE_MIN_JSON_NATIVE_VERSION = 21
ORACLE_MIN_JSON_NATIVE_COMPATIBLE = 20
ORACLE_MIN_JSON_BLOB_VERSION = 12
ORACLE_MIN_OSON_VERSION = 19
ORACLE_VERSION_PARTS_COUNT = 3

VERSION_NUMBER_PATTERN = re.compile(r"(\d+)")
logger = get_logger("sqlspec.adapters.oracledb.data_dictionary")

__all__ = ("OracleVersionInfo", "OracledbAsyncDataDictionary", "OracledbSyncDataDictionary")


class OracleVersionInfo(VersionInfo):
    """Oracle database version information."""

    def __init__(
        self, major: int, minor: int = 0, patch: int = 0, compatible: "str | None" = None, is_autonomous: bool = False
    ) -> None:
        """Initialize Oracle version info.

        Args:
            major: Major version number (e.g., 19, 21, 23).
            minor: Minor version number.
            patch: Patch version number.
            compatible: Compatible parameter value.
            is_autonomous: Whether this is an Autonomous Database.
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
        """Check if database supports native JSON data type."""
        return (
            self.major >= ORACLE_MIN_JSON_NATIVE_VERSION
            and (self.compatible_major or 0) >= ORACLE_MIN_JSON_NATIVE_COMPATIBLE
        )

    def supports_oson_blob(self) -> bool:
        """Check if database supports BLOB with OSON format."""
        if self.major >= ORACLE_MIN_JSON_NATIVE_VERSION:
            return True
        return self.major >= ORACLE_MIN_OSON_VERSION and self.is_autonomous

    def supports_json_blob(self) -> bool:
        """Check if database supports BLOB with JSON validation."""
        return self.major >= ORACLE_MIN_JSON_BLOB_VERSION

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.patch}"
        if self.compatible:
            version_str += f" (compatible={self.compatible})"
        if self.is_autonomous:
            version_str += " [Autonomous]"
        return version_str


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbSyncDataDictionary(SyncDataDictionaryBase):
    """Oracle-specific sync data dictionary."""

    dialect: ClassVar[str] = "oracle"

    def __init__(self) -> None:
        super().__init__()

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def _extract_version_value(self, row: Any) -> "str | None":
        if isinstance(row, dict):
            for key in ("version", "VERSION", "Version"):
                value = row.get(key)
                if value:
                    return str(value)
        if isinstance(row, (list, tuple)) and row:
            return str(row[0])
        if row is not None:
            return str(row)
        return None

    def _parse_version_components(self, version_str: str) -> "tuple[int, int, int] | None":
        parts = [int(value) for value in VERSION_NUMBER_PATTERN.findall(version_str)]
        if not parts:
            return None
        while len(parts) < ORACLE_VERSION_PARTS_COUNT:
            parts.append(0)
        return parts[0], parts[1], parts[2]

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = self._parse_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def _get_oracle_json_type(self, version_info: "OracleVersionInfo | None") -> str:
        if version_info is None:
            return "CLOB"
        if version_info.supports_native_json():
            return "JSON"
        if version_info.supports_oson_blob():
            return "BLOB"
        if version_info.supports_json_blob():
            return "BLOB"
        return "CLOB"

    def _merge_table_lists(
        self, ordered: "list[TableMetadata]", all_tables: "list[TableMetadata]"
    ) -> "list[TableMetadata]":
        if not ordered:
            return sorted(all_tables, key=lambda item: item.get("table_name") or "")
        ordered_names = {item.get("table_name") for item in ordered if item.get("table_name")}
        remainder = [item for item in all_tables if item.get("table_name") not in ordered_names]
        return ordered + remainder

    def _resolve_feature_flag(self, version_info: "OracleVersionInfo | None", feature: str) -> bool:
        if feature == "is_autonomous":
            return bool(version_info and version_info.is_autonomous)
        if version_info is None:
            return False
        if feature == "supports_native_json":
            return version_info.supports_native_json()
        if feature == "supports_oson_blob":
            return version_info.supports_oson_blob()
        if feature == "supports_json_blob":
            return version_info.supports_json_blob()
        if feature == "supports_json":
            return version_info.supports_json_blob()

        config = get_dialect_config(type(self).dialect)
        flag = config.get_feature_flag(feature)
        if flag is not None:
            return flag
        required_version = config.get_feature_version(feature)
        if required_version is None:
            return False
        return bool(version_info >= required_version)

    def list_available_features(self) -> "list[str]":
        config = get_dialect_config(type(self).dialect)
        features: set[str] = set()
        features.update(config.feature_flags.keys())
        features.update(config.feature_versions.keys())
        features.update({
            "is_autonomous",
            "supports_native_json",
            "supports_oson_blob",
            "supports_json_blob",
            "supports_json",
        })
        return sorted(features)

    def _get_compatible_value(self, driver: "OracleSyncDriver") -> "str | None":
        query_text = self.get_query_text("compatible")
        try:
            value = driver.select_value(query_text)
            if value is None:
                return None
            return str(value)
        except Exception:
            return None

    def _is_autonomous(self, driver: "OracleSyncDriver") -> bool:
        query_text = self.get_query_text("autonomous_service")
        try:
            return bool(driver.select_value_or_none(query_text))
        except Exception:
            return False

    def get_version(self, driver: "OracleSyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information."""
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return cast("OracleVersionInfo | None", self._version_cache.get(driver_id))
        # Not cached, fetch from database

        version_row = driver.select_one_or_none(self.get_query_text("version"))
        if not version_row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = self._extract_version_value(version_row)
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        compatible = self._get_compatible_value(driver)
        is_autonomous = self._is_autonomous(driver)
        version_info = self._build_version_info(version_value, compatible, is_autonomous)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "OracleSyncDriver", feature: str) -> bool:
        """Check if Oracle database supports a specific feature."""
        version_info = self.get_version(driver)
        return self._resolve_feature_flag(version_info, feature)

    def get_optimal_type(self, driver: "OracleSyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            return self._get_oracle_json_type(self.get_version(driver))
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "OracleSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by dependency order with full coverage."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered_rows = driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        all_rows = driver.select(
            self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        return self._merge_table_lists(ordered_rows, all_rows)

    def get_columns(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return driver.select(
            self.get_query("columns_by_table"), schema_name=schema_name, table_name=table, schema_type=ColumnMetadata
        )

    def get_indexes(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return driver.select(
            self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
        )

    def get_foreign_keys(
        self, driver: "OracleSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbAsyncDataDictionary(AsyncDataDictionaryBase):
    """Oracle-specific async data dictionary."""

    dialect: ClassVar[str] = "oracle"

    def __init__(self) -> None:
        super().__init__()

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def _extract_version_value(self, row: Any) -> "str | None":
        if isinstance(row, dict):
            for key in ("version", "VERSION", "Version"):
                value = row.get(key)
                if value:
                    return str(value)
        if isinstance(row, (list, tuple)) and row:
            return str(row[0])
        if row is not None:
            return str(row)
        return None

    def _parse_version_components(self, version_str: str) -> "tuple[int, int, int] | None":
        parts = [int(value) for value in VERSION_NUMBER_PATTERN.findall(version_str)]
        if not parts:
            return None
        while len(parts) < ORACLE_VERSION_PARTS_COUNT:
            parts.append(0)
        return parts[0], parts[1], parts[2]

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = self._parse_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def _get_oracle_json_type(self, version_info: "OracleVersionInfo | None") -> str:
        if version_info is None:
            return "CLOB"
        if version_info.supports_native_json():
            return "JSON"
        if version_info.supports_oson_blob():
            return "BLOB"
        if version_info.supports_json_blob():
            return "BLOB"
        return "CLOB"

    def _merge_table_lists(
        self, ordered: "list[TableMetadata]", all_tables: "list[TableMetadata]"
    ) -> "list[TableMetadata]":
        if not ordered:
            return sorted(all_tables, key=lambda item: item.get("table_name") or "")
        ordered_names = {item.get("table_name") for item in ordered if item.get("table_name")}
        remainder = [item for item in all_tables if item.get("table_name") not in ordered_names]
        return ordered + remainder

    def _resolve_feature_flag(self, version_info: "OracleVersionInfo | None", feature: str) -> bool:
        if feature == "is_autonomous":
            return bool(version_info and version_info.is_autonomous)
        if version_info is None:
            return False
        if feature == "supports_native_json":
            return version_info.supports_native_json()
        if feature == "supports_oson_blob":
            return version_info.supports_oson_blob()
        if feature == "supports_json_blob":
            return version_info.supports_json_blob()
        if feature == "supports_json":
            return version_info.supports_json_blob()

        config = get_dialect_config(type(self).dialect)
        flag = config.get_feature_flag(feature)
        if flag is not None:
            return flag
        required_version = config.get_feature_version(feature)
        if required_version is None:
            return False
        return bool(version_info >= required_version)

    def list_available_features(self) -> "list[str]":
        config = get_dialect_config(type(self).dialect)
        features: set[str] = set()
        features.update(config.feature_flags.keys())
        features.update(config.feature_versions.keys())
        features.update({
            "is_autonomous",
            "supports_native_json",
            "supports_oson_blob",
            "supports_json_blob",
            "supports_json",
        })
        return sorted(features)

    async def _get_compatible_value(self, driver: "OracleAsyncDriver") -> "str | None":
        query_text = self.get_query_text("compatible")
        try:
            value = await driver.select_value(query_text)
            if value is None:
                return None
            return str(value)
        except Exception:
            return None

    async def _is_autonomous(self, driver: "OracleAsyncDriver") -> bool:
        query_text = self.get_query_text("autonomous_service")
        try:
            return bool(await driver.select_value_or_none(query_text))
        except Exception:
            return False

    async def get_version(self, driver: "OracleAsyncDriver") -> "OracleVersionInfo | None":
        """Get Oracle database version information."""
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return cast("OracleVersionInfo | None", self._version_cache.get(driver_id))
        # Not cached, fetch from database

        version_row = await driver.select_one_or_none(self.get_query_text("version"))
        if not version_row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = self._extract_version_value(version_row)
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        compatible = await self._get_compatible_value(driver)
        is_autonomous = await self._is_autonomous(driver)
        version_info = self._build_version_info(version_value, compatible, is_autonomous)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "OracleAsyncDriver", feature: str) -> bool:
        """Check if Oracle database supports a specific feature."""
        version_info = await self.get_version(driver)
        return self._resolve_feature_flag(version_info, feature)

    async def get_optimal_type(self, driver: "OracleAsyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            return self._get_oracle_json_type(await self.get_version(driver))
        return self.get_dialect_config().get_optimal_type(type_category)

    async def get_tables(self, driver: "OracleAsyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by dependency order with full coverage."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered_rows = await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        all_rows = await driver.select(
            self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )
        return self._merge_table_lists(ordered_rows, all_rows)

    async def get_columns(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return await driver.select(
            self.get_query("columns_by_table"), schema_name=schema_name, table_name=table, schema_type=ColumnMetadata
        )

    async def get_indexes(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return await driver.select(
            self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
        )

    async def get_foreign_keys(
        self, driver: "OracleAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )
