"""Oracle-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import get_dialect_config
from sqlspec.data_dictionary.dialects.oracle import (
    extract_oracle_version_value,
    list_oracle_available_features,
    merge_oracle_table_lists,
    oracle_supports_json_blob,
    oracle_supports_native_json,
    oracle_supports_oson_blob,
    parse_oracle_compatible_major,
    parse_oracle_version_components,
    resolve_oracle_feature_flag,
    resolve_oracle_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("OracleVersionInfo", "OracledbAsyncDataDictionary", "OracledbSyncDataDictionary")

logger = get_logger("sqlspec.adapters.oracledb.data_dictionary")


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
        return parse_oracle_compatible_major(self.compatible)

    def supports_native_json(self) -> bool:
        """Check if database supports native JSON data type."""
        return oracle_supports_native_json(self.major, self.compatible_major)

    def supports_oson_blob(self) -> bool:
        """Check if database supports BLOB with OSON format."""
        return oracle_supports_oson_blob(self.major, self.is_autonomous)

    def supports_json_blob(self) -> bool:
        """Check if database supports BLOB with JSON validation."""
        return oracle_supports_json_blob(self.major)

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

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = parse_oracle_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def list_available_features(self) -> "list[str]":
        return list_oracle_available_features(self.get_dialect_config())

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

        version_value = extract_oracle_version_value(version_row)
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
        return resolve_oracle_feature_flag(
            self.get_dialect_config(),
            version_info,
            feature,
            compatible_major=version_info.compatible_major if version_info is not None else None,
            is_autonomous=bool(version_info and version_info.is_autonomous),
        )

    def get_optimal_type(self, driver: "OracleSyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            version_info = self.get_version(driver)
            return resolve_oracle_json_type(
                version_info,
                compatible_major=version_info.compatible_major if version_info is not None else None,
                is_autonomous=bool(version_info and version_info.is_autonomous),
            )
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
        return merge_oracle_table_lists(ordered_rows, all_rows)

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

    def _build_version_info(
        self, version_value: "str | None", compatible: "str | None", is_autonomous: bool
    ) -> "OracleVersionInfo | None":
        if not version_value:
            return None
        parts = parse_oracle_version_components(version_value)
        if parts is None:
            return None
        return OracleVersionInfo(parts[0], parts[1], parts[2], compatible=compatible, is_autonomous=is_autonomous)

    def list_available_features(self) -> "list[str]":
        return list_oracle_available_features(self.get_dialect_config())

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

        version_value = extract_oracle_version_value(version_row)
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
        return resolve_oracle_feature_flag(
            self.get_dialect_config(),
            version_info,
            feature,
            compatible_major=version_info.compatible_major if version_info is not None else None,
            is_autonomous=bool(version_info and version_info.is_autonomous),
        )

    async def get_optimal_type(self, driver: "OracleAsyncDriver", type_category: str) -> str:
        """Get optimal Oracle type for a category."""
        if type_category == "json":
            version_info = await self.get_version(driver)
            return resolve_oracle_json_type(
                version_info,
                compatible_major=version_info.compatible_major if version_info is not None else None,
                is_autonomous=bool(version_info and version_info.is_autonomous),
            )
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
        return merge_oracle_table_lists(ordered_rows, all_rows)

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
