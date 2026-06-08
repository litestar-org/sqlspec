"""mssql-python data dictionary."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
    get_dialect_config,
)
from sqlspec.data_dictionary.dialects.mssql import (
    extract_mssql_version_value,
    is_mssql_azure_sql,
    list_mssql_available_features,
    merge_mssql_table_lists,
    mssql_supports_native_json,
    parse_mssql_engine_edition,
    parse_mssql_version_components,
    resolve_mssql_feature_flag,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("MssqlPythonAsyncDataDictionary", "MssqlPythonSyncDataDictionary", "MssqlVersionInfo")

logger = get_logger("sqlspec.adapters.mssql_python.data_dictionary")


class MssqlVersionInfo(VersionInfo):
    """MSSQL database version info with build, revision, and Azure SQL detection."""

    def __init__(
        self,
        major: int,
        minor: int = 0,
        build: int = 0,
        revision: int = 0,
        edition: str | None = None,
        engine_edition: int | None = None,
    ) -> None:
        super().__init__(major, minor, 0)
        self.build = build
        self.revision = revision
        self.edition = edition
        self.engine_edition = engine_edition
        self.is_azure_sql = is_mssql_azure_sql(engine_edition)

    def supports_native_json(self) -> bool:
        """Return whether this server supports the native JSON type."""
        return mssql_supports_native_json(self.major, is_azure_sql=self.is_azure_sql)

    @property
    def version_tuple(self) -> "tuple[int, int, int]":
        """Get version tuple using the MSSQL build number as the third component."""
        return (self.major, self.minor, self.build)

    def __str__(self) -> str:
        """String representation of version info."""
        version_str = f"{self.major}.{self.minor}.{self.build}.{self.revision}"
        if self.edition:
            version_str += f" ({self.edition})"
        if self.is_azure_sql:
            version_str += " [Azure]"
        return version_str


class _MssqlDataDictionaryMixin:
    """Shared helpers for MSSQL data dictionaries."""

    dialect: ClassVar[str] = "mssql"

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def list_available_features(self) -> list[str]:
        """List available feature flags for this dialect."""
        return list_mssql_available_features(self.get_dialect_config())

    def _build_version_info(
        self, version_value: str | None, edition: str | None, engine_edition_value: Any
    ) -> MssqlVersionInfo | None:
        if not version_value:
            return None
        major, minor, build, revision = parse_mssql_version_components(version_value)
        return MssqlVersionInfo(
            major,
            minor,
            build,
            revision,
            edition=edition,
            engine_edition=parse_mssql_engine_edition(engine_edition_value),
        )

    def _get_optimal_type_from_version(self, version_info: MssqlVersionInfo | None, type_category: str) -> str:
        if type_category in {"json", "jsonb"} and version_info is not None and version_info.supports_native_json():
            return "JSON"
        return self.get_dialect_config().get_optimal_type(type_category)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MssqlPythonSyncDataDictionary(_MssqlDataDictionaryMixin, SyncDataDictionaryBase):
    """MSSQL sync data dictionary."""

    dialect: ClassVar[str] = "mssql"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: Any) -> MssqlVersionInfo | None:
        """Get SQL Server version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("MssqlVersionInfo | None", self._version_cache.get(driver_id))

        row = driver.select_one_or_none(self.get_query_text("version"))
        if not row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = extract_mssql_version_value(
            _row_value(row, "product_version") or _row_value(row, "version_string", "version")
        )
        edition_value = _row_value(row, "edition")
        edition = str(edition_value) if edition_value is not None else None
        version_info = self._build_version_info(version_value, edition, _row_value(row, "engine_edition"))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Check whether SQL Server supports a feature."""
        version_info = self.get_version(driver)
        return resolve_mssql_feature_flag(
            feature,
            major=version_info.major if version_info is not None else 0,
            is_azure_sql=bool(version_info and version_info.is_azure_sql),
            config=self.get_dialect_config(),
            version_info=version_info,
        )

    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Get optimal SQL Server type for a category."""
        return self._get_optimal_type_from_version(self.get_version(driver), type_category)

    def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered = cast(
            "list[TableMetadata]",
            driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata),
        )
        all_rows = cast(
            "list[TableMetadata]",
            driver.select(self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata),
        )
        return merge_mssql_table_lists(ordered, all_rows)

    def get_columns(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[ColumnMetadata]:
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                driver.select(self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            driver.select(
                self.get_query("columns_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            ),
        )

    def get_indexes(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[IndexMetadata]:
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return cast(
                "list[IndexMetadata]",
                driver.select(self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return cast(
            "list[IndexMetadata]",
            driver.select(
                self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
            ),
        )

    def get_foreign_keys(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return cast(
                "list[ForeignKeyMetadata]",
                driver.select(
                    self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return cast(
            "list[ForeignKeyMetadata]",
            driver.select(
                self.get_query("foreign_keys_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            ),
        )


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MssqlPythonAsyncDataDictionary(_MssqlDataDictionaryMixin, AsyncDataDictionaryBase):
    """MSSQL async data dictionary."""

    dialect: ClassVar[str] = "mssql"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: Any) -> MssqlVersionInfo | None:
        """Get SQL Server version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("MssqlVersionInfo | None", self._version_cache.get(driver_id))

        row = await driver.select_one_or_none(self.get_query_text("version"))
        if not row:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_value = extract_mssql_version_value(
            _row_value(row, "product_version") or _row_value(row, "version_string", "version")
        )
        edition_value = _row_value(row, "edition")
        edition = str(edition_value) if edition_value is not None else None
        version_info = self._build_version_info(version_value, edition, _row_value(row, "engine_edition"))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Check whether SQL Server supports a feature."""
        version_info = await self.get_version(driver)
        return resolve_mssql_feature_flag(
            feature,
            major=version_info.major if version_info is not None else 0,
            is_azure_sql=bool(version_info and version_info.is_azure_sql),
            config=self.get_dialect_config(),
            version_info=version_info,
        )

    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Get optimal SQL Server type for a category."""
        return self._get_optimal_type_from_version(await self.get_version(driver), type_category)

    async def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        ordered = cast(
            "list[TableMetadata]",
            await driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata),
        )
        all_rows = cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("all_tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
            ),
        )
        return merge_mssql_table_lists(ordered, all_rows)

    async def get_columns(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return cast(
                "list[ColumnMetadata]",
                await driver.select(
                    self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return cast(
            "list[ColumnMetadata]",
            await driver.select(
                self.get_query("columns_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ColumnMetadata,
            ),
        )

    async def get_indexes(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return cast(
                "list[IndexMetadata]",
                await driver.select(
                    self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return cast(
            "list[IndexMetadata]",
            await driver.select(
                self.get_query("indexes_by_table"), schema_name=schema_name, table_name=table, schema_type=IndexMetadata
            ),
        )

    async def get_foreign_keys(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return cast(
                "list[ForeignKeyMetadata]",
                await driver.select(
                    self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
                ),
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return cast(
            "list[ForeignKeyMetadata]",
            await driver.select(
                self.get_query("foreign_keys_by_table"),
                schema_name=schema_name,
                table_name=table,
                schema_type=ForeignKeyMetadata,
            ),
        )


def _row_value(row: object, *names: str) -> Any:
    """Return the first named value from a row-like object."""
    if isinstance(row, dict):
        for name in names:
            if name in row:
                return row[name]
            upper_name = name.upper()
            if upper_name in row:
                return row[upper_name]
        return None
    return getattr(row, names[0], None) if names else None
