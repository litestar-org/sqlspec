"""Generic data dictionary for arrow-odbc connections."""

from typing import TYPE_CHECKING, Any, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError

if TYPE_CHECKING:
    from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
    from sqlspec.core import SQL
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("ArrowOdbcDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class ArrowOdbcDataDictionary(SyncDataDictionaryBase):
    """Runtime-dialect data dictionary for generic ODBC connections."""

    dialect: ClassVar[str] = "sqlite"

    def __init__(self, dialect: str = "sqlite") -> None:
        super().__init__()
        self._dialect = dialect

    def get_dialect_config(self) -> "DialectConfig":
        """Return the runtime dialect configuration for this data dictionary."""
        return get_dialect_config(self._dialect)

    def get_query(self, name: str) -> "SQL":
        """Return a named SQL query for the runtime dialect."""
        loader = get_data_dictionary_loader()
        return loader.get_query(self._dialect, name)

    def get_query_text(self, name: str) -> str:
        """Return raw SQL text for a named runtime dialect query."""
        loader = get_data_dictionary_loader()
        return loader.get_query_text(self._dialect, name)

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a schema name using runtime dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def get_version(self, driver: "ArrowOdbcDriver") -> VersionInfo | None:
        """Get database version information when the runtime dialect provides a query."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        try:
            version_value = driver.select_value_or_none(self.get_query("version"))
        except SQLFileNotFoundError:
            self._log_version_unavailable(self._dialect, "no_query")
            self.cache_version(driver_id, None)
            return None
        except Exception:
            self._log_version_unavailable(self._dialect, "query_failed")
            self.cache_version(driver_id, None)
            return None

        if not version_value:
            self._log_version_unavailable(self._dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        config = self.get_dialect_config()
        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(self._dialect, "parse_failed")
        else:
            self._log_version_detected(self._dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "ArrowOdbcDriver", feature: str) -> bool:
        """Check whether the runtime dialect supports a feature."""
        return self.resolve_feature_flag(feature, self.get_version(driver))

    def get_optimal_type(self, driver: "ArrowOdbcDriver", type_category: str) -> str:
        """Get the optimal runtime dialect type for a category."""
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "ArrowOdbcDriver", schema: str | None = None) -> list[TableMetadata]:
        """Get table metadata for dialects with bundled catalog queries."""
        try:
            return driver.select(
                self.get_query("tables_by_schema"), schema_name=self.resolve_schema(schema), schema_type=TableMetadata
            )
        except SQLFileNotFoundError:
            return []

    def get_columns(
        self, driver: "ArrowOdbcDriver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get column metadata for dialects with bundled catalog queries."""
        query_name = "columns_by_table" if table is not None else "columns_by_schema"
        parameters: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        if table is not None:
            parameters["table_name"] = table
        try:
            return driver.select(self.get_query(query_name), schema_type=ColumnMetadata, **parameters)
        except SQLFileNotFoundError:
            return []

    def get_indexes(
        self, driver: "ArrowOdbcDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get index metadata for dialects with bundled catalog queries."""
        query_name = "indexes_by_table" if table is not None else "indexes_by_schema"
        parameters: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        if table is not None:
            parameters["table_name"] = table
        try:
            return driver.select(self.get_query(query_name), schema_type=IndexMetadata, **parameters)
        except SQLFileNotFoundError:
            return []

    def get_foreign_keys(
        self, driver: "ArrowOdbcDriver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign-key metadata for dialects with bundled catalog queries."""
        query_name = "foreign_keys_by_table" if table is not None else "foreign_keys_by_schema"
        parameters: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        if table is not None:
            parameters["table_name"] = table
        try:
            return driver.select(self.get_query(query_name), schema_type=ForeignKeyMetadata, **parameters)
        except SQLFileNotFoundError:
            return []
