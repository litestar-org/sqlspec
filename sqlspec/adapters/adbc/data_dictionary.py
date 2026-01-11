"""ADBC multi-dialect data dictionary for metadata queries."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite.core import format_identifier
from sqlspec.data_dictionary import (
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

__all__ = ("AdbcDataDictionary",)

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.core import SQL


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AdbcDataDictionary(SyncDataDictionaryBase):
    """ADBC multi-dialect data dictionary."""

    dialect: ClassVar[str] = "generic"

    def __init__(self) -> None:
        super().__init__()

    def _normalize_dialect(self, driver: "AdbcDriver") -> str:
        dialect_value = str(driver.dialect)
        return normalize_dialect_name(dialect_value)

    def _get_query(self, dialect: str, name: str) -> "SQL":
        loader = get_data_dictionary_loader()
        return loader.get_query(dialect, name)

    def _get_query_text(self, dialect: str, name: str) -> str:
        loader = get_data_dictionary_loader()
        return loader.get_query_text(dialect, name)

    def _get_query_text_or_none(self, dialect: str, name: str) -> "str | None":
        try:
            return self._get_query_text(dialect, name)
        except SQLFileNotFoundError:
            return None

    def _resolve_schema(self, dialect: str, schema: "str | None") -> "str | None":
        if schema is not None:
            return schema
        try:
            config = get_dialect_config(dialect)
        except ValueError:
            return None
        return config.default_schema

    def _resolve_feature_flag(self, dialect: str, feature: str, version_info: "VersionInfo | None") -> bool:
        try:
            config = get_dialect_config(dialect)
        except ValueError:
            return False
        flag = config.get_feature_flag(feature)
        if flag is not None:
            return flag
        required_version = config.get_feature_version(feature)
        if required_version is None or version_info is None:
            return False
        return bool(version_info >= required_version)

    def list_available_features(self) -> "list[str]":
        features = set(self.get_default_features())
        for dialect in list_registered_dialects():
            try:
                config = get_dialect_config(dialect)
            except ValueError:
                continue
            features.update(config.feature_flags.keys())
            features.update(config.feature_versions.keys())
        return sorted(features)

    def get_version(self, driver: "AdbcDriver") -> "VersionInfo | None":
        """Get database version information based on detected dialect."""
        dialect = self._normalize_dialect(driver)
        if dialect == "bigquery":
            return None

        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        # Not cached, fetch from database

        try:
            version_value = driver.select_value_or_none(self._get_query(dialect, "version"))
        except Exception:
            self._log_version_unavailable(dialect, "query_failed")
            self.cache_version(driver_id, None)
            return None

        if not version_value:
            self._log_version_unavailable(dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        try:
            config = get_dialect_config(dialect)
        except ValueError:
            self._log_version_unavailable(dialect, "unknown_dialect")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(config.version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "AdbcDriver", feature: str) -> bool:
        """Check if database supports a specific feature."""
        dialect = self._normalize_dialect(driver)
        version_info = self.get_version(driver)
        return self._resolve_feature_flag(dialect, feature, version_info)

    def get_optimal_type(self, driver: "AdbcDriver", type_category: str) -> str:
        """Get optimal database type for a category."""
        dialect = self._normalize_dialect(driver)
        try:
            config = get_dialect_config(dialect)
        except ValueError:
            return self.get_default_type_mapping().get(type_category, "TEXT")

        if type_category == "json":
            json_version = config.get_feature_version("supports_json")
            version_info = self.get_version(driver)
            if json_version and (version_info is None or version_info < json_version):
                return "TEXT"

        return config.get_optimal_type(type_category)

    def get_tables(self, driver: "AdbcDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables for the current dialect."""
        dialect = self._normalize_dialect(driver)
        schema_name: str | None = self._resolve_schema(dialect, schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")

        if dialect == "bigquery":
            if schema_name:
                tables_table = f"`{schema_name}.INFORMATION_SCHEMA.TABLES`"
                kcu_table = f"`{schema_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
                rc_table = f"`{schema_name}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
            else:
                tables_table = "INFORMATION_SCHEMA.TABLES"
                kcu_table = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
                rc_table = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"
            query_text = self._get_query_text(dialect, "tables_by_schema").format(
                tables_table=tables_table, kcu_table=kcu_table, rc_table=rc_table
            )
            return driver.select(query_text, schema_type=TableMetadata)

        if dialect == "sqlite":
            schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
            query_text = self._get_query_text(dialect, "tables_by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_type=TableMetadata)

        return driver.select(
            self._get_query(dialect, "tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    def get_columns(
        self, driver: "AdbcDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        dialect = self._normalize_dialect(driver)
        schema_name: str | None = self._resolve_schema(dialect, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")

        if dialect == "bigquery":
            schema_prefix = f"`{schema_name}`." if schema_name else ""
            if table is None:
                query_text = self._get_query_text(dialect, "columns_by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_name=schema_name, schema_type=ColumnMetadata)
            query_text = self._get_query_text(dialect, "columns_by_table").format(schema_prefix=schema_prefix)
            return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ColumnMetadata)

        if dialect == "sqlite":
            schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
            if table is None:
                query_text = self._get_query_text(dialect, "columns_by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_type=ColumnMetadata)
            table_identifier = f"{schema_name}.{table}" if schema_name else table
            query_text = self._get_query_text(dialect, "columns_by_table").format(
                table_name=format_identifier(table_identifier)
            )
            return driver.select(query_text, schema_type=ColumnMetadata)

        if table is None:
            return driver.select(
                self._get_query(dialect, "columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )
        return driver.select(
            self._get_query(dialect, "columns_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ColumnMetadata,
        )

    def get_indexes(
        self, driver: "AdbcDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index information for a table or schema."""
        dialect = self._normalize_dialect(driver)
        schema_name: str | None = self._resolve_schema(dialect, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")

        if dialect == "sqlite":
            if table is None:
                tables = self.get_tables(driver, schema=schema_name)
                indexes: list[IndexMetadata] = []
                for table_info in tables:
                    table_name = table_info.get("table_name")
                    if not table_name:
                        continue
                    indexes.extend(self.get_indexes(driver, table=table_name, schema=schema_name))
                return indexes

            assert table is not None
            table_name = table
            table_identifier = f"{schema_name}.{table_name}" if schema_name else table_name
            index_list_sql = self._get_query_text(dialect, "indexes_by_table").format(
                table_name=format_identifier(table_identifier)
            )
            index_list_rows = driver.select(index_list_sql)
            index_metadata_list: list[IndexMetadata] = []
            for row in index_list_rows:
                index_name = row.get("name")
                if not index_name:
                    continue
                index_identifier = f"{schema_name}.{index_name}" if schema_name else index_name
                columns_sql = self._get_query_text(dialect, "index_columns_by_index").format(
                    index_name=format_identifier(index_identifier)
                )
                columns_rows = driver.select(columns_sql)
                columns: list[str] = []
                for col in columns_rows:
                    column_name = col.get("name")
                    if column_name is None:
                        continue
                    columns.append(str(column_name))
                index_metadata: IndexMetadata = {"index_name": index_name, "table_name": table_name, "columns": columns}
                if schema_name is not None:
                    index_metadata["schema_name"] = schema_name
                unique_value = row.get("unique")
                if unique_value is not None:
                    index_metadata["is_unique"] = unique_value
                index_metadata_list.append(index_metadata)
            return index_metadata_list

        if table is None:
            return driver.select(
                self._get_query(dialect, "indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        return driver.select(
            self._get_query(dialect, "indexes_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=IndexMetadata,
        )

    def get_foreign_keys(
        self, driver: "AdbcDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        dialect = self._normalize_dialect(driver)
        schema_name: str | None = self._resolve_schema(dialect, schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")

        if dialect == "bigquery":
            if schema_name:
                kcu_table = f"`{schema_name}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
                rc_table = f"`{schema_name}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
            else:
                kcu_table = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
                rc_table = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"
            if table is None:
                query_text = self._get_query_text(dialect, "foreign_keys_by_schema").format(
                    kcu_table=kcu_table, rc_table=rc_table
                )
                return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)
            query_text = self._get_query_text(dialect, "foreign_keys_by_table").format(
                kcu_table=kcu_table, rc_table=rc_table
            )
            return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ForeignKeyMetadata)

        if dialect == "sqlite":
            if table is None:
                schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
                query_text = self._get_query_text(dialect, "foreign_keys_by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_type=ForeignKeyMetadata)
            table_label = table.replace("'", "''")
            table_identifier = f"{schema_name}.{table}" if schema_name else table
            query_text = self._get_query_text(dialect, "foreign_keys_by_table").format(
                table_name=format_identifier(table_identifier), table_label=table_label
            )
            return driver.select(query_text, schema_type=ForeignKeyMetadata)

        if table is None:
            query_text_optional = self._get_query_text_or_none(dialect, "foreign_keys_by_schema")
            if query_text_optional is not None:
                return driver.select(query_text_optional, schema_name=schema_name, schema_type=ForeignKeyMetadata)

        return driver.select(
            self._get_query(dialect, "foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=table,
            schema_type=ForeignKeyMetadata,
        )
