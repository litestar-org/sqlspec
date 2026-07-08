"""ADBC multi-dialect data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from adbc_driver_manager import NotSupportedError as AdbcNotSupportedError
from adbc_driver_manager import OperationalError as AdbcOperationalError
from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite.core import format_identifier
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ConstraintMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableMetadata,
    TableStatisticsMetadata,
    VersionInfo,
    ensure_system_metadata_request,
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
    system_metadata_gated_result,
    unsupported_system_metadata_capability,
)
from sqlspec.data_dictionary.dialects.bigquery import (
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)
from sqlspec.data_dictionary.dialects.cockroachdb import resolve_cockroachdb_json_type
from sqlspec.data_dictionary.dialects.mysql import resolve_mysql_json_type
from sqlspec.data_dictionary.dialects.postgres import resolve_postgres_json_type
from sqlspec.data_dictionary.dialects.sqlite import resolve_sqlite_json_type
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import OperationalError, SQLFileNotFoundError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import normalize_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.core import SQL

__all__ = ("AdbcDataDictionary",)

logger = get_logger("sqlspec.adapters.adbc")

_NATIVE_TABLE_TYPES: Final = frozenset({"table", "base table"})
_NATIVE_FALLBACK_ERRORS: Final = (AdbcNotSupportedError, AdbcOperationalError)
_ADBC_METADATA_DOMAINS: Final = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "privileges",
    "dependencies",
    "ddl",
    "statistics",
    "system",
)
_ADBC_TRANSPORT_WARNING: Final = (
    "ADBC transport metadata is portable discovery metadata; dialect SQL packs remain the DDL-grade source."
)
_ADBC_CONSTRAINT_WARNING: Final = (
    "ADBC GetObjects constraint metadata is lossy; expressions, enforcement details, privileges, and dialect DDL "
    "are not preserved."
)
_ADBC_DDL_UNSUPPORTED_WARNING: Final = "ADBC metadata is not a lossless DDL source for this dialect."
_ADBC_STATISTICS_WARNING: Final = "ADBC statistics metadata may be approximate, expensive, and driver-specific."


class _NativeMetadataIncompleteError(Exception):
    pass


def _iter_object_tables(rows: "list[dict[str, Any]]") -> "list[tuple[str | None, str | None, dict[str, Any]]]":
    entries: list[tuple[str | None, str | None, dict[str, Any]]] = []
    for catalog in rows:
        catalog_name = catalog.get("catalog_name")
        for db_schema in catalog.get("catalog_db_schemas") or []:
            schema_name = db_schema.get("db_schema_name")
            entries.extend((catalog_name, schema_name, table) for table in db_schema.get("db_schema_tables") or [])
    return entries


def _primary_key_columns(table: "dict[str, Any]") -> "set[str]":
    names: set[str] = set()
    for constraint in table.get("table_constraints") or []:
        if str(constraint.get("constraint_type") or "").upper() == "PRIMARY KEY":
            names.update(str(column) for column in constraint.get("constraint_column_names") or [])
    return names


def _normalize_native_tables(rows: "list[dict[str, Any]]") -> "list[TableMetadata]":
    tables: list[TableMetadata] = []
    for catalog_name, schema_name, table in _iter_object_tables(rows):
        table_type = str(table.get("table_type") or "")
        if table_type.lower() not in _NATIVE_TABLE_TYPES:
            continue
        metadata: TableMetadata = {"table_name": str(table["table_name"]), "table_type": table_type}
        resolved_schema = schema_name or catalog_name
        if resolved_schema:
            metadata["schema_name"] = str(resolved_schema)
        if catalog_name:
            metadata["table_catalog"] = str(catalog_name)
        if schema_name:
            metadata["table_schema"] = str(schema_name)
        tables.append(metadata)
    return tables


def _normalize_native_columns(
    rows: "list[dict[str, Any]]", table_name_exact: "str | None" = None
) -> "list[ColumnMetadata]":
    columns: list[ColumnMetadata] = []
    for catalog_name, schema_name, table in _iter_object_tables(rows):
        if str(table.get("table_type") or "").lower() not in _NATIVE_TABLE_TYPES:
            continue
        table_name = str(table["table_name"])
        if table_name_exact is not None and table_name != table_name_exact:
            continue
        primary_columns = _primary_key_columns(table)
        resolved_schema = schema_name or catalog_name
        for column in table.get("table_columns") or []:
            entry: ColumnMetadata = {"table_name": table_name, "column_name": str(column["column_name"])}
            if resolved_schema:
                entry["schema_name"] = str(resolved_schema)
            type_name = column.get("xdbc_type_name")
            if type_name:
                entry["data_type"] = str(type_name)
            ordinal = column.get("ordinal_position")
            if ordinal is not None:
                entry["ordinal_position"] = int(ordinal)
            nullable = column.get("xdbc_is_nullable")
            if nullable is not None:
                entry["is_nullable"] = str(nullable)
            default = column.get("xdbc_column_def")
            if default is not None:
                entry["column_default"] = str(default)
            size = column.get("xdbc_column_size")
            if size is not None:
                entry["max_length"] = int(size)
            digits = column.get("xdbc_decimal_digits")
            if digits is not None:
                entry["numeric_scale"] = int(digits)
            if entry["column_name"] in primary_columns:
                entry["is_primary"] = True
            columns.append(entry)
    return columns


def _normalize_native_foreign_keys(
    rows: "list[dict[str, Any]]", table_name_exact: "str | None" = None
) -> "list[ForeignKeyMetadata]":
    keys: list[ForeignKeyMetadata] = []
    for catalog_name, schema_name, table in _iter_object_tables(rows):
        table_name = str(table["table_name"])
        if table_name_exact is not None and table_name != table_name_exact:
            continue
        resolved_schema = schema_name or catalog_name
        for constraint in table.get("table_constraints") or []:
            if str(constraint.get("constraint_type") or "").upper() != "FOREIGN KEY":
                continue
            column_names = [str(column) for column in constraint.get("constraint_column_names") or []]
            usage_entries = constraint.get("constraint_column_usage") or []
            constraint_name = constraint.get("constraint_name")
            for column_name, usage in zip(column_names, usage_entries, strict=False):
                referenced_schema = usage.get("fk_db_schema") or usage.get("fk_catalog")
                keys.append(
                    ForeignKeyMetadata(
                        table_name=table_name,
                        column_name=column_name,
                        referenced_table=str(usage["fk_table"]),
                        referenced_column=str(usage["fk_column_name"]),
                        constraint_name=str(constraint_name) if constraint_name else None,
                        schema=str(resolved_schema) if resolved_schema else None,
                        referenced_schema=str(referenced_schema) if referenced_schema else None,
                    )
                )
    return keys


def _generated_constraint_name(table_name: str, constraint_type: str, column_names: "tuple[str, ...]") -> str:
    constraint_label = constraint_type.lower().replace(" ", "_") or "constraint"
    column_label = "_".join(column_names) if column_names else "unnamed"
    return f"{table_name}_{constraint_label}_{column_label}"


def _normalize_native_constraints(
    rows: "list[dict[str, Any]]", table_name_exact: "str | None" = None, dialect: "str | None" = None
) -> "list[ConstraintMetadata]":
    constraints: list[ConstraintMetadata] = []
    for catalog_name, schema_name, table in _iter_object_tables(rows):
        table_name = str(table["table_name"])
        if table_name_exact is not None and table_name != table_name_exact:
            continue
        resolved_schema = schema_name or catalog_name
        for constraint in table.get("table_constraints") or []:
            constraint_type = str(constraint.get("constraint_type") or "").upper()
            if not constraint_type:
                continue
            column_names = tuple(str(column) for column in constraint.get("constraint_column_names") or ())
            raw_name = constraint.get("constraint_name")
            constraint_name = (
                str(raw_name) if raw_name else _generated_constraint_name(table_name, constraint_type, column_names)
            )
            identity = ObjectIdentity(
                name=constraint_name,
                object_type="constraint",
                catalog=str(catalog_name) if catalog_name else None,
                schema=str(resolved_schema) if resolved_schema else None,
                dialect=dialect,
                source=MetadataSource.DRIVER_METADATA,
            )
            constraints.append(
                ConstraintMetadata(
                    identity=identity,
                    source=MetadataSource.DRIVER_METADATA,
                    attributes={
                        "table_name": table_name,
                        "constraint_type": constraint_type,
                        "column_names": column_names,
                        "column_usage": tuple(constraint.get("constraint_column_usage") or ()),
                        "is_lossy": True,
                    },
                )
            )
    return constraints


_ARROW_DECIMAL_FORMAT: Final = "DECIMAL({precision},{scale})"


def _arrow_type_to_sql(data_type: Any) -> str:
    import pyarrow as pa

    types = pa.types
    if types.is_boolean(data_type):
        return "BOOLEAN"
    if types.is_int8(data_type) or types.is_int16(data_type) or types.is_uint8(data_type) or types.is_uint16(data_type):
        return "SMALLINT"
    if types.is_int32(data_type) or types.is_uint32(data_type):
        return "INTEGER"
    if types.is_int64(data_type) or types.is_uint64(data_type):
        return "BIGINT"
    if types.is_float16(data_type) or types.is_float32(data_type):
        return "REAL"
    if types.is_float64(data_type):
        return "DOUBLE"
    if types.is_decimal(data_type):
        return _ARROW_DECIMAL_FORMAT.format(precision=data_type.precision, scale=data_type.scale)
    if types.is_string(data_type) or types.is_large_string(data_type):
        return "VARCHAR"
    if types.is_binary(data_type) or types.is_large_binary(data_type) or types.is_fixed_size_binary(data_type):
        return "VARBINARY"
    if types.is_date(data_type):
        return "DATE"
    if types.is_time(data_type):
        return "TIME"
    if types.is_timestamp(data_type):
        return "TIMESTAMP"
    return str(data_type).upper()


_ADBC_STATISTIC_NAMES: dict[int, str] = {
    0: "adbc.statistic.byte_width",
    1: "adbc.statistic.distinct_count",
    2: "adbc.statistic.max_byte_width",
    3: "adbc.statistic.max_value",
    4: "adbc.statistic.min_value",
    5: "adbc.statistic.null_count",
    6: "adbc.statistic.row_count",
}


def _normalize_native_statistic_names(rows: "list[dict[str, Any]]") -> "dict[int, str]":
    statistic_names: dict[int, str] = {}
    for entry in rows:
        key = entry.get("statistic_key")
        name = entry.get("statistic_name")
        if key is None or name is None:
            continue
        try:
            statistic_names[int(key)] = str(name)
        except (TypeError, ValueError):
            continue
    return statistic_names


def _normalize_native_statistics(
    rows: "list[dict[str, Any]]", statistic_names: "dict[int, str] | None" = None
) -> "list[TableStatisticsMetadata]":
    statistic_name_map = (
        _ADBC_STATISTIC_NAMES if statistic_names is None else {**_ADBC_STATISTIC_NAMES, **statistic_names}
    )
    statistics: list[TableStatisticsMetadata] = []
    for catalog in rows:
        catalog_name = catalog.get("catalog_name")
        for db_schema in catalog.get("catalog_db_schemas") or []:
            schema_name = db_schema.get("db_schema_name")
            for entry in db_schema.get("db_schema_statistics") or []:
                key = int(entry["statistic_key"])
                column_name = entry.get("column_name")
                record: TableStatisticsMetadata = {
                    "table_name": str(entry["table_name"]),
                    "column_name": str(column_name) if column_name is not None else None,
                    "statistic_key": key,
                    "statistic_name": statistic_name_map.get(key, str(key)),
                    "statistic_value": entry.get("statistic_value"),
                    "is_approximate": bool(entry.get("statistic_is_approximate")),
                }
                if catalog_name:
                    record["catalog_name"] = str(catalog_name)
                if schema_name:
                    record["schema_name"] = str(schema_name)
                statistics.append(record)
    return statistics


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AdbcDataDictionary(SyncDataDictionaryBase):
    """ADBC multi-dialect data dictionary."""

    dialect: ClassVar[str] = "generic"

    def __init__(self) -> None:
        super().__init__()

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
            version_query_dialect = "mysql" if dialect == "mariadb" else dialect
            version_value = driver.select_value_or_none(self._get_query(version_query_dialect, "version"))
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
            version_info = self.get_version(driver)
            if dialect == "postgres":
                return resolve_postgres_json_type(version_info)
            if dialect == "sqlite":
                return resolve_sqlite_json_type(version_info)
            if dialect in {"mysql", "mariadb"}:
                return resolve_mysql_json_type(version_info)
            if dialect == "cockroachdb":
                return resolve_cockroachdb_json_type(version_info)

            json_version = config.get_feature_version("supports_json")
            if json_version and (version_info is None or version_info < json_version):
                return "TEXT"

        return config.get_optimal_type(type_category)

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

    def get_metadata_capabilities(
        self, driver: "AdbcDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Report ADBC transport metadata capabilities without implying DDL fidelity."""
        dialect = self._normalize_dialect(driver)
        requested_domains = tuple(domains or _ADBC_METADATA_DOMAINS)
        probes = self._probe_transport_metadata(driver)
        capabilities = tuple(self._capability_for_domain(dialect, domain, probes) for domain in requested_domains)
        return MetadataCapabilityProfile(dialect=dialect, adapter="adbc", capabilities=capabilities)

    def get_tables(self, driver: "AdbcDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables for the current dialect."""
        dialect = self._normalize_dialect(driver)
        schema_name: str | None = self._resolve_schema(dialect, schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")

        try:
            return self._native_get_tables(driver, dialect, schema_name)
        except (*_NATIVE_FALLBACK_ERRORS, _NativeMetadataIncompleteError) as exc:
            logger.debug("ADBC native get_objects unavailable for tables: %s", exc)

        if dialect == "bigquery":
            tables_table, kcu_table, rc_table = format_bigquery_information_schema_tables(schema_name)
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

        resolved_table = self._resolve_identifier(dialect, table) if table is not None else None
        try:
            return self._native_get_columns(driver, dialect, resolved_table, schema_name)
        except (*_NATIVE_FALLBACK_ERRORS, _NativeMetadataIncompleteError) as exc:
            logger.debug("ADBC native get_objects unavailable for columns: %s", exc)

        if dialect == "bigquery":
            schema_prefix = format_bigquery_schema_prefix(schema_name)
            if table is None:
                query_text = self._get_query_text(dialect, "columns_by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_name=schema_name, schema_type=ColumnMetadata)
            query_text = self._get_query_text(dialect, "columns_by_table").format(schema_prefix=schema_prefix)
            table_name = self._resolve_identifier(dialect, table)
            return driver.select(query_text, table_name=table_name, schema_name=schema_name, schema_type=ColumnMetadata)

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
        table_name = self._resolve_identifier(dialect, table)
        return driver.select(
            self._get_query(dialect, "columns_by_table"),
            schema_name=schema_name,
            table_name=table_name,
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

        if dialect == "duckdb":
            query_name = "indexes_by_schema" if table is None else "indexes_by_table"
            return driver.select(self._get_query(dialect, query_name), schema_type=IndexMetadata)

        if table is None:
            return driver.select(
                self._get_query(dialect, "indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        table_name = self._resolve_identifier(dialect, table)
        return driver.select(
            self._get_query(dialect, "indexes_by_table"),
            schema_name=schema_name,
            table_name=table_name,
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

        resolved_table = self._resolve_identifier(dialect, table) if table is not None else None
        try:
            return self._native_get_foreign_keys(driver, dialect, resolved_table, schema_name)
        except (*_NATIVE_FALLBACK_ERRORS, _NativeMetadataIncompleteError) as exc:
            logger.debug("ADBC native get_objects unavailable for foreign keys: %s", exc)

        if dialect == "bigquery":
            _, kcu_table, rc_table = format_bigquery_information_schema_tables(schema_name)
            if table is None:
                query_text = self._get_query_text(dialect, "foreign_keys_by_schema").format(
                    kcu_table=kcu_table, rc_table=rc_table
                )
                return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)
            query_text = self._get_query_text(dialect, "foreign_keys_by_table").format(
                kcu_table=kcu_table, rc_table=rc_table
            )
            table_name = self._resolve_identifier(dialect, table)
            return driver.select(
                query_text, table_name=table_name, schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

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

        resolved_table_name = self._resolve_identifier(dialect, table) if table is not None else None
        return driver.select(
            self._get_query(dialect, "foreign_keys_by_table"),
            schema_name=schema_name,
            table_name=resolved_table_name,
            schema_type=ForeignKeyMetadata,
        )

    def get_statistics(
        self, driver: "AdbcDriver", table: str, schema: "str | None" = None, *, approximate: bool = True
    ) -> "list[TableStatisticsMetadata]":
        """Get native driver statistics for a table via ADBC GetStatistics."""
        dialect = self._normalize_dialect(driver)
        schema_name = self._resolve_schema(dialect, schema)
        table_name = self._resolve_identifier(dialect, table)
        self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="statistics")
        filters = self._native_object_filters(dialect, schema_name, table_name)
        try:
            reader = driver.connection.adbc_get_statistics(
                catalog_filter=filters["catalog_filter"],
                db_schema_filter=filters["db_schema_filter"],
                table_name_filter=filters["table_name_filter"],
                approximate=approximate,
            )
        except _NATIVE_FALLBACK_ERRORS as exc:
            msg = f"ADBC driver for dialect {dialect!r} does not support native table statistics: {exc}"
            raise OperationalError(msg) from exc
        rows = reader.read_all().to_pylist()
        statistic_names = self._native_statistic_names(driver)
        return [
            entry for entry in _normalize_native_statistics(rows, statistic_names) if entry["table_name"] == table_name
        ]

    def get_constraints(
        self, driver: "AdbcDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get lossy ADBC constraint shells from GetObjects."""
        dialect = self._normalize_dialect(driver)
        schema_name = self._resolve_schema(dialect, schema)
        table_name = self._resolve_identifier(dialect, table) if table is not None else None
        try:
            rows = self._native_get_objects(driver, dialect, "all", schema_name, table_name)
        except (*_NATIVE_FALLBACK_ERRORS, _NativeMetadataIncompleteError) as exc:
            warning = f"ADBC GetObjects is unavailable for constraints: {exc}"
            capability = MetadataCapability(
                domain="constraints",
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(warning,),
            )
            return MetadataResult(domain="constraints", capability=capability, warnings=capability.warnings)

        capability = MetadataCapability(
            domain="constraints",
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.LOSSY,
            source=MetadataSource.DRIVER_METADATA,
            warnings=(_ADBC_CONSTRAINT_WARNING,),
        )
        return MetadataResult(
            domain="constraints",
            capability=capability,
            items=tuple(_normalize_native_constraints(rows, table_name_exact=table_name, dialect=dialect)),
            warnings=capability.warnings,
        )

    def get_ddl(
        self,
        driver: "AdbcDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Fail closed because ADBC metadata is not a lossless DDL source."""
        _ = include_dependencies, prefer_native, redact
        dialect = self._normalize_dialect(driver)
        schema_name = self._resolve_schema(dialect, schema)
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=schema_name,
            dialect=dialect,
            source=MetadataSource.DRIVER_METADATA,
        )
        return DDLResult.unsupported(
            identity, source=MetadataSource.DRIVER_METADATA, warnings=(_ADBC_DDL_UNSUPPORTED_WARNING,)
        )

    def get_system_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "tuple[SystemMetadataCapability, ...]":
        """Get ADBC system metadata capability disclosures."""
        requested_domains = ("table_statistics",) if domains is None else tuple(domains)
        return tuple(self._system_metadata_capability(domain) for domain in requested_domains)

    def get_system_metadata(
        self, driver: "AdbcDriver", request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Get ADBC transport metadata through the opt-in system namespace."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        capability = self._system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        if metadata_request.domain != "table_statistics":
            return gate_result
        if metadata_request.table is None:
            missing_table_capability = capability.with_support(
                MetadataSupport.GATED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                warnings=("ADBC table_statistics system metadata requires request.table.",),
            )
            return SystemMetadataResult(
                metadata_request, missing_table_capability, source=MetadataSource.DRIVER_METADATA
            )
        try:
            statistics = self.get_statistics(driver, metadata_request.table, metadata_request.schema)
        except OperationalError as exc:
            unsupported_capability = SystemMetadataCapability.unsupported(
                "table_statistics", source=MetadataSource.DRIVER_METADATA
            ).with_support(MetadataSupport.UNSUPPORTED, fidelity=MetadataFidelity.UNSUPPORTED, warnings=(str(exc),))
            return SystemMetadataResult(metadata_request, unsupported_capability, source=MetadataSource.DRIVER_METADATA)
        rows = tuple(cast("dict[str, object]", entry) for entry in statistics)
        return SystemMetadataResult.from_rows(
            metadata_request, capability, rows=rows, source=MetadataSource.DRIVER_METADATA
        )

    def _system_metadata_capability(self, domain: str) -> "SystemMetadataCapability":
        if domain != "table_statistics":
            return unsupported_system_metadata_capability(domain)
        return SystemMetadataCapability(
            "table_statistics",
            MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.TRANSPORT_FALLBACK,
            source=MetadataSource.DRIVER_METADATA,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.EXPENSIVE, MetadataRisk.REDACTED),
            required_privileges=("ADBC GetStatistics support",),
            redaction_fields=("catalog_name", "schema_name", "table_name", "column_name"),
            warnings=("ADBC statistics are driver-reported transport metadata, not a full system query namespace.",),
        )

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
        try:
            config = get_dialect_config(dialect)
        except ValueError:
            return schema
        if schema is not None:
            return normalize_identifier(schema, config.name)
        if config.default_schema is None:
            return None
        return normalize_identifier(config.default_schema, config.name)

    def _resolve_identifier(self, dialect: str, identifier: str) -> str:
        try:
            config = get_dialect_config(dialect)
        except ValueError:
            return identifier
        return normalize_identifier(identifier, config.name)

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

    def _probe_transport_metadata(self, driver: "AdbcDriver") -> "dict[str, bool]":
        connection = driver.connection
        return {
            "info": self._probe_callable(connection, "adbc_get_info"),
            "table_types": self._probe_callable(connection, "adbc_get_table_types"),
            "objects": self._probe_reader_callable(connection, "adbc_get_objects", depth="catalogs"),
            "table_schema": callable(getattr(connection, "adbc_get_table_schema", None)),
            "statistics": self._probe_reader_callable(
                connection, "adbc_get_statistics", table_name_filter="__sqlspec_metadata_probe__", approximate=True
            ),
            "statistic_names": self._probe_reader_callable(connection, "adbc_get_statistic_names"),
        }

    def _probe_callable(self, connection: Any, name: str) -> bool:
        method = getattr(connection, name, None)
        if not callable(method):
            return False
        try:
            method()
        except Exception:
            return False
        return True

    def _probe_reader_callable(self, connection: Any, name: str, **kwargs: Any) -> bool:
        method = getattr(connection, name, None)
        if not callable(method):
            return False
        try:
            reader = cast("Any", method(**kwargs))
            reader.read_all()
        except Exception:
            return False
        return True

    def _capability_for_domain(self, dialect: str, domain: str, probes: "dict[str, bool]") -> "MetadataCapability":
        if domain == "ddl":
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(_ADBC_DDL_UNSUPPORTED_WARNING,),
            )
        if domain in {"views", "routines", "privileges", "dependencies"}:
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(_ADBC_TRANSPORT_WARNING,),
            )
        if domain in {"statistics", "system"}:
            if probes["statistics"] or probes["statistic_names"]:
                fidelity = (
                    MetadataFidelity.TRANSPORT_FALLBACK
                    if probes["statistics"] and probes["statistic_names"]
                    else MetadataFidelity.PARTIAL
                )
                return MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=fidelity,
                    source=MetadataSource.DRIVER_METADATA,
                    risks=(MetadataRisk.EXPENSIVE,),
                    warnings=(_ADBC_STATISTICS_WARNING,),
                )
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=("ADBC statistics metadata is unavailable for this driver.",),
            )
        if domain == "constraints":
            if probes["objects"]:
                return MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.LOSSY,
                    source=MetadataSource.DRIVER_METADATA,
                    warnings=(_ADBC_CONSTRAINT_WARNING,),
                )
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=("ADBC GetObjects is unavailable for constraints.",),
            )
        if domain == "columns" and (probes["objects"] or probes["table_schema"]):
            return self._transport_capability(domain)
        if domain in {"schemas", "objects", "tables"} and (probes["objects"] or probes["table_types"]):
            return self._transport_capability(domain)
        if domain == "indexes" and self._has_query(dialect, "indexes_by_schema"):
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.NATIVE,
                source=MetadataSource.CATALOG,
                warnings=("ADBC uses the dialect SQL pack for index metadata; no transport index API is available.",),
            )
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.DRIVER_METADATA,
            warnings=(_ADBC_TRANSPORT_WARNING,),
        )

    def _transport_capability(self, domain: str) -> "MetadataCapability":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.TRANSPORT_FALLBACK,
            source=MetadataSource.DRIVER_METADATA,
            warnings=(_ADBC_TRANSPORT_WARNING,),
        )

    def _has_query(self, dialect: str, name: str) -> bool:
        try:
            self._get_query(dialect, name)
        except SQLFileNotFoundError:
            return False
        return True

    def _native_statistic_names(self, driver: "AdbcDriver") -> "dict[int, str]":
        try:
            reader = driver.connection.adbc_get_statistic_names()
            rows = reader.read_all().to_pylist()
        except Exception:
            return {}
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            return {}
        return _normalize_native_statistic_names(cast("list[dict[str, Any]]", rows))

    def _native_object_filters(
        self, dialect: str, schema_name: "str | None", table_name: "str | None"
    ) -> "dict[str, str | None]":
        catalog_filter: str | None = None
        db_schema_filter: str | None = None
        if schema_name:
            if dialect == "sqlite":
                catalog_filter = schema_name
            else:
                db_schema_filter = schema_name
        return {"catalog_filter": catalog_filter, "db_schema_filter": db_schema_filter, "table_name_filter": table_name}

    def _native_get_objects(
        self, driver: "AdbcDriver", dialect: str, depth: str, schema_name: "str | None", table_name: "str | None"
    ) -> "list[dict[str, Any]]":
        filters = self._native_object_filters(dialect, schema_name, table_name)
        reader = driver.connection.adbc_get_objects(
            depth=depth,
            catalog_filter=filters["catalog_filter"],
            db_schema_filter=filters["db_schema_filter"],
            table_name_filter=filters["table_name_filter"],
        )
        rows = reader.read_all().to_pylist()
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise _NativeMetadataIncompleteError
        return cast("list[dict[str, Any]]", rows)

    def _native_get_tables(
        self, driver: "AdbcDriver", dialect: str, schema_name: "str | None"
    ) -> "list[TableMetadata]":
        rows = self._native_get_objects(driver, dialect, "tables", schema_name, None)
        return _normalize_native_tables(rows)

    def _native_get_columns(
        self, driver: "AdbcDriver", dialect: str, table_name: "str | None", schema_name: "str | None"
    ) -> "list[ColumnMetadata]":
        rows = self._native_get_objects(driver, dialect, "all", schema_name, table_name)
        columns = _normalize_native_columns(rows, table_name_exact=table_name)
        missing_types = [entry for entry in columns if "data_type" not in entry]
        missing_nullability = any("is_nullable" not in entry for entry in columns)
        if not missing_types and not missing_nullability:
            return columns
        if table_name is None or missing_nullability:
            raise _NativeMetadataIncompleteError
        filters = self._native_object_filters(dialect, schema_name, None)
        arrow_schema = driver.connection.adbc_get_table_schema(
            table_name, catalog_filter=filters["catalog_filter"], db_schema_filter=filters["db_schema_filter"]
        )
        type_by_name = {field.name: _arrow_type_to_sql(field.type) for field in arrow_schema}
        for entry in missing_types:
            resolved = type_by_name.get(entry["column_name"])
            if resolved is not None:
                entry["data_type"] = resolved
        if any("data_type" not in entry or "is_nullable" not in entry for entry in columns):
            raise _NativeMetadataIncompleteError
        return columns

    def _native_get_foreign_keys(
        self, driver: "AdbcDriver", dialect: str, table_name: "str | None", schema_name: "str | None"
    ) -> "list[ForeignKeyMetadata]":
        rows = self._native_get_objects(driver, dialect, "all", schema_name, table_name)
        foreign_keys = _normalize_native_foreign_keys(rows, table_name_exact=table_name)
        if not foreign_keys:
            raise _NativeMetadataIncompleteError
        return foreign_keys
