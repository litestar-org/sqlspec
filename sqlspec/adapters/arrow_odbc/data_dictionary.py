"""Generic data dictionary for arrow-odbc connections."""

from typing import TYPE_CHECKING, Any, ClassVar, Final

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.utils.text import normalize_identifier, quote_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
    from sqlspec.core import SQL
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("ArrowOdbcDataDictionary",)

_ARROW_DECIMAL_FORMAT: Final = "DECIMAL({precision},{scale})"
_ODBC_METADATA_DOMAINS: Final = (
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
    "system",
    "odbc_catalog",
)
_ODBC_CATALOG_UNAVAILABLE_WARNING: Final = (
    "arrow-odbc does not expose SQLGetInfo, SQLGetFunctions, or raw ODBC catalog functions through its Python "
    "Connection API."
)
_ODBC_DDL_UNSUPPORTED_WARNING: Final = "Arrow ODBC transport metadata is not a lossless DDL source."
_ODBC_COLUMN_PARTIAL_WARNING: Final = (
    "Arrow ODBC column metadata comes from dialect SQL packs or zero-row Arrow schema probes, not SQLColumns."
)


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

    def get_metadata_capabilities(
        self, driver: "ArrowOdbcDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Report Arrow ODBC metadata capabilities without claiming raw catalog support."""
        _ = driver
        requested_domains = tuple(domains or _ODBC_METADATA_DOMAINS)
        capabilities = tuple(self._capability_for_domain(domain) for domain in requested_domains)
        return MetadataCapabilityProfile(dialect=self._dialect, adapter="arrow_odbc", capabilities=capabilities)

    def resolve_schema(self, schema: str | None) -> str | None:
        """Return a schema name using runtime dialect defaults when missing."""
        config = self.get_dialect_config()
        if schema is not None:
            return normalize_identifier(schema, config.name)
        if config.default_schema is None:
            return None
        return normalize_identifier(config.default_schema, config.name)

    def resolve_identifier(self, identifier: str) -> str:
        """Return a runtime-dialect-normalized identifier."""
        return normalize_identifier(identifier, self.get_dialect_config().name)

    def _probe_columns(self, driver: "ArrowOdbcDriver", table: str, schema: "str | None") -> "list[ColumnMetadata]":
        qualified = (
            quote_identifier(table) if schema is None else f"{quote_identifier(schema)}.{quote_identifier(table)}"
        )
        probe_sql = f"SELECT * FROM {qualified} WHERE 1=0"
        try:
            reader = driver._read_arrow_batches(probe_sql, None, 1)  # pyright: ignore[reportPrivateUsage]
        except Exception:
            return []
        columns: list[ColumnMetadata] = []
        for position, field in enumerate(reader.schema, start=1):
            entry: ColumnMetadata = {
                "table_name": table,
                "column_name": field.name,
                "data_type": _arrow_type_to_sql(field.type),
                "is_nullable": bool(field.nullable),
                "ordinal_position": position,
            }
            if schema is not None:
                entry["schema_name"] = schema
            columns.append(entry)
        return columns

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
        resolved_schema = self.resolve_schema(schema)
        resolved_table = self.resolve_identifier(table) if table is not None else None
        parameters: dict[str, Any] = {"schema_name": resolved_schema}
        if table is not None:
            parameters["table_name"] = resolved_table
        try:
            rows = driver.select(self.get_query(query_name), schema_type=ColumnMetadata, **parameters)
        except SQLFileNotFoundError:
            rows = []
        if rows or resolved_table is None:
            return rows
        return self._probe_columns(driver, resolved_table, resolved_schema)

    def get_indexes(
        self, driver: "ArrowOdbcDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get index metadata for dialects with bundled catalog queries."""
        query_name = "indexes_by_table" if table is not None else "indexes_by_schema"
        parameters: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        if table is not None:
            parameters["table_name"] = self.resolve_identifier(table)
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
            parameters["table_name"] = self.resolve_identifier(table)
        try:
            return driver.select(self.get_query(query_name), schema_type=ForeignKeyMetadata, **parameters)
        except SQLFileNotFoundError:
            return []

    def get_ddl(
        self,
        driver: "ArrowOdbcDriver",
        object_name: str,
        schema: str | None = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> DDLResult:
        """Fail closed because arrow-odbc does not expose lossless DDL metadata."""
        _ = driver, include_dependencies, prefer_native, redact
        schema_name = self.resolve_schema(schema)
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=schema_name,
            dialect=self._dialect,
            source=MetadataSource.DRIVER_METADATA,
        )
        return DDLResult.unsupported(
            identity, source=MetadataSource.DRIVER_METADATA, warnings=(_ODBC_DDL_UNSUPPORTED_WARNING,)
        )

    def _capability_for_domain(self, domain: str) -> MetadataCapability:
        if domain == "odbc_catalog":
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(_ODBC_CATALOG_UNAVAILABLE_WARNING,),
            )
        if domain == "ddl":
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.UNSUPPORTED,
                fidelity=MetadataFidelity.UNSUPPORTED,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(_ODBC_DDL_UNSUPPORTED_WARNING,),
            )
        if domain == "columns":
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.PARTIAL,
                source=MetadataSource.DRIVER_METADATA,
                warnings=(_ODBC_COLUMN_PARTIAL_WARNING,),
            )
        query_by_domain = {
            "tables": "tables_by_schema",
            "indexes": "indexes_by_schema",
            "foreign_keys": "foreign_keys_by_schema",
        }
        query_name = query_by_domain.get(domain)
        if query_name is not None and self._has_query(query_name):
            return MetadataCapability(
                domain=domain,
                support=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.NATIVE,
                source=MetadataSource.CATALOG,
            )
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.DRIVER_METADATA,
            warnings=(_ODBC_CATALOG_UNAVAILABLE_WARNING,),
        )

    def _has_query(self, name: str) -> bool:
        try:
            self.get_query(name)
        except SQLFileNotFoundError:
            return False
        return True
