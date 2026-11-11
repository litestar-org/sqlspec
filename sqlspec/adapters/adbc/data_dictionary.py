"""ADBC multi-dialect data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.adbc._typing import AdbcNotSupportedError, AdbcOperationalError, AdbcProgrammingError
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
from sqlspec.data_dictionary.dialects.oracle import parse_oracle_version_components, resolve_oracle_json_type
from sqlspec.data_dictionary.dialects.postgres import resolve_postgres_json_type
from sqlspec.data_dictionary.dialects.sqlite import format_sqlite_schema_prefix, resolve_sqlite_json_type
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import OperationalError, SQLFileNotFoundError, SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow, import_optional
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.core import SQL

__all__ = ("AdbcDataDictionary",)

logger = get_logger("sqlspec.adapters.adbc")

_NATIVE_TABLE_TYPES: Final = frozenset({"table", "base table"})
_NATIVE_FALLBACK_ERRORS: Final = (AdbcNotSupportedError, AdbcOperationalError, AdbcProgrammingError, AttributeError)
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


_ARROW_DECIMAL_FORMAT: Final = "DECIMAL({precision},{scale})"


_ADBC_STATISTIC_NAMES: dict[int, str] = {
    0: "adbc.statistic.byte_width",
    1: "adbc.statistic.distinct_count",
    2: "adbc.statistic.max_byte_width",
    3: "adbc.statistic.max_value",
    4: "adbc.statistic.min_value",
    5: "adbc.statistic.null_count",
    6: "adbc.statistic.row_count",
}


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AdbcDataDictionary(SyncDataDictionaryBase):
    """ADBC multi-dialect data dictionary."""

    dialect: ClassVar[str] = "generic"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "AdbcDriver") -> "VersionInfo | None":
        """Get database version information for the active ADBC dialect."""
        dialect = self._normalize_dialect(driver)
        if dialect == "bigquery":
            return None

        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        try:
            version_query_dialect = "mysql" if dialect == "mariadb" else dialect
            version_value = driver.select_value_or_none(self._get_query(version_query_dialect, "version", "current"))
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

        if dialect == "oracle":
            components = parse_oracle_version_components(str(version_value))
            version_info = VersionInfo(*components) if components is not None else None
        else:
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
            if dialect == "oracle":
                return resolve_oracle_json_type(
                    version_info,
                    compatible_major=version_info.major if version_info is not None else None,
                    is_autonomous=False,
                )

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
            query_text = self._get_query_text(dialect, "tables", "by_schema").format(
                tables_table=tables_table, kcu_table=kcu_table, rc_table=rc_table
            )
            return driver.select(query_text, schema_type=TableMetadata)

        if dialect == "sqlite":
            schema_prefix = format_sqlite_schema_prefix(schema_name)
            query_text = self._get_query_text(dialect, "tables", "by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_name=schema_name, schema_type=TableMetadata)

        if dialect == "snowflake":
            return self._snowflake_get_tables(driver, schema_name)

        rows = driver.select(self._get_query(dialect, "tables", "by_schema"), schema_name=schema_name, table_name=None)
        return cast("list[TableMetadata]", _lowercase_dict_rows(rows, dialect))

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
                query_text = self._get_query_text(dialect, "columns", "by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_name=schema_name, schema_type=ColumnMetadata)
            query_text = self._get_query_text(dialect, "columns", "by_table").format(schema_prefix=schema_prefix)
            table_name = self._resolve_identifier(dialect, table)
            return driver.select(query_text, table_name=table_name, schema_name=schema_name, schema_type=ColumnMetadata)

        if dialect == "sqlite":
            schema_prefix = format_sqlite_schema_prefix(schema_name)
            if table is None:
                query_text = self._get_query_text(dialect, "columns", "by_schema").format(schema_prefix=schema_prefix)
                return driver.select(query_text, schema_name=schema_name, schema_type=ColumnMetadata)
            query_text = self._get_query_text(dialect, "columns", "by_table").format(
                schema_prefix=schema_prefix,
                table_name=_format_sqlite_identifier(f"{schema_name}.{table}" if schema_name else table),
            )
            return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ColumnMetadata)

        if dialect == "snowflake":
            return self._snowflake_get_columns(driver, resolved_table, schema_name)

        if table is None:
            rows = driver.select(
                self._get_query(dialect, "columns", "by_schema"), schema_name=schema_name, table_name=None
            )
            return cast("list[ColumnMetadata]", _lowercase_dict_rows(rows, dialect))
        table_name = self._resolve_identifier(dialect, table)
        rows = driver.select(
            self._get_query(dialect, "columns", "by_table"), schema_name=schema_name, table_name=table_name
        )
        return cast("list[ColumnMetadata]", _lowercase_dict_rows(rows, dialect))

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
            schema_prefix = format_sqlite_schema_prefix(schema_name)
            if table is None:
                query_text = self._get_query_text(dialect, "indexes", "by_schema").format(schema_prefix=schema_prefix)
                rows = driver.select(query_text, schema_name=schema_name)
                return _sqlite_index_rows_to_metadata(rows, schema_name)

            query_text = self._get_query_text(dialect, "indexes", "by_table").format(schema_prefix=schema_prefix)
            rows = driver.select(query_text, table_name=table, schema_name=schema_name)
            return _sqlite_index_rows_to_metadata(rows, schema_name)

        if dialect == "snowflake":
            return []

        try:
            if table is None:
                rows = driver.select(
                    self._get_query(dialect, "indexes", "by_schema"), schema_name=schema_name, table_name=None
                )
                return _normalize_sql_index_rows(rows, dialect)

            table_name = self._resolve_identifier(dialect, table)
            rows = driver.select(
                self._get_query(dialect, "indexes", "by_table"), schema_name=schema_name, table_name=table_name
            )
            return _normalize_sql_index_rows(rows, dialect)
        except SQLFileNotFoundError:
            return []

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
                query_text = self._get_query_text(dialect, "foreign_keys", "by_schema").format(
                    kcu_table=kcu_table, rc_table=rc_table
                )
                return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)
            query_text = self._get_query_text(dialect, "foreign_keys", "by_table").format(
                kcu_table=kcu_table, rc_table=rc_table
            )
            table_name = self._resolve_identifier(dialect, table)
            return driver.select(
                query_text, table_name=table_name, schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        if dialect == "sqlite":
            if table is None:
                schema_prefix = format_sqlite_schema_prefix(schema_name)
                query_text = self._get_query_text(dialect, "foreign_keys", "by_schema").format(
                    schema_prefix=schema_prefix
                )
                return driver.select(query_text, schema_name=schema_name, schema_type=ForeignKeyMetadata)
            table_label = table.replace("'", "''")
            table_identifier = f"{schema_name}.{table}" if schema_name else table
            query_text = self._get_query_text(dialect, "foreign_keys", "by_table").format(
                table_name=_format_sqlite_identifier(table_identifier), table_label=table_label
            )
            return driver.select(query_text, table_name=table, schema_name=schema_name, schema_type=ForeignKeyMetadata)

        if table is None:
            query_text_optional = self._get_query_text_or_none(dialect, "foreign_keys", "by_schema")
            if query_text_optional is not None:
                rows = driver.select(query_text_optional, schema_name=schema_name, schema_type=ForeignKeyMetadata)
                return cast("list[ForeignKeyMetadata]", _lowercase_dict_rows(rows, dialect))

        resolved_table_name = self._resolve_identifier(dialect, table) if table is not None else None
        rows = driver.select(
            self._get_query(dialect, "foreign_keys", "by_table"),
            schema_name=schema_name,
            table_name=resolved_table_name,
            schema_type=ForeignKeyMetadata,
        )
        return cast("list[ForeignKeyMetadata]", _lowercase_dict_rows(rows, dialect))

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

    def resolve_ddl_dialect(self, driver: "AdbcDriver") -> "str | None":
        """Return the dialect detected from the ADBC driver for parsing and rendering DDL."""
        dialect = self._normalize_dialect(driver)
        return None if dialect in {"", "generic", "unknown"} else dialect

    def resolve_ddl_object_name(
        self, driver: "AdbcDriver", object_name: str, schema: "str | None"
    ) -> "tuple[str, str | None]":
        """Resolve a DDL object name against the dialect detected from the ADBC driver."""
        dialect = self._normalize_dialect(driver)
        return (self._resolve_identifier(dialect, object_name), self._resolve_schema(dialect, schema))

    def exact_catalog_identifier(self, driver: "AdbcDriver", name: str) -> str:
        """Return ``name`` in the form ADBC metadata getters resolve back to the exact catalog name."""
        if self._resolve_identifier(self._normalize_dialect(driver), name) == name:
            return name
        return quote_identifier(name)

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

    def _get_native_object_ddl(
        self,
        driver: "AdbcDriver",
        object_name: str,
        *,
        schema: "str | None",
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> "DDLResult | None":
        """Fail closed because ADBC metadata is not a lossless DDL source; tables fall back to synthesis."""
        _ = include_dependencies, redact
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=schema,
            dialect=self._normalize_dialect(driver),
            source=MetadataSource.DRIVER_METADATA,
        )
        return DDLResult.unsupported(
            identity,
            source=MetadataSource.DRIVER_METADATA,
            warnings=(_ADBC_DDL_UNSUPPORTED_WARNING, _ADBC_TRANSPORT_WARNING),
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
        if str(dialect_value).strip().lower() == "vantage":
            return "teradata"
        return normalize_dialect_name(dialect_value)

    def _get_query(self, dialect: str, domain: str, operation: str) -> "SQL":
        loader = get_data_dictionary_loader()
        query = loader.get_domain_query(dialect, domain, operation)
        if query.sql is None:
            msg = f"No data-dictionary query found for {dialect}/{domain}/{operation}"
            raise SQLFileNotFoundError(msg)
        return query.sql

    def _get_query_text(self, dialect: str, domain: str, operation: str) -> str:
        return self._get_query(dialect, domain, operation).raw_sql

    def _get_query_text_or_none(self, dialect: str, domain: str, operation: str) -> "str | None":
        try:
            return self._get_query_text(dialect, domain, operation)
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
        if domain == "indexes" and self._has_query(dialect, "indexes", "by_schema"):
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

    def _has_query(self, dialect: str, domain: str, operation: str) -> bool:
        try:
            self._get_query(dialect, domain, operation)
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
        if dialect == "teradata":
            rows = _strip_teradata_metadata(rows)
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
        if not rows:
            raise _NativeMetadataIncompleteError
        for _, _, table in _iter_object_tables(rows):
            if table.get("table_constraints") is None:
                raise _NativeMetadataIncompleteError
        return _normalize_native_foreign_keys(rows, table_name_exact=table_name)

    def _snowflake_get_tables(self, driver: "AdbcDriver", schema_name: "str | None") -> "list[TableMetadata]":
        if schema_name is not None:
            rows = driver.select(
                "SELECT table_schema AS schema_name, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE') AND UPPER(table_schema) = UPPER(?) "
                "ORDER BY table_schema, table_name",
                schema_name,
            )
        else:
            rows = driver.select(
                "SELECT table_schema AS schema_name, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE') "
                "AND UPPER(table_schema) != 'INFORMATION_SCHEMA' "
                "ORDER BY table_schema, table_name"
            )
        return cast("list[TableMetadata]", _lowercase_dict_rows(rows, "snowflake"))

    def _snowflake_get_columns(
        self, driver: "AdbcDriver", table_name: "str | None", schema_name: "str | None"
    ) -> "list[ColumnMetadata]":
        base_select = (
            "SELECT table_schema AS schema_name, table_name, column_name, data_type, "
            "ordinal_position, is_nullable, column_default "
            "FROM information_schema.columns "
        )
        if table_name is not None and schema_name is not None:
            rows = driver.select(
                f"{base_select}WHERE UPPER(table_schema) = UPPER(?) AND UPPER(table_name) = UPPER(?) "
                "ORDER BY ordinal_position",
                schema_name,
                table_name,
            )
        elif table_name is not None:
            rows = driver.select(
                f"{base_select}WHERE UPPER(table_schema) != 'INFORMATION_SCHEMA' AND UPPER(table_name) = UPPER(?) "
                "ORDER BY table_schema, ordinal_position",
                table_name,
            )
        elif schema_name is not None:
            rows = driver.select(
                f"{base_select}WHERE UPPER(table_schema) = UPPER(?) ORDER BY table_name, ordinal_position", schema_name
            )
        else:
            rows = driver.select(
                f"{base_select}WHERE UPPER(table_schema) != 'INFORMATION_SCHEMA' "
                "ORDER BY table_schema, table_name, ordinal_position"
            )
        return cast("list[ColumnMetadata]", _lowercase_dict_rows(rows, "snowflake"))


def _lowercase_dict_rows(rows: "Sequence[Any]", dialect: str = "") -> "list[dict[str, Any]]":
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append({
                str(key).lower(): _strip_teradata_metadata(value) if dialect == "teradata" else value
                for key, value in row.items()
            })
        else:
            normalized.append(cast("dict[str, Any]", row))
    return normalized


def _normalize_sql_index_rows(rows: "Sequence[Any]", dialect: str) -> "list[IndexMetadata]":
    lowered = _lowercase_dict_rows(rows, dialect)
    if not lowered:
        return []
    if dialect == "db2" or ("column_name" in lowered[0] and "columns" not in lowered[0]):
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in lowered:
            if not isinstance(row, dict):
                continue
            key = (str(row.get("schema_name", "")), str(row.get("table_name", "")), str(row.get("index_name", "")))
            entry = grouped.get(key)
            if entry is None:
                entry = {
                    "schema_name": key[0],
                    "table_name": key[1],
                    "index_name": key[2],
                    "columns": [],
                    "is_unique": _coerce_index_unique(row.get("is_unique", row.get("unique", 0))),
                    "is_primary": _coerce_index_unique(row.get("is_primary", 0)),
                }
                grouped[key] = entry
            col_name = row.get("column_name")
            if col_name:
                cast("list[str]", entry["columns"]).append(str(col_name))
        return [cast("IndexMetadata", item) for item in grouped.values()]

    for row in lowered:
        if isinstance(row, dict) and isinstance(row.get("columns"), str):
            row["columns"] = [col.strip() for col in row["columns"].split(",") if col.strip()]
    return cast("list[IndexMetadata]", lowered)


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
            if nullable is None and column.get("xdbc_nullable") in {0, 1}:
                nullable = "YES" if column["xdbc_nullable"] == 1 else "NO"
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
            autoincrement = column.get("xdbc_is_autoincrement")
            if autoincrement is not None and str(autoincrement).strip().upper() in {"YES", "TRUE", "1", "Y"}:
                entry["is_identity"] = True
            generated = column.get("xdbc_is_generatedcolumn")
            if generated is not None and str(generated).strip().upper() in {"YES", "TRUE", "1", "Y"}:
                entry["is_generated"] = True
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


def _arrow_type_to_sql(data_type: Any) -> str:
    ensure_pyarrow()
    pa = import_optional("pyarrow")
    assert pa is not None
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


_SQLITE_INDEX_EXPRESSION_COLUMN_ID: Final[int] = -2


def _sqlite_index_rows_to_metadata(rows: "list[dict[str, Any]]", schema_name: "str | None") -> "list[IndexMetadata]":
    """Group SQLite pragma_index_xinfo rows into public IndexMetadata records."""
    if not rows:
        return []
    if "columns" in rows[0]:
        return cast("list[IndexMetadata]", rows)

    grouped: dict[tuple[str | None, str, str], dict[str, Any]] = {}
    for row in rows:
        table_name_value = row.get("table_name")
        index_name_value = row.get("index_name")
        if table_name_value is None or index_name_value is None:
            continue
        row_schema = cast("str | None", row.get("schema_name", schema_name))
        table_name = str(table_name_value)
        index_name = str(index_name_value)
        key = (row_schema, table_name, index_name)
        index_metadata = grouped.get(key)
        if index_metadata is None:
            index_metadata = {
                "index_name": index_name,
                "table_name": table_name,
                "columns": [],
                "is_primary": bool(row.get("is_primary")),
            }
            if row_schema is not None:
                index_metadata["schema_name"] = row_schema
            unique_value = row.get("is_unique")
            if unique_value is not None:
                index_metadata["is_unique"] = unique_value
            if row.get("is_partial") is not None:
                index_metadata["is_partial"] = row.get("is_partial")
            if row.get("index_sql") is not None:
                index_metadata["native_sql"] = row.get("index_sql")
            grouped[key] = index_metadata

        if row.get("is_key_column") in (0, False):
            continue
        columns = cast("list[str]", index_metadata["columns"])
        column_name = row.get("column_name")
        if column_name is not None:
            columns.append(str(column_name))
        elif row.get("column_id") == _SQLITE_INDEX_EXPRESSION_COLUMN_ID:
            columns.append("<expression>")

    return [cast("IndexMetadata", metadata) for metadata in grouped.values()]


def _format_sqlite_identifier(identifier: str) -> str:
    """Format and validate a SQLite identifier for PRAGMA table-valued function queries."""
    cleaned = identifier.strip()
    if not cleaned or "\x00" in cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)

    parts = split_qualified_identifier(cleaned, quote_chars='"')
    if not parts:
        msg = f"Invalid SQLite identifier: {identifier!r}"
        raise SQLSpecError(msg)
    return ".".join(quote_identifier(part) for part in parts)


def _strip_teradata_metadata(value: Any) -> Any:
    """Remove fixed CHAR padding from a Teradata metadata tree."""
    if isinstance(value, str):
        return value.rstrip()
    if isinstance(value, list):
        return [_strip_teradata_metadata(item) for item in value]
    if isinstance(value, dict):
        return {key: _strip_teradata_metadata(item) for key, item in value.items()}
    return value


def _coerce_index_unique(value: Any) -> bool:
    """Interpret numeric, boolean, and catalog character uniqueness flags."""
    if isinstance(value, str):
        return value.strip().upper() in {"1", "TRUE", "T", "Y", "YES", "U", "Q", "K", "P"}
    return bool(value) if isinstance(value, (bool, int, float)) else False
