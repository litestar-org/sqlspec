"""MySQL-specific data dictionary for metadata queries via mysql-connector."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    MetadataSupport,
    SystemMetadataResult,
    TableMetadata,
    VersionInfo,
    ensure_system_metadata_request,
    get_data_dictionary_loader,
    system_metadata_gated_result,
)
from sqlspec.data_dictionary.dialects.mysql import (
    MySQLEngineVersion,
    build_mysql_metadata_capability_profile,
    build_mysql_show_create_statement,
    build_mysql_system_metadata_capability,
    make_mysql_ddl_result,
    mysql_system_metadata_query_name,
    parse_mysql_engine_version,
    resolve_mysql_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
    from sqlspec.data_dictionary import SystemMetadataRequest

__all__ = ("MysqlConnectorAsyncDataDictionary", "MysqlConnectorSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MysqlConnectorSyncDataDictionary(SyncDataDictionaryBase):
    """MySQL-specific sync data dictionary."""

    dialect: ClassVar[str] = "mysql"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "MysqlConnectorSyncDriver") -> "VersionInfo | None":
        """Get MySQL database version information."""
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        # Not cached, fetch from database

        version_value = driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "MysqlConnectorSyncDriver", feature: str) -> bool:
        """Check if MySQL database supports a specific feature."""
        version_info = self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    def get_metadata_capabilities(
        self, driver: "MysqlConnectorSyncDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get replacement data-dictionary capability profile."""
        engine_version = self._get_engine_version(driver)
        dialect = engine_version.engine_family if engine_version is not None else type(self).dialect
        requested_domains = None if domains is None else tuple(domains)
        return build_mysql_metadata_capability_profile(dialect, type(self).__name__, requested_domains)

    def get_schemas(self, driver: "MysqlConnectorSyncDriver") -> "MetadataResult":
        """Get schema metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        return self._select_domain_result(driver, dialect, "schemas", "list", schema_name=None)

    def get_objects(self, driver: "MysqlConnectorSyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get object metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return self._select_domain_result(driver, dialect, "objects", "by_schema", schema_name=schema_name)

    def get_table_details(
        self, driver: "MysqlConnectorSyncDriver", table: str, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get rich table metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table)
        return self._select_domain_result(
            driver, dialect, "tables", "by_schema", schema_name=schema_name, table_name=table_name
        )

    def get_constraints(
        self, driver: "MysqlConnectorSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get constraint metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        return self._select_domain_result(
            driver, dialect, "constraints", "by_schema", schema_name=schema_name, table_name=table_name
        )

    def get_views(self, driver: "MysqlConnectorSyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return self._select_domain_result(driver, dialect, "views", "by_schema", schema_name=schema_name)

    def get_routines(self, driver: "MysqlConnectorSyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get routine metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return self._select_domain_result(driver, dialect, "routines", "by_schema", schema_name=schema_name)

    def get_privileges(
        self, driver: "MysqlConnectorSyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get privilege metadata from INFORMATION_SCHEMA."""
        dialect = self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return self._select_domain_result(driver, dialect, "privileges", "by_schema", schema_name=schema_name)

    def get_ddl(
        self,
        driver: "MysqlConnectorSyncDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Get native SHOW CREATE output and replay-sensitive context for a table."""
        _ = include_dependencies, prefer_native, redact
        schema_name = self._resolve_metadata_schema(schema)
        raw_version = driver.select_value_or_none(self.get_query_text("version"))
        sql_mode = driver.select_value_or_none("SELECT @@sql_mode")
        sql_quote_show_create = driver.select_value_or_none("SELECT @@sql_quote_show_create")
        statement = build_mysql_show_create_statement(object_name, schema_name, object_type)
        row = driver.select_one(statement, schema_type=dict)
        return make_mysql_ddl_result(
            object_name,
            schema_name,
            object_type,
            row,
            str(raw_version) if raw_version is not None else None,
            str(sql_mode) if sql_mode is not None else None,
            str(sql_quote_show_create) if sql_quote_show_create is not None else None,
        )

    def get_system_metadata(
        self, driver: "MysqlConnectorSyncDriver", request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Get opt-in system metadata from performance_schema or sys."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        dialect = self._get_metadata_dialect(driver)
        capability = build_mysql_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        query_name = mysql_system_metadata_query_name(metadata_request.domain)
        if query_name is None:
            return gate_result
        query = get_data_dictionary_loader().get_domain_query(dialect, "system", query_name)
        if not query.is_supported or query.sql is None:
            return gate_result
        if query_name == "sys_schema_table_statistics":
            rows = driver.select(query.sql, schema_name=metadata_request.schema)
        else:
            rows = driver.select(query.sql)
        return SystemMetadataResult.from_rows(
            metadata_request,
            capability,
            rows=tuple(cast("dict[str, object]", row) for row in rows),
            source=capability.source,
        )

    def get_optimal_type(self, driver: "MysqlConnectorSyncDriver", type_category: str) -> str:
        """Get optimal MySQL type for a category."""
        config = self.get_dialect_config()
        version_info = self.get_version(driver)

        if type_category == "json":
            return resolve_mysql_json_type(version_info)

        return config.get_optimal_type(type_category)

    def get_tables(self, driver: "MysqlConnectorSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using the MySQL catalog."""
        schema_name = self._resolve_metadata_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "MysqlConnectorSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return driver.select(
            self.get_query("columns_by_table"), table_name=table, schema_name=schema_name, schema_type=ColumnMetadata
        )

    def get_indexes(
        self, driver: "MysqlConnectorSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return driver.select(
            self.get_query("indexes_by_table"), table_name=table, schema_name=schema_name, schema_type=IndexMetadata
        )

    def get_foreign_keys(
        self, driver: "MysqlConnectorSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )

    def _resolve_metadata_schema(self, schema: "str | None") -> "str | None":
        schema_name = self.resolve_schema(schema)
        return schema_name or None

    def _get_engine_version(self, driver: "MysqlConnectorSyncDriver") -> "MySQLEngineVersion | None":
        try:
            version_value = driver.select_value_or_none(self.get_query_text("version"))
        except Exception:
            return None
        if version_value is None:
            return None
        return parse_mysql_engine_version(str(version_value))

    def _get_metadata_dialect(self, driver: "MysqlConnectorSyncDriver") -> str:
        engine_version = self._get_engine_version(driver)
        return engine_version.engine_family if engine_version is not None else type(self).dialect

    def _select_domain_result(
        self,
        driver: "MysqlConnectorSyncDriver",
        dialect: str,
        domain: str,
        query_name: str,
        *,
        schema_name: "str | None",
        table_name: "str | None" = None,
    ) -> "MetadataResult":
        query = get_data_dictionary_loader().get_domain_query(dialect, domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult.unsupported(domain)
        capability = build_mysql_metadata_capability_profile(dialect, type(self).__name__).get(domain)
        if table_name is None:
            rows = driver.select(query.sql, schema_name=schema_name)
        else:
            rows = driver.select(query.sql, schema_name=schema_name, table_name=table_name)
        return MetadataResult(domain, capability=capability, items=tuple(rows))


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MysqlConnectorAsyncDataDictionary(AsyncDataDictionaryBase):
    """MySQL-specific async data dictionary."""

    dialect: ClassVar[str] = "mysql"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: "MysqlConnectorAsyncDriver") -> "VersionInfo | None":
        """Get MySQL database version information."""
        driver_id = id(driver)
        # Inline cache check to avoid cross-module method call that causes mypyc segfault
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)
        # Not cached, fetch from database

        version_value = await driver.select_value_or_none(self.get_query("version"))
        if not version_value:
            self._log_version_unavailable(type(self).dialect, "missing")
            self.cache_version(driver_id, None)
            return None

        version_info = self.parse_version_with_pattern(self.get_dialect_config().version_pattern, str(version_value))
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "parse_failed")
            self.cache_version(driver_id, None)
            return None

        self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "MysqlConnectorAsyncDriver", feature: str) -> bool:
        """Check if MySQL database supports a specific feature."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_metadata_capabilities(
        self, driver: "MysqlConnectorAsyncDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get replacement data-dictionary capability profile."""
        engine_version = await self._get_engine_version(driver)
        dialect = engine_version.engine_family if engine_version is not None else type(self).dialect
        requested_domains = None if domains is None else tuple(domains)
        return build_mysql_metadata_capability_profile(dialect, type(self).__name__, requested_domains)

    async def get_schemas(self, driver: "MysqlConnectorAsyncDriver") -> "MetadataResult":
        """Get schema metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        return await self._select_domain_result(driver, dialect, "schemas", "list", schema_name=None)

    async def get_objects(self, driver: "MysqlConnectorAsyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get object metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "objects", "by_schema", schema_name=schema_name)

    async def get_table_details(
        self, driver: "MysqlConnectorAsyncDriver", table: str, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get rich table metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table)
        return await self._select_domain_result(
            driver, dialect, "tables", "by_schema", schema_name=schema_name, table_name=table_name
        )

    async def get_constraints(
        self, driver: "MysqlConnectorAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get constraint metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        return await self._select_domain_result(
            driver, dialect, "constraints", "by_schema", schema_name=schema_name, table_name=table_name
        )

    async def get_views(self, driver: "MysqlConnectorAsyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "views", "by_schema", schema_name=schema_name)

    async def get_routines(self, driver: "MysqlConnectorAsyncDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get routine metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "routines", "by_schema", schema_name=schema_name)

    async def get_privileges(
        self, driver: "MysqlConnectorAsyncDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get privilege metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "privileges", "by_schema", schema_name=schema_name)

    async def get_ddl(
        self,
        driver: "MysqlConnectorAsyncDriver",
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
    ) -> "DDLResult":
        """Get native SHOW CREATE output and replay-sensitive context for a table."""
        _ = include_dependencies, prefer_native, redact
        schema_name = self._resolve_metadata_schema(schema)
        raw_version = await driver.select_value_or_none(self.get_query_text("version"))
        sql_mode = await driver.select_value_or_none("SELECT @@sql_mode")
        sql_quote_show_create = await driver.select_value_or_none("SELECT @@sql_quote_show_create")
        statement = build_mysql_show_create_statement(object_name, schema_name, object_type)
        row = await driver.select_one(statement, schema_type=dict)
        return make_mysql_ddl_result(
            object_name,
            schema_name,
            object_type,
            row,
            str(raw_version) if raw_version is not None else None,
            str(sql_mode) if sql_mode is not None else None,
            str(sql_quote_show_create) if sql_quote_show_create is not None else None,
        )

    async def get_system_metadata(
        self, driver: "MysqlConnectorAsyncDriver", request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Get opt-in system metadata from performance_schema or sys."""
        metadata_request = ensure_system_metadata_request(request, **kwargs)
        dialect = await self._get_metadata_dialect(driver)
        capability = build_mysql_system_metadata_capability(metadata_request.domain)
        gate_result = system_metadata_gated_result(metadata_request, capability)
        if gate_result.capability.support != MetadataSupport.SUPPORTED:
            return gate_result
        query_name = mysql_system_metadata_query_name(metadata_request.domain)
        if query_name is None:
            return gate_result
        query = get_data_dictionary_loader().get_domain_query(dialect, "system", query_name)
        if not query.is_supported or query.sql is None:
            return gate_result
        if query_name == "sys_schema_table_statistics":
            rows = await driver.select(query.sql, schema_name=metadata_request.schema)
        else:
            rows = await driver.select(query.sql)
        return SystemMetadataResult.from_rows(
            metadata_request,
            capability,
            rows=tuple(cast("dict[str, object]", row) for row in rows),
            source=capability.source,
        )

    async def get_optimal_type(self, driver: "MysqlConnectorAsyncDriver", type_category: str) -> str:
        """Get optimal MySQL type for a category."""
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            return resolve_mysql_json_type(version_info)

        return config.get_optimal_type(type_category)

    async def get_tables(
        self, driver: "MysqlConnectorAsyncDriver", schema: "str | None" = None
    ) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using the MySQL catalog."""
        schema_name = self._resolve_metadata_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "MysqlConnectorAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return await driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return await driver.select(
            self.get_query("columns_by_table"), table_name=table, schema_name=schema_name, schema_type=ColumnMetadata
        )

    async def get_indexes(
        self, driver: "MysqlConnectorAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return await driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return await driver.select(
            self.get_query("indexes_by_table"), table_name=table, schema_name=schema_name, schema_type=IndexMetadata
        )

    async def get_foreign_keys(
        self, driver: "MysqlConnectorAsyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self._resolve_metadata_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return await driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return await driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )

    def _resolve_metadata_schema(self, schema: "str | None") -> "str | None":
        schema_name = self.resolve_schema(schema)
        return schema_name or None

    async def _get_engine_version(self, driver: "MysqlConnectorAsyncDriver") -> "MySQLEngineVersion | None":
        try:
            version_value = await driver.select_value_or_none(self.get_query_text("version"))
        except Exception:
            return None
        if version_value is None:
            return None
        return parse_mysql_engine_version(str(version_value))

    async def _get_metadata_dialect(self, driver: "MysqlConnectorAsyncDriver") -> str:
        engine_version = await self._get_engine_version(driver)
        return engine_version.engine_family if engine_version is not None else type(self).dialect

    async def _select_domain_result(
        self,
        driver: "MysqlConnectorAsyncDriver",
        dialect: str,
        domain: str,
        query_name: str,
        *,
        schema_name: "str | None",
        table_name: "str | None" = None,
    ) -> "MetadataResult":
        query = get_data_dictionary_loader().get_domain_query(dialect, domain, query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult.unsupported(domain)
        capability = build_mysql_metadata_capability_profile(dialect, type(self).__name__).get(domain)
        if table_name is None:
            rows = await driver.select(query.sql, schema_name=schema_name)
        else:
            rows = await driver.select(query.sql, schema_name=schema_name, table_name=table_name)
        return MetadataResult(domain, capability=capability, items=tuple(rows))
