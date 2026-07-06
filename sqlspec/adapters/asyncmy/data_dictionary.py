"""MySQL-specific data dictionary for metadata queries via asyncmy."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
)
from sqlspec.data_dictionary.dialects.mysql import (
    MySQLEngineVersion,
    build_mysql_metadata_capability_profile,
    build_mysql_show_create_statement,
    make_mysql_ddl_result,
    parse_mysql_engine_version,
    resolve_mysql_json_type,
)
from sqlspec.driver import AsyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

__all__ = ("AsyncmyDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AsyncmyDataDictionary(AsyncDataDictionaryBase):
    """MySQL-specific async data dictionary."""

    dialect: ClassVar[str] = "mysql"

    def __init__(self) -> None:
        super().__init__()

    async def get_version(self, driver: "AsyncmyDriver") -> "VersionInfo | None":
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

    async def get_feature_flag(self, driver: "AsyncmyDriver", feature: str) -> bool:
        """Check if MySQL database supports a specific feature."""
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_metadata_capabilities(
        self, driver: "AsyncmyDriver", domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Get replacement data-dictionary capability profile."""
        engine_version = await self._get_engine_version(driver)
        dialect = engine_version.engine_family if engine_version is not None else type(self).dialect
        requested_domains = None if domains is None else tuple(domains)
        return build_mysql_metadata_capability_profile(dialect, type(self).__name__, requested_domains)

    async def get_schemas(self, driver: "AsyncmyDriver") -> "MetadataResult":
        """Get schema metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        return await self._select_domain_result(driver, dialect, "schemas", "list", schema_name=None)

    async def get_objects(self, driver: "AsyncmyDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get object metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "objects", "by_schema", schema_name=schema_name)

    async def get_table_details(
        self, driver: "AsyncmyDriver", table: str, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get rich table metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table)
        return await self._select_domain_result(
            driver, dialect, "tables", "by_schema", schema_name=schema_name, table_name=table_name
        )

    async def get_constraints(
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get constraint metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        return await self._select_domain_result(
            driver, dialect, "constraints", "by_schema", schema_name=schema_name, table_name=table_name
        )

    async def get_views(self, driver: "AsyncmyDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "views", "by_schema", schema_name=schema_name)

    async def get_routines(self, driver: "AsyncmyDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get routine metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "routines", "by_schema", schema_name=schema_name)

    async def get_privileges(
        self, driver: "AsyncmyDriver", object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get privilege metadata from INFORMATION_SCHEMA."""
        dialect = await self._get_metadata_dialect(driver)
        schema_name = self._resolve_metadata_schema(schema)
        return await self._select_domain_result(driver, dialect, "privileges", "by_schema", schema_name=schema_name)

    async def get_ddl(self, driver: "AsyncmyDriver", object_name: str, schema: "str | None" = None) -> "MetadataResult":
        """Get native SHOW CREATE output and replay-sensitive context for a table."""
        schema_name = self._resolve_metadata_schema(schema)
        raw_version = await driver.select_value_or_none(self.get_query_text("version"))
        sql_mode = await driver.select_value_or_none("SELECT @@sql_mode")
        sql_quote_show_create = await driver.select_value_or_none("SELECT @@sql_quote_show_create")
        statement = build_mysql_show_create_statement(object_name, schema_name, "TABLE")
        row = await driver.select_one(statement, schema_type=dict)
        ddl = make_mysql_ddl_result(
            object_name,
            schema_name,
            "TABLE",
            row,
            str(raw_version) if raw_version is not None else None,
            str(sql_mode) if sql_mode is not None else None,
            str(sql_quote_show_create) if sql_quote_show_create is not None else None,
        )
        capability = build_mysql_metadata_capability_profile(
            ddl.identity.dialect or type(self).dialect, type(self).__name__
        ).get("ddl")
        return MetadataResult("ddl", capability=capability, items=(ddl,))

    async def get_system_metadata(
        self, driver: "AsyncmyDriver", domain: str, *, include_sensitive: bool = False
    ) -> "MetadataResult":
        """Get opt-in system metadata from performance_schema or sys."""
        dialect = await self._get_metadata_dialect(driver)
        capability = build_mysql_metadata_capability_profile(dialect, type(self).__name__).get("system")
        if not include_sensitive:
            return MetadataResult("system", capability=capability, warnings=capability.warnings)
        query_name = (
            "sys_schema_table_statistics" if domain == "sys_schema_table_statistics" else "performance_schema_tables"
        )
        query = get_data_dictionary_loader().get_domain_query(dialect, "system", query_name)
        if not query.is_supported or query.sql is None:
            return MetadataResult.unsupported("system")
        if query_name == "sys_schema_table_statistics":
            rows = await driver.select(query.sql, schema_name=None)
        else:
            rows = await driver.select(query.sql)
        return MetadataResult("system", capability=capability, items=tuple(rows))

    async def get_optimal_type(self, driver: "AsyncmyDriver", type_category: str) -> str:
        """Get optimal MySQL type for a category."""
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            return resolve_mysql_json_type(version_info)

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "AsyncmyDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using the MySQL catalog."""
        schema_name = self._resolve_metadata_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return await driver.select(
            self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata
        )

    async def get_columns(
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
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
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
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
        self, driver: "AsyncmyDriver", table: "str | None" = None, schema: "str | None" = None
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

    async def _get_engine_version(self, driver: "AsyncmyDriver") -> "MySQLEngineVersion | None":
        try:
            version_value = await driver.select_value_or_none(self.get_query_text("version"))
        except Exception:
            return None
        if version_value is None:
            return None
        return parse_mysql_engine_version(str(version_value))

    async def _get_metadata_dialect(self, driver: "AsyncmyDriver") -> str:
        engine_version = await self._get_engine_version(driver)
        return engine_version.engine_family if engine_version is not None else type(self).dialect

    async def _select_domain_result(
        self,
        driver: "AsyncmyDriver",
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
