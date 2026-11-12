"""Snowflake native catalog reflection over shared dialect query packs."""

from collections.abc import Sequence
from typing import Any, ClassVar, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
)
from sqlspec.data_dictionary.dialects.snowflake import (
    SNOWFLAKE_CONFIG,
    parse_snowflake_imported_keys,
    parse_snowflake_primary_or_unique_keys,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

__all__ = ("SnowflakeSyncDataDictionary",)

_SUPPORTED: Final[frozenset[str]] = frozenset({
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "views",
    "routines",
    "sequences",
    "ddl",
})
_DDL_TYPES: Final[dict[str, str]] = {
    "table": "TABLE",
    "base table": "TABLE",
    "view": "VIEW",
    "sequence": "SEQUENCE",
    "function": "FUNCTION",
    "procedure": "PROCEDURE",
    "schema": "SCHEMA",
}


def _format_snowflake_identifier(identifier: str) -> str:
    from sqlspec.adapters.snowflake.driver import _format_identifier

    return _format_identifier(identifier)


def _format_snowflake_show_scope(schema_name: str | None = None, table_name: str | None = None) -> str:
    if table_name is not None:
        qualified = _format_snowflake_identifier(table_name)
        if schema_name is not None and len(split_qualified_identifier(table_name)) == 1:
            qualified = f"{_format_snowflake_identifier(schema_name)}.{qualified}"
        return f"IN TABLE {qualified}"
    return f"IN SCHEMA {_format_snowflake_identifier(schema_name)}" if schema_name is not None else "IN SCHEMA"


def _capability(domain: str) -> MetadataCapability:
    if domain in _SUPPORTED:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
        )
    return MetadataCapability.unsupported(domain)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SnowflakeSyncDataDictionary(SyncDataDictionaryBase):
    """Reflect tables, typed columns, SHOW keys and native GET_DDL definitions."""

    __slots__ = ()
    dialect: ClassVar[str] = "snowflake"

    def get_metadata_capabilities(self, driver: Any, domains: Sequence[str] | None = None) -> MetadataCapabilityProfile:
        """Report implemented catalog domains and unsupported secondary indexes."""
        requested = (
            tuple(domains)
            if domains is not None
            else tuple(sorted(_SUPPORTED | {"indexes", "privileges", "dependencies"}))
        )
        return MetadataCapabilityProfile(
            self.dialect, adapter=type(self).__name__, capabilities=tuple(_capability(domain) for domain in requested)
        )

    def get_version(self, driver: Any) -> VersionInfo | None:
        """Read and cache CURRENT_VERSION for the connection."""
        key = id(driver)
        if key in self._version_fetch_attempted:
            return self._version_cache.get(key)
        value = driver.select_value_or_none(self.get_query("version", "current"))
        version = self.parse_version_with_pattern(SNOWFLAKE_CONFIG.version_pattern, str(value)) if value else None
        self.cache_version(key, version)
        return version

    def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Resolve dialect feature support against the server version."""
        return self.resolve_feature_flag(feature, self.get_version(driver))

    def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Choose the Snowflake SQL type for a logical category."""
        return SNOWFLAKE_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> list[str]:
        """List advertised Snowflake feature names."""
        return list(SNOWFLAKE_CONFIG.feature_flags)

    def _metadata(self, driver: Any, domain: str, schema: str | None = None) -> MetadataResult:
        operation = "list" if domain == "schemas" else "by_schema"
        parameters = {} if domain == "schemas" else {"schema_name": self.resolve_schema(schema)}
        rows = driver.select(self.get_query(domain, operation), **parameters)
        return MetadataResult(domain, capability=_capability(domain), items=tuple(rows))

    def get_schemas(self, driver: Any) -> MetadataResult:
        """List schemas in the active database."""
        return self._metadata(driver, "schemas")

    def get_objects(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List database objects in a schema."""
        return self._metadata(driver, "objects", schema)

    def get_tables(self, driver: Any, schema: str | None = None) -> list[TableMetadata]:
        """List tables from INFORMATION_SCHEMA.TABLES."""
        return cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=self.resolve_schema(schema),
                schema_type=TableMetadata,
            ),
        )

    def get_columns(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[ColumnMetadata]:
        """List columns, preserving exact quoted table and schema names."""
        params: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        operation = "by_schema"
        if table is not None:
            operation = "by_table"
            params["table_name"] = self.resolve_identifier(table)
        return cast(
            "list[ColumnMetadata]",
            driver.select(self.get_query("columns", operation), schema_type=ColumnMetadata, **params),
        )

    def get_indexes(self, driver: Any, table: str | None = None, schema: str | None = None) -> list[IndexMetadata]:
        """Return no secondary indexes for standard Snowflake tables."""
        return []

    def get_foreign_keys(
        self, driver: Any, table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Read ordered imported-key metadata without KEY_COLUMN_USAGE."""
        rows = driver.select(f"SHOW IMPORTED KEYS {_format_snowflake_show_scope(schema, table)}")
        return parse_snowflake_imported_keys(rows, table_name=self.resolve_identifier(table) if table else None)

    def get_constraints(self, driver: Any, table: str | None = None, schema: str | None = None) -> MetadataResult:
        """Combine catalog constraints with column-ordered SHOW key metadata."""
        params: dict[str, Any] = {"schema_name": self.resolve_schema(schema)}
        operation = "by_schema"
        if table is not None:
            params["table_name"] = self.resolve_identifier(table)
            operation = "by_table"
        rows = list(driver.select(self.get_query("constraints", operation), **params))
        scope = _format_snowflake_show_scope(schema, table)
        keys = parse_snowflake_primary_or_unique_keys(driver.select(f"SHOW PRIMARY KEYS {scope}"))
        keys += parse_snowflake_primary_or_unique_keys(driver.select(f"SHOW UNIQUE KEYS {scope}"), unique=True)
        keyed = {(str(row.get("constraint_name")), str(row.get("table_name"))) for row in keys}
        combined = [row for row in rows if (str(row.get("constraint_name")), str(row.get("table_name"))) not in keyed]
        combined.extend(keys)
        return MetadataResult("constraints", capability=_capability("constraints"), items=tuple(combined))

    def get_views(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List view definitions in a schema."""
        return self._metadata(driver, "views", schema)

    def get_sequences(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List sequence definitions in a schema."""
        return self._metadata(driver, "sequences", schema)

    def get_routines(self, driver: Any, schema: str | None = None) -> MetadataResult:
        """List functions and procedures in a schema."""
        return self._metadata(driver, "routines", schema)

    def _get_native_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: str | None,
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> DDLResult | None:
        kind = _DDL_TYPES.get(object_type.lower())
        if kind is None:
            return None
        name = quote_identifier(object_name)
        if schema is not None:
            name = f"{quote_identifier(schema)}.{name}"
        ddl = driver.select_value_or_none(self.get_query("ddl", "by_object"), object_type=kind, object_name=name)
        identity = ObjectIdentity(object_name, object_type, schema=schema, dialect=self.dialect)
        if not ddl:
            return DDLResult(identity, MetadataSupport.UNSUPPORTED, warnings=("GET_DDL returned no definition",))
        return DDLResult(
            identity,
            MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
            ddl=str(ddl),
        )
