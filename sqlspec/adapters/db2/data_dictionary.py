"""IBM Db2 database data dictionary implementation."""

import logging
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    TableMetadata,
)
from sqlspec.data_dictionary.dialects.db2 import (
    DB2_CONFIG,
    DB2_VERSION_PATTERN,
    Db2VersionInfo,
    build_db2_metadata_capability_profile,
    extract_db2_version_value,
    list_db2_available_features,
    merge_db2_table_lists,
    resolve_db2_feature_flag,
)
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = ("DB2_CONFIG", "Db2AsyncDataDictionary", "Db2SyncDataDictionary", "Db2VersionInfo")

logger = get_logger("sqlspec.adapters.db2.data_dictionary")


def parse_db2_version(row: "dict[str, Any] | None") -> "Db2VersionInfo | None":
    """Parse the instance service level returned by the version query.

    Args:
        row: Version query row, or ``None`` when the query returned nothing.

    Returns:
        The parsed version, or ``None`` when the service level cannot be parsed.
    """
    service_level = extract_db2_version_value(row)
    match = DB2_VERSION_PATTERN.search(service_level) if service_level else None
    if match is None:
        log_with_context(logger, logging.DEBUG, "data_dictionary.version.unparsed", dialect="db2", value=service_level)
        return None
    return Db2VersionInfo(
        int(match.group(1)),
        int(match.group(2)) if match.group(2) else 0,
        int(match.group(3)) if match.group(3) else 0,
        service_level=service_level,
    )


def build_column_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ColumnMetadata]":
    """Convert SYSCAT.COLUMNS query rows to column metadata.

    Args:
        rows: Column query rows.

    Returns:
        Column metadata in row order.
    """
    return [
        ColumnMetadata(
            schema_name=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            data_type=str(r.get("data_type", "")),
            is_nullable=bool(r.get("is_nullable", True)),
            column_default=str(r["column_default"]) if r.get("column_default") is not None else None,
            ordinal_position=int(r.get("ordinal_position", 1)),
            max_length=int(r.get("max_length", 0)) if r.get("max_length") is not None else 0,
            numeric_scale=int(r.get("numeric_scale", 0)) if r.get("numeric_scale") is not None else 0,
            is_primary=bool(r.get("is_primary")),
            is_unique=bool(r.get("is_unique")),
            identity_generation=str(r["identity_generation"]) if r.get("identity_generation") else None,
            is_generated=bool(r.get("is_generated")),
        )
        for r in rows
    ]


def build_index_metadata(rows: "Iterable[dict[str, Any]]") -> "list[IndexMetadata]":
    """Group per-column SYSCAT.INDEXES / SYSCAT.INDEXCOLUSE rows into index metadata.

    Args:
        rows: Index column query rows, one per indexed column.

    Returns:
        One entry per index, in first-seen order, with columns in row order.
    """
    indexes_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r.get("schema_name", "")), str(r.get("table_name", "")), str(r.get("index_name", "")))
        if key not in indexes_map:
            indexes_map[key] = {
                "schema_name": key[0],
                "table_name": key[1],
                "index_name": key[2],
                "columns": [],
                "is_unique": bool(r.get("is_unique", 0)),
                "is_primary": bool(r.get("is_primary", 0)),
            }
        col_name = str(r.get("column_name", ""))
        if col_name:
            indexes_map[key]["columns"].append(col_name)

    return [
        IndexMetadata(
            schema_name=v["schema_name"],
            table_name=v["table_name"],
            index_name=v["index_name"],
            columns=v["columns"],
            is_unique=v["is_unique"],
            is_primary=v["is_primary"],
        )
        for v in indexes_map.values()
    ]


def build_foreign_key_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ForeignKeyMetadata]":
    """Convert SYSCAT.REFERENCES / SYSCAT.KEYCOLUSE query rows to foreign key metadata.

    Args:
        rows: Foreign key query rows, one per referencing column.

    Returns:
        Foreign key metadata in row order.
    """
    return [
        ForeignKeyMetadata(
            schema=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            constraint_name=str(r.get("constraint_name", "")),
            column_name=str(r.get("column_name", "")),
            referenced_schema=str(r.get("referenced_schema", "")),
            referenced_table=str(r.get("referenced_table", "")),
            referenced_column=str(r.get("referenced_column", "")),
        )
        for r in rows
    ]


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class Db2SyncDataDictionary(SyncDataDictionaryBase):
    """IBM Db2 sync data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "db2"

    def __init__(self) -> None:
        """Initialize Db2 sync data dictionary."""
        super().__init__()

    def get_metadata_capabilities(
        self, driver: "Db2SyncDriver", domains: "Sequence[str] | None" = None
    ) -> MetadataCapabilityProfile:
        """Get Db2 data-dictionary capability profile."""
        _ = driver
        return build_db2_metadata_capability_profile(type(self).__name__, domains)

    def get_version(self, driver: "Db2SyncDriver") -> Db2VersionInfo | None:
        """Get Db2 database version information.

        The instance service level is parsed once per driver and cached. Query errors propagate
        as mapped driver errors.

        Args:
            driver: Db2 driver.

        Returns:
            The parsed version, or ``None`` when the service level cannot be parsed.
        """
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("Db2VersionInfo | None", self._version_cache.get(driver_id))

        version_info = parse_db2_version(driver.select_one_or_none(self.get_query("version", "current")))
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "Db2SyncDriver", feature: str) -> bool:
        """Check whether Db2 supports a feature."""
        version_info = self.get_version(driver)
        return resolve_db2_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "Db2SyncDriver", type_category: str) -> str:
        """Get optimal Db2 type for a category."""
        _ = driver
        return DB2_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> list[str]:
        """List available feature flags for this dialect."""
        return list_db2_available_features()

    def get_tables(self, driver: "Db2SyncDriver", schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")

        ordered_rows = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        all_rows = cast(
            "list[TableMetadata]",
            driver.select(
                self.get_query("tables", "all_by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        return merge_db2_table_lists(ordered_rows, all_rows)

    def get_columns(
        self, driver: "Db2SyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get columns for a table or schema from SYSCAT.COLUMNS."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            rows = driver.select(self.get_query("columns", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
            rows = driver.select(self.get_query("columns", "by_table"), schema_name=schema_name, table_name=table_name)

        return build_column_metadata(rows)

    def get_indexes(
        self, driver: "Db2SyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get indexes for a table or schema from SYSCAT.INDEXES and SYSCAT.INDEXCOLUSE."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = driver.select(self.get_query("indexes", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
            rows = driver.select(self.get_query("indexes", "by_table"), schema_name=schema_name, table_name=table_name)

        return build_index_metadata(rows)

    def get_foreign_keys(
        self, driver: "Db2SyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign keys from SYSCAT.REFERENCES and SYSCAT.KEYCOLUSE."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = driver.select(self.get_query("foreign_keys", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
            rows = driver.select(
                self.get_query("foreign_keys", "by_table"), schema_name=schema_name, table_name=table_name
            )

        return build_foreign_key_metadata(rows)

    def get_constraints(
        self, driver: "Db2SyncDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get Db2 constraint metadata."""
        schema_name = self.resolve_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        query = self.get_query("constraints", "by_table" if table_name else "by_schema")
        rows = driver.select(query, schema_name=schema_name, table_name=table_name)
        return MetadataResult("constraints", items=tuple(rows))

    def get_views(self, driver: "Db2SyncDriver", schema: str | None = None) -> MetadataResult:
        """Get Db2 view metadata."""
        schema_name = self.resolve_schema(schema)
        query = self.get_query("views", "by_schema")
        rows = driver.select(query, schema_name=schema_name, view_name=None)
        return MetadataResult("views", items=tuple(rows))

    def get_objects(self, driver: "Db2SyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get tables, views, aliases, sequences and routines from the Db2 catalog.

        Args:
            driver: Db2 driver.
            schema: Schema to list; defaults to the session's ``CURRENT SCHEMA``.

        Returns:
            Objects-domain metadata result.
        """
        rows = driver.select(
            self.get_query("objects", "by_schema"), schema_name=self.resolve_schema(schema), object_name=None
        )
        return MetadataResult("objects", items=tuple(rows))

    def get_schemas(self, driver: "Db2SyncDriver") -> MetadataResult:
        """Get Db2 schema metadata."""
        query = self.get_query("schemas", "by_schema")
        rows = driver.select(query, schema_name=None)
        return MetadataResult("schemas", items=tuple(rows))


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class Db2AsyncDataDictionary(AsyncDataDictionaryBase):
    """IBM Db2 async data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "db2"

    def __init__(self) -> None:
        """Initialize Db2 async data dictionary."""
        super().__init__()

    async def get_metadata_capabilities(
        self, driver: "Db2AsyncDriver", domains: "Sequence[str] | None" = None
    ) -> MetadataCapabilityProfile:
        """Get Db2 data-dictionary capability profile."""
        _ = driver
        return build_db2_metadata_capability_profile(type(self).__name__, domains)

    async def get_version(self, driver: "Db2AsyncDriver") -> Db2VersionInfo | None:
        """Get Db2 database version information.

        The instance service level is parsed once per driver and cached. Query errors propagate
        as mapped driver errors.

        Args:
            driver: Async Db2 driver.

        Returns:
            The parsed version, or ``None`` when the service level cannot be parsed.
        """
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("Db2VersionInfo | None", self._version_cache.get(driver_id))

        version_info = parse_db2_version(await driver.select_one_or_none(self.get_query("version", "current")))
        self.cache_version(driver_id, version_info)
        return version_info

    async def get_feature_flag(self, driver: "Db2AsyncDriver", feature: str) -> bool:
        """Check whether Db2 supports a feature."""
        version_info = await self.get_version(driver)
        return resolve_db2_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "Db2AsyncDriver", type_category: str) -> str:
        """Get optimal Db2 type for a category."""
        _ = driver
        return DB2_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> list[str]:
        """List available feature flags for this dialect."""
        return list_db2_available_features()

    async def get_tables(self, driver: "Db2AsyncDriver", schema: str | None = None) -> list[TableMetadata]:
        """Get tables sorted by dependency order with catalog fallback."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")

        ordered_rows = cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("tables", "by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        all_rows = cast(
            "list[TableMetadata]",
            await driver.select(
                self.get_query("tables", "all_by_schema"),
                schema_name=schema_name,
                table_name=None,
                schema_type=TableMetadata,
            ),
        )
        return merge_db2_table_lists(ordered_rows, all_rows)

    async def get_columns(
        self, driver: "Db2AsyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get columns for a table or schema from SYSCAT.COLUMNS."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            rows = await driver.select(self.get_query("columns", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
            rows = await driver.select(
                self.get_query("columns", "by_table"), schema_name=schema_name, table_name=table_name
            )

        return build_column_metadata(rows)

    async def get_indexes(
        self, driver: "Db2AsyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get indexes for a table or schema from SYSCAT.INDEXES and SYSCAT.INDEXCOLUSE."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            rows = await driver.select(self.get_query("indexes", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="indexes")
            rows = await driver.select(
                self.get_query("indexes", "by_table"), schema_name=schema_name, table_name=table_name
            )

        return build_index_metadata(rows)

    async def get_foreign_keys(
        self, driver: "Db2AsyncDriver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign keys from SYSCAT.REFERENCES and SYSCAT.KEYCOLUSE."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = await driver.select(
                self.get_query("foreign_keys", "by_schema"), schema_name=schema_name, table_name=None
            )
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="foreign_keys")
            rows = await driver.select(
                self.get_query("foreign_keys", "by_table"), schema_name=schema_name, table_name=table_name
            )

        return build_foreign_key_metadata(rows)

    async def get_constraints(
        self, driver: "Db2AsyncDriver", table: str | None = None, schema: str | None = None
    ) -> MetadataResult:
        """Get Db2 constraint metadata."""
        schema_name = self.resolve_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        query = self.get_query("constraints", "by_table" if table_name else "by_schema")
        rows = await driver.select(query, schema_name=schema_name, table_name=table_name)
        return MetadataResult("constraints", items=tuple(rows))

    async def get_views(self, driver: "Db2AsyncDriver", schema: str | None = None) -> MetadataResult:
        """Get Db2 view metadata."""
        schema_name = self.resolve_schema(schema)
        query = self.get_query("views", "by_schema")
        rows = await driver.select(query, schema_name=schema_name, view_name=None)
        return MetadataResult("views", items=tuple(rows))

    async def get_objects(self, driver: "Db2AsyncDriver", schema: "str | None" = None) -> MetadataResult:
        """Get tables, views, aliases, sequences and routines from the Db2 catalog.

        Args:
            driver: Async Db2 driver.
            schema: Schema to list; defaults to the session's ``CURRENT SCHEMA``.

        Returns:
            Objects-domain metadata result.
        """
        rows = await driver.select(
            self.get_query("objects", "by_schema"), schema_name=self.resolve_schema(schema), object_name=None
        )
        return MetadataResult("objects", items=tuple(rows))

    async def get_schemas(self, driver: "Db2AsyncDriver") -> MetadataResult:
        """Get Db2 schema metadata."""
        query = self.get_query("schemas", "by_schema")
        rows = await driver.select(query, schema_name=None)
        return MetadataResult("schemas", items=tuple(rows))
