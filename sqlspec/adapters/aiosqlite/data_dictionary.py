"""SQLite-specific data dictionary for metadata queries via aiosqlite."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.aiosqlite.core import format_identifier
from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataResult,
    TableMetadata,
    VersionInfo,
    get_data_dictionary_loader,
    get_dialect_config,
)
from sqlspec.data_dictionary.dialects.sqlite import list_sqlite_available_features, resolve_sqlite_json_type
from sqlspec.data_dictionary.dialects.sqlite.config import (
    sqlite_check_constraints,
    sqlite_constraint_metadata,
    sqlite_generated_expressions,
    sqlite_object_ddl_result,
    sqlite_schema_ddl_results,
)
from sqlspec.driver import AsyncDataDictionaryBase

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite.driver import AiosqliteDriver
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("AiosqliteDataDictionary",)

SQLITE_INDEX_EXPRESSION_COLUMN_ID = -2


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AiosqliteDataDictionary(AsyncDataDictionaryBase):
    """SQLite-specific async data dictionary."""

    dialect: ClassVar[str] = "sqlite"

    def __init__(self) -> None:
        super().__init__()

    def get_dialect_config(self) -> "DialectConfig":
        """Return the dialect configuration for this data dictionary."""
        return get_dialect_config(type(self).dialect)

    def resolve_schema(self, schema: "str | None") -> "str | None":
        """Return a schema name using dialect defaults when missing."""
        if schema is not None:
            return schema
        return self.get_dialect_config().default_schema

    def list_available_features(self) -> "list[str]":
        """List available feature flags for this dialect."""
        return list_sqlite_available_features()

    async def get_version(self, driver: "AiosqliteDriver") -> "VersionInfo | None":
        """Get SQLite database version information.

        Args:
            driver: Async database driver instance.

        Returns:
            SQLite version information or None if detection fails.
        """
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return self._version_cache.get(driver_id)

        version_value = await driver.select_value_or_none(self.get_query("version", "current"))
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

    async def get_feature_flag(self, driver: "AiosqliteDriver", feature: str) -> bool:
        """Check if SQLite database supports a specific feature.

        Args:
            driver: Async database driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.
        """
        version_info = await self.get_version(driver)
        return self.resolve_feature_flag(feature, version_info)

    async def get_optimal_type(self, driver: "AiosqliteDriver", type_category: str) -> str:
        """Get optimal SQLite type for a category.

        Args:
            driver: Async database driver instance.
            type_category: Type category.

        Returns:
            SQLite-specific type name.
        """
        config = self.get_dialect_config()
        version_info = await self.get_version(driver)

        if type_category == "json":
            return resolve_sqlite_json_type(version_info)

        return config.get_optimal_type(type_category)

    async def get_tables(self, driver: "AiosqliteDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using SQLite catalog."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        query_text = self._get_domain_query_text("tables", "by_schema")
        return await driver.select(query_text, schema_name=schema_name, schema_type=TableMetadata)

    async def get_columns(
        self, driver: "AiosqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema, including generated-column expressions."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            query_text = self._get_domain_query_text("columns", "by_schema").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name)
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
            query_text = self._get_domain_query_text("columns", "by_table").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, table_name=table, schema_name=schema_name)
        if any(str(row.get("extra") or "").endswith("generated") for row in rows):
            table_sql = await self._get_table_sql(driver, schema_name, table)
            for row in rows:
                if str(row.get("extra") or "").endswith("generated"):
                    expressions = sqlite_generated_expressions(table_sql.get(str(row.get("table_name"))))
                    expression = expressions.get(str(row.get("column_name")))
                    if expression is not None:
                        row["generation_expression"] = expression
        return cast("list[ColumnMetadata]", rows)

    async def get_indexes(
        self, driver: "AiosqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
            query_text = self._get_domain_query_text("indexes", "by_schema").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name)
            return _sqlite_index_rows_to_metadata(rows, schema_name)

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("indexes", "by_table").format(schema_prefix=schema_prefix)
        rows = await driver.select(query_text, table_name=table, schema_name=schema_name)
        return _sqlite_index_rows_to_metadata(rows, schema_name)

    async def get_foreign_keys(
        self, driver: "AiosqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata, including ON DELETE / ON UPDATE actions."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            rows = await driver.select(
                self._get_domain_query_text("foreign_keys", "by_schema"), schema_name=schema_name
            )
        else:
            self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
            rows = await driver.select(
                self._get_domain_query_text("foreign_keys", "by_table"), table_name=table, schema_name=schema_name
            )
        return [ForeignKeyMetadata.from_row(row) for row in rows]

    async def get_constraints(
        self, driver: "AiosqliteDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Get PRIMARY KEY, UNIQUE, and CHECK constraint metadata.

        PRIMARY KEY and UNIQUE come from index pragmas; CHECK constraints are parsed from the
        stored CREATE TABLE statement because SQLite exposes them nowhere else.
        """
        schema_name = self.resolve_schema(schema)
        if table is None:
            rows = await driver.select(
                self._get_domain_query_text("constraints", "by_schema"), schema_name=schema_name, table_name=None
            )
        else:
            rows = await driver.select(
                self._get_domain_query_text("constraints", "by_table"), schema_name=schema_name, table_name=table
            )
        items: list[dict[str, Any]] = sqlite_constraint_metadata(rows)
        warnings: list[str] = []
        for table_name, create_sql in (await self._get_table_sql(driver, schema_name, table)).items():
            checks, check_warnings = sqlite_check_constraints(table_name, create_sql, schema_name=schema_name)
            items.extend(checks)
            warnings.extend(check_warnings)
        return MetadataResult("constraints", items=tuple(items), warnings=tuple(warnings))

    async def get_views(self, driver: "AiosqliteDriver", schema: "str | None" = None) -> "MetadataResult":
        """Get view metadata including the stored CREATE VIEW statement."""
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("views", "by_schema").format(schema_prefix=schema_prefix)
        return MetadataResult("views", items=tuple(await driver.select(query_text, schema_name=schema_name)))

    async def get_sequences(self, driver: "AiosqliteDriver", schema: "str | None" = None) -> "MetadataResult":
        """Return an empty sequence result because SQLite has no sequence objects."""
        _ = driver, schema
        return MetadataResult("sequences", items=())

    async def get_types(self, driver: "AiosqliteDriver", schema: "str | None" = None) -> "MetadataResult":
        """Return an empty type result because SQLite has no user-defined types."""
        _ = driver, schema
        return MetadataResult("types", items=())

    async def _get_native_object_ddl(
        self,
        driver: "AiosqliteDriver",
        object_name: str,
        *,
        schema: "str | None",
        object_type: str,
        include_dependencies: bool,
        redact: bool,
    ) -> "DDLResult | None":
        """Return the stored SQL for an object from sqlite_schema."""
        _ = include_dependencies, redact
        schema_prefix = f"{format_identifier(schema)}." if schema else ""
        query_text = self._get_domain_query_text("ddl", "by_object").format(schema_prefix=schema_prefix)
        rows = await driver.select(query_text, schema_name=schema, object_name=object_name, object_type=object_type)
        return sqlite_object_ddl_result(rows, object_name=object_name, object_type=object_type, schema=schema)

    async def _get_native_schema_ddl(
        self, driver: "AiosqliteDriver", schema: "str | None", *, include_dependencies: bool, redact: bool
    ) -> "list[DDLResult] | None":
        """Return the stored SQL for every sqlite_schema object."""
        _ = include_dependencies, redact
        schema_name = self.resolve_schema(schema)
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        query_text = self._get_domain_query_text("ddl", "by_schema").format(schema_prefix=schema_prefix)
        return sqlite_schema_ddl_results(await driver.select(query_text, schema_name=schema_name), schema=schema_name)

    async def _get_table_sql(
        self, driver: "AiosqliteDriver", schema_name: "str | None", table: "str | None"
    ) -> "dict[str, str]":
        """Return stored CREATE TABLE statements keyed by table name for one table or the whole schema."""
        schema_prefix = f"{format_identifier(schema_name)}." if schema_name else ""
        if table is None:
            query_text = self._get_domain_query_text("ddl", "by_schema").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name)
        else:
            query_text = self._get_domain_query_text("ddl", "by_object").format(schema_prefix=schema_prefix)
            rows = await driver.select(query_text, schema_name=schema_name, object_name=table, object_type="table")
        return {
            str(row["object_name"]): str(row["ddl"])
            for row in rows
            if str(row.get("object_type") or "").lower() == "table" and row.get("ddl") is not None
        }

    def _get_domain_query_text(self, domain: str, query_name: str) -> str:
        """Return a required direct-domain query for SQLite."""
        query_text = get_data_dictionary_loader().get_domain_query_text(type(self).dialect, domain, query_name)
        if query_text is None:
            msg = f"Missing SQLite data-dictionary query: {domain}/{query_name}"
            raise RuntimeError(msg)
        return query_text


def _sqlite_index_rows_to_metadata(rows: "list[dict[str, Any]]", schema_name: "str | None") -> "list[IndexMetadata]":
    """Group SQLite index_xinfo rows into the public index metadata shape."""
    if not rows:
        return []
    first_row = rows[0]
    if "columns" in first_row:
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
        elif row.get("column_id") == SQLITE_INDEX_EXPRESSION_COLUMN_ID:
            columns.append("<expression>")

    return [cast("IndexMetadata", metadata) for metadata in grouped.values()]
