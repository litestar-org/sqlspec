"""Teradata Vantage database data dictionary implementation."""

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
from sqlspec.data_dictionary.dialects.teradata import (
    TERADATA_CONFIG,
    TeradataVersionInfo,
    build_teradata_metadata_capability_profile,
    extract_teradata_version_value,
    list_teradata_available_features,
    merge_teradata_table_lists,
    resolve_teradata_feature_flag,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlspec.adapters.teradata.driver import TeradataDriver

__all__ = (
    "TERADATA_CONFIG",
    "TeradataDataDictionary",
    "TeradataSyncDataDictionary",
    "TeradataVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_teradata_version",
)

logger = get_logger("sqlspec.adapters.teradata.data_dictionary")
_UNICODE_CHAR_TYPE = 2


def parse_teradata_version(row: "dict[str, Any] | None") -> "TeradataVersionInfo | None":
    """Parse the Teradata version string returned by the version query."""
    value = extract_teradata_version_value(row)
    return TeradataVersionInfo.from_version_string(value) if value else None


def build_column_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ColumnMetadata]":
    """Convert Teradata DBC.ColumnsV query rows to column metadata."""
    return [
        ColumnMetadata(
            schema_name=str(r.get("schema_name", "")),
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            data_type=str(r.get("data_type") or "UNKNOWN"),
            is_nullable=r.get("is_nullable") in (True, 1, "YES", "yes"),
            column_default=str(r["column_default"]) if r.get("column_default") is not None else None,
            ordinal_position=int(r.get("ordinal_position", 1)),
            max_length=int(r.get("max_length", 0)) if r.get("max_length") is not None else 0,
            numeric_precision=int(r.get("numeric_precision", 0)) if r.get("numeric_precision") is not None else 0,
            numeric_scale=int(r.get("numeric_scale", 0)) if r.get("numeric_scale") is not None else 0,
            is_primary=bool(r.get("is_primary")),
            is_unique=bool(r.get("is_unique")),
            is_identity=bool(r.get("is_identity")),
            identity_generation=str(r["identity_generation"])
            if r.get("identity_generation")
            else ("ALWAYS" if r.get("is_identity") else None),
            is_generated=bool(r.get("is_generated")),
            generation_expression=str(r["generation_expression"])
            if r.get("generation_expression") is not None
            else None,
            is_hidden=bool(r.get("is_hidden")),
        )
        for r in rows
    ]


def build_index_metadata(rows: "Iterable[dict[str, Any]]") -> "list[IndexMetadata]":
    """Group per-column Teradata DBC.IndicesV rows into index metadata."""
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
            schema_name=idx["schema_name"],
            table_name=idx["table_name"],
            index_name=idx["index_name"],
            columns=idx["columns"],
            is_unique=idx["is_unique"],
            is_primary=idx["is_primary"],
        )
        for idx in indexes_map.values()
    ]


def build_foreign_key_metadata(rows: "Iterable[dict[str, Any]]") -> "list[ForeignKeyMetadata]":
    """Convert Teradata DBC.All_RI_ChildrenV query rows to foreign key metadata."""
    return [
        ForeignKeyMetadata(
            table_name=str(r.get("table_name", "")),
            column_name=str(r.get("column_name", "")),
            referenced_table=str(r.get("referenced_table", r.get("referenced_table_name", ""))),
            referenced_column=str(r.get("referenced_column", r.get("referenced_column_name", ""))),
            constraint_name=str(r.get("constraint_name", "")),
            schema=str(r["schema_name"]) if r.get("schema_name") else None,
            referenced_schema=str(r["referenced_schema"]) if r.get("referenced_schema") else None,
        )
        for r in rows
    ]


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class TeradataSyncDataDictionary(SyncDataDictionaryBase):
    """Synchronous Teradata Vantage data dictionary."""

    dialect: ClassVar[str] = "teradata"

    def get_metadata_capabilities(
        self, driver: "TeradataDriver", domains: "Sequence[str] | None" = None
    ) -> MetadataCapabilityProfile:
        """Return the Teradata metadata capability profile."""
        _ = driver
        return build_teradata_metadata_capability_profile(type(self).__name__, domains)

    def get_version(self, driver: "TeradataDriver") -> "TeradataVersionInfo | None":
        """Detect and cache the Teradata server version."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("TeradataVersionInfo | None", self._version_cache.get(driver_id))

        version_row = driver.select_one_or_none(self.get_query("version", "current"))
        version_info = parse_teradata_version(version_row)
        if version_info is None:
            self._log_version_unavailable(type(self).dialect, "missing")
        else:
            self._log_version_detected(type(self).dialect, version_info)
        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "TeradataDriver", feature: str) -> bool:
        """Check whether the connected Teradata server supports a feature."""
        version_info = self.get_version(driver)
        return resolve_teradata_feature_flag(feature, version_info)

    def get_optimal_type(self, driver: "TeradataDriver", type_category: str) -> str:
        """Return the optimal Teradata SQL type for a logical category."""
        _ = driver
        return TERADATA_CONFIG.get_optimal_type(type_category)

    def list_available_features(self) -> "list[str]":
        """List feature flags defined for the Teradata dialect."""
        return list_teradata_available_features()

    def get_tables(self, driver: "TeradataDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Return tables in topological dependency order for a Teradata schema."""
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
        return merge_teradata_table_lists(ordered_rows, all_rows)

    def get_columns(
        self, driver: "TeradataDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Return column metadata for a table or entire Teradata schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            rows = driver.select(self.get_query("columns", "by_schema"), schema_name=schema_name, table_name=None)
        else:
            table_name = self.resolve_identifier(table)
            self._log_table_describe(driver, schema_name=schema_name, table_name=table_name, operation="columns")
            rows = driver.select(self.get_query("columns", "by_table"), schema_name=schema_name, table_name=table_name)
        if table is not None and rows and any(row.get("data_type") in (None, "", "UNKNOWN") for row in rows):
            query = (
                "LOCKING ROW FOR ACCESS SELECT TRIM(ColumnName) AS column_name, "
                "TRIM(ColumnType) AS raw_type_code, TRIM(ColumnUDTName) AS column_udt_name, "
                "ColumnLength AS max_length, DecimalTotalDigits AS numeric_precision, "
                "DecimalFractionalDigits AS numeric_scale, Nullable AS is_nullable, "
                "CharType AS char_type FROM DBC.ColumnsQV "
                "WHERE UPPER(TRIM(DatabaseName)) = UPPER(TRIM(COALESCE(CAST(? AS VARCHAR(128)), DATABASE))) "
                "AND UPPER(TRIM(TableName)) = UPPER(TRIM(?)) "
                "ORDER BY ColumnId"
            )
            try:
                details = driver.select(query, schema_name, self.resolve_identifier(table))
            except SQLSpecError:
                details = []
            by_name = {str(row["column_name"]).strip().upper(): row for row in details}
            type_map = {
                "CV": "VARCHAR",
                "CF": "CHAR",
                "I": "INTEGER",
                "I8": "BIGINT",
                "I2": "SMALLINT",
                "I1": "BYTEINT",
                "D": "DECIMAL",
                "F": "DOUBLE PRECISION",
                "DA": "DATE",
                "TS": "TIMESTAMP",
                "SZ": "TIMESTAMP WITH TIME ZONE",
                "BV": "VARBYTE",
                "BF": "BYTE",
                "BO": "BLOB",
                "CO": "CLOB",
                "JN": "JSON",
                "AT": "TIME",
                "TZ": "TIME WITH TIME ZONE",
                "XM": "XML",
                "PD": "PERIOD(DATE)",
                "PS": "PERIOD(TIMESTAMP)",
                "PM": "PERIOD(TIMESTAMP WITH TIME ZONE)",
                "PT": "PERIOD(TIME)",
                "PZ": "PERIOD(TIME WITH TIME ZONE)",
                "N": "NUMBER",
            }
            enriched = []
            for row in rows:
                item = dict(row)
                detail = by_name.get(str(row["column_name"]).strip().upper())
                if detail and row.get("data_type") in (None, "", "UNKNOWN"):
                    item.update(detail)
                    code = str(detail.get("raw_type_code", "")).strip()
                    if code == "UT":
                        item["data_type"] = str(detail.get("column_udt_name") or "").strip() or "UDT"
                    else:
                        item["data_type"] = type_map.get(code, "UNKNOWN")
                    item["is_nullable"] = detail.get("is_nullable") in (True, 1, "Y", "YES")
                    if (
                        item["data_type"] in {"VARCHAR", "CHAR"}
                        and detail.get("char_type") == _UNICODE_CHAR_TYPE
                        and isinstance(detail.get("max_length"), int)
                    ):
                        item["max_length"] = detail["max_length"] // _UNICODE_CHAR_TYPE
                enriched.append(item)
            rows = enriched
        return build_column_metadata(rows)

    def get_indexes(
        self, driver: "TeradataDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Return index metadata for a table or entire Teradata schema."""
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
        self, driver: "TeradataDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Return foreign key metadata for a table or entire Teradata schema."""
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
        self, driver: "TeradataDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> MetadataResult:
        """Get Teradata constraint metadata."""
        schema_name = self.resolve_schema(schema)
        table_name = self.resolve_identifier(table) if table is not None else None
        query = self.get_query("constraints", "by_table" if table_name else "by_schema")
        rows = driver.select(query, schema_name=schema_name, table_name=table_name)
        return MetadataResult("constraints", items=tuple(rows))

    def get_views(self, driver: "TeradataDriver", schema: "str | None" = None) -> MetadataResult:
        """Get Teradata view metadata."""
        schema_name = self.resolve_schema(schema)
        query = self.get_query("views", "by_schema")
        rows = driver.select(query, schema_name=schema_name, view_name=None)
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            identity = (str(row.get("schema_name", "")), str(row.get("view_name", "")))
            grouped.setdefault(identity, []).append(row)
        views = []
        for chunks in grouped.values():
            chunks.sort(key=lambda row: int(row.get("line_no", 1)))
            view = dict(chunks[0])
            view["definition"] = "".join(str(row.get("definition") or "") for row in chunks)
            views.append(view)
        return MetadataResult("views", items=tuple(views))

    def get_objects(self, driver: "TeradataDriver", schema: "str | None" = None) -> MetadataResult:
        """Get tables, views, and procedures from the Teradata catalog."""
        rows = driver.select(
            self.get_query("objects", "by_schema"), schema_name=self.resolve_schema(schema), object_name=None
        )
        return MetadataResult("objects", items=tuple(rows))

    def get_schemas(self, driver: "TeradataDriver") -> MetadataResult:
        """Get Teradata schema metadata."""
        query = self.get_query("schemas", "by_schema")
        rows = driver.select(query, schema_name=None)
        return MetadataResult("schemas", items=tuple(rows))


TeradataDataDictionary = TeradataSyncDataDictionary
