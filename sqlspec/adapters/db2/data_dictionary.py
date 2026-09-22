"""IBM Db2 database data dictionary implementation."""

import re
from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    VersionInfo,
    register_dialect,
)
from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.db2.driver import Db2Driver

__all__ = (
    "DB2_CONFIG",
    "Db2SyncDataDictionary",
    "Db2VersionInfo",
)

logger = get_logger("sqlspec.adapters.db2.data_dictionary")

DB2_VERSION_PATTERN = re.compile(r"(?:v|V)?(\d+)\.(\d+)(?:\.(\d+))?")

DB2_FEATURE_VERSIONS: FeatureVersions = {}

DB2_FEATURE_FLAGS: FeatureFlags = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_in_memory": False,
    "supports_for_update": True,
    "supports_skip_locked": True,
    "supports_on_conflict": False,
    "supports_update_from": False,
}

DB2_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "VARCHAR(36)",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "text": "CLOB",
    "blob": "BLOB",
    "json": "CLOB",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE",
    "decimal": "DECFLOAT",
    "varchar": "VARCHAR(255)",
}

DB2_CONFIG = DialectConfig(
    name="db2",
    feature_versions=DB2_FEATURE_VERSIONS,
    feature_flags=DB2_FEATURE_FLAGS,
    type_mappings=DB2_TYPE_MAPPINGS,
    version_pattern=DB2_VERSION_PATTERN,
)

register_dialect(DB2_CONFIG)


class Db2VersionInfo(VersionInfo):
    """Db2 database version info."""

    __slots__ = ("service_level",)

    def __init__(
        self,
        major: int,
        minor: int = 0,
        patch: int = 0,
        service_level: str | None = None,
    ) -> None:
        """Initialize Db2 version information."""
        super().__init__(major, minor, patch)
        self.service_level = service_level


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class Db2SyncDataDictionary(SyncDataDictionaryBase):
    """IBM Db2 sync data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "db2"

    def __init__(self) -> None:
        """Initialize Db2 sync data dictionary."""
        super().__init__()

    def get_version(self, driver: "Db2Driver") -> Db2VersionInfo | None:
        """Get Db2 database version information."""
        driver_id = id(driver)
        if driver_id in self._version_fetch_attempted:
            return cast("Db2VersionInfo | None", self._version_cache.get(driver_id))

        version_info: Db2VersionInfo | None = None
        try:
            sql = "SELECT SERVICE_LEVEL FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS T"
            row = driver.select_one_or_none(sql)
            if row and "SERVICE_LEVEL" in row:
                lvl = str(row["SERVICE_LEVEL"])
                match = DB2_VERSION_PATTERN.search(lvl)
                if match:
                    major = int(match.group(1))
                    minor = int(match.group(2)) if match.group(2) else 0
                    patch = int(match.group(3)) if match.group(3) else 0
                    version_info = Db2VersionInfo(major, minor, patch, service_level=lvl)
        except Exception:
            version_info = Db2VersionInfo(11, 5, 0)

        if version_info is None:
            version_info = Db2VersionInfo(11, 5, 0)

        self.cache_version(driver_id, version_info)
        return version_info

    def get_feature_flag(self, driver: "Db2Driver", feature: str) -> bool:
        """Check whether Db2 supports a feature."""
        flag = DB2_CONFIG.get_feature_flag(feature)
        return bool(flag) if flag is not None else False

    def get_optimal_type(self, driver: "Db2Driver", type_category: str) -> str:
        """Get optimal Db2 type for a category."""
        return DB2_CONFIG.get_optimal_type(type_category)

    def get_tables(self, driver: "Db2Driver", schema: str | None = None) -> list[TableMetadata]:
        """Get tables in schema from SYSCAT.TABLES."""
        sql = (
            "SELECT "
            "RTRIM(TABSCHEMA) AS schema_name, "
            "RTRIM(TABNAME) AS table_name, "
            "CASE TYPE "
            "  WHEN 'T' THEN 'BASE TABLE' "
            "  WHEN 'V' THEN 'VIEW' "
            "  WHEN 'A' THEN 'ALIAS' "
            "  WHEN 'N' THEN 'NICKNAME' "
            "  ELSE 'TABLE' "
            "END AS table_type "
            "FROM SYSCAT.TABLES "
            "WHERE TABSCHEMA NOT LIKE 'SYS%' "
        )
        params: list[Any] = []
        if schema:
            sql += "AND TABSCHEMA = ? "
            params.append(schema.upper())
        sql += "ORDER BY TABSCHEMA, TABNAME"

        rows = driver.select(sql, *params)
        return [
            TableMetadata(
                schema_name=str(r.get("schema_name", "")),
                table_name=str(r.get("table_name", "")),
                table_type=str(r.get("table_type", "BASE TABLE")),
            )
            for r in rows
        ]

    def get_columns(
        self, driver: "Db2Driver", table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        """Get columns for a table or schema from SYSCAT.COLUMNS."""
        sql = (
            "SELECT "
            "RTRIM(TABSCHEMA) AS schema_name, "
            "RTRIM(TABNAME) AS table_name, "
            "RTRIM(COLNAME) AS column_name, "
            "RTRIM(TYPENAME) AS data_type, "
            "CASE NULLS WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable, "
            "DEFAULT AS column_default, "
            "COLNO + 1 AS ordinal_position, "
            "LENGTH AS max_length, "
            "SCALE AS numeric_scale, "
            "KEYSEQ AS is_primary, "
            "IDENTITY AS identity_generation, "
            "GENERATED AS is_generated "
            "FROM SYSCAT.COLUMNS "
            "WHERE TABSCHEMA NOT LIKE 'SYS%' "
        )
        params: list[Any] = []
        if schema:
            sql += "AND TABSCHEMA = ? "
            params.append(schema.upper())
        if table:
            sql += "AND TABNAME = ? "
            params.append(table.upper())
        sql += "ORDER BY TABSCHEMA, TABNAME, COLNO"

        rows = driver.select(sql, *params)
        columns: list[ColumnMetadata] = []
        for r in rows:
            keyseq = r.get("is_primary")
            is_pk = bool(keyseq is not None and keyseq not in {0, "0"})
            columns.append(
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
                    is_primary=is_pk,
                    is_unique=is_pk,
                    identity_generation=str(r["identity_generation"]) if r.get("identity_generation") else None,
                    is_generated=bool(r.get("is_generated") == "A" or r.get("is_generated") == "D"),
                )
            )
        return columns

    def get_indexes(
        self, driver: "Db2Driver", table: str | None = None, schema: str | None = None
    ) -> list[IndexMetadata]:
        """Get indexes for a table or schema from SYSCAT.INDEXES and SYSCAT.INDEXCOLUSE."""
        sql = (
            "SELECT "
            "RTRIM(i.TABSCHEMA) AS schema_name, "
            "RTRIM(i.TABNAME) AS table_name, "
            "RTRIM(i.INDNAME) AS index_name, "
            "RTRIM(c.COLNAME) AS column_name, "
            "c.COLSEQ AS column_position, "
            "CASE i.UNIQUERULE WHEN 'D' THEN 0 ELSE 1 END AS is_unique, "
            "CASE i.UNIQUERULE WHEN 'P' THEN 1 ELSE 0 END AS is_primary "
            "FROM SYSCAT.INDEXES i "
            "JOIN SYSCAT.INDEXCOLUSE c "
            "  ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME "
            "WHERE i.TABSCHEMA NOT LIKE 'SYS%' "
        )
        params: list[Any] = []
        if schema:
            sql += "AND i.TABSCHEMA = ? "
            params.append(schema.upper())
        if table:
            sql += "AND i.TABNAME = ? "
            params.append(table.upper())
        sql += "ORDER BY i.TABSCHEMA, i.TABNAME, i.INDNAME, c.COLSEQ"

        rows = driver.select(sql, *params)
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

    def get_foreign_keys(
        self, driver: "Db2Driver", table: str | None = None, schema: str | None = None
    ) -> list[ForeignKeyMetadata]:
        """Get foreign keys from SYSCAT.REFERENCES and SYSCAT.KEYCOLUSE."""
        sql = (
            "SELECT "
            "RTRIM(r.TABSCHEMA) AS schema_name, "
            "RTRIM(r.TABNAME) AS table_name, "
            "RTRIM(r.CONSTNAME) AS constraint_name, "
            "RTRIM(k.COLNAME) AS column_name, "
            "RTRIM(r.REFTABSCHEMA) AS referenced_schema, "
            "RTRIM(r.REFTABNAME) AS referenced_table, "
            "RTRIM(refk.COLNAME) AS referenced_column "
            "FROM SYSCAT.REFERENCES r "
            "JOIN SYSCAT.KEYCOLUSE k "
            "  ON r.TABSCHEMA = k.TABSCHEMA AND r.CONSTNAME = k.CONSTNAME "
            "JOIN SYSCAT.KEYCOLUSE refk "
            "  ON r.REFTABSCHEMA = refk.TABSCHEMA AND r.REFKEYNAME = refk.CONSTNAME AND k.COLSEQ = refk.COLSEQ "
            "WHERE r.TABSCHEMA NOT LIKE 'SYS%' "
        )
        params: list[Any] = []
        if schema:
            sql += "AND r.TABSCHEMA = ? "
            params.append(schema.upper())
        if table:
            sql += "AND r.TABNAME = ? "
            params.append(table.upper())
        sql += "ORDER BY r.TABSCHEMA, r.TABNAME, r.CONSTNAME, k.COLSEQ"

        rows = driver.select(sql, *params)
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
