"""Spanner metadata queries using INFORMATION_SCHEMA."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase

if TYPE_CHECKING:
    from sqlspec.driver import VersionInfo


__all__ = ("SpannerDataDictionary",)


class SpannerDataDictionary(SyncDataDictionaryBase):
    """Fetch table, column, and index metadata from Spanner."""

    def get_version(self, driver: "SyncDriverAdapterBase") -> "VersionInfo | None":
        _ = driver
        return None

    def get_feature_flag(self, driver: "SyncDriverAdapterBase", feature: str) -> bool:
        _ = driver, feature
        return False

    def get_optimal_type(self, driver: "SyncDriverAdapterBase", type_category: str) -> str:
        _ = driver, type_category
        return ""

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema"
        params: dict[str, Any]
        if schema is None:
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''"
            params = {}
        else:
            params = {"schema": schema}

        results = driver.select(sql, params)
        return [cast("str", row["TABLE_NAME"]) for row in results]

    def get_columns(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        sql = """
            SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @table
        """
        params: dict[str, Any] = {"table": table}
        if schema is not None:
            sql = f"{sql} AND TABLE_SCHEMA = @schema"
            params["schema"] = schema
        else:
            sql = f"{sql} AND TABLE_SCHEMA = ''"

        results = driver.select(sql, params)
        return [
            {
                "name": row["COLUMN_NAME"],
                "type": row["SPANNER_TYPE"],
                "nullable": row["IS_NULLABLE"] == "YES",
            }
            for row in results
        ]

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        sql = """
            SELECT INDEX_NAME, INDEX_TYPE, IS_UNIQUE
            FROM INFORMATION_SCHEMA.INDEXES
            WHERE TABLE_NAME = @table
        """
        params: dict[str, Any] = {"table": table}
        if schema is not None:
            sql = f"{sql} AND TABLE_SCHEMA = @schema"
            params["schema"] = schema
        else:
            sql = f"{sql} AND TABLE_SCHEMA = ''"

        results = driver.select(sql, params)
        return [
            {"name": row["INDEX_NAME"], "type": row["INDEX_TYPE"], "unique": row["IS_UNIQUE"]}
            for row in results
        ]

