"""Spanner metadata queries using INFORMATION_SCHEMA."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase

if TYPE_CHECKING:
    from sqlspec.driver import VersionInfo


__all__ = ("SpannerDataDictionary",)


class SpannerDataDictionary(SyncDataDictionaryBase):
    """Fetch table, column, and index metadata from Spanner."""

    def get_version(self, driver: "SyncDriverAdapterBase") -> "VersionInfo | None":
        """Get Spanner version information.

        Spanner is a fully managed cloud service that doesn't expose
        traditional version information. Feature availability is determined
        by the service itself, not version numbers.

        Args:
            driver: Spanner driver instance

        Returns:
            None - Spanner doesn't provide version information.
        """
        _ = driver
        return None

    def get_feature_flag(self, driver: "SyncDriverAdapterBase", feature: str) -> bool:
        _ = driver
        feature_checks = {
            "supports_json": True,
            "supports_generators": False,
            "supports_index_clustering": True,
            "supports_interleaved_tables": True,
        }
        return feature_checks.get(feature, False)

    def get_optimal_type(self, driver: "SyncDriverAdapterBase", type_category: str) -> str:
        _ = driver
        type_map = {
            "json": "JSON",
            "uuid": "BYTES(16)",
            "boolean": "BOOL",
            "timestamp": "TIMESTAMP",
            "text": "STRING(MAX)",
            "blob": "BYTES(MAX)",
            "numeric": "NUMERIC",
            "bignumeric": "NUMERIC",
            "array": "ARRAY",
        }
        return type_map.get(type_category, "STRING(MAX)")

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        sql = 'SELECT TABLE_NAME AS "table_name" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema'
        params: dict[str, Any]
        if schema is None:
            sql = "SELECT TABLE_NAME AS \"table_name\" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''"
            params = {}
        else:
            params = {"schema": schema}

        results = driver.select(sql, params)
        return [cast("str", row["table_name"]) for row in results]

    def get_columns(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        sql = """
            SELECT COLUMN_NAME AS "column_name", SPANNER_TYPE AS "spanner_type", IS_NULLABLE AS "is_nullable"
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
            {"name": row["column_name"], "type": row["spanner_type"], "nullable": row["is_nullable"] == "YES"}
            for row in results
        ]

    def get_indexes(
        self, driver: "SyncDriverAdapterBase", table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        sql = """
            SELECT INDEX_NAME AS "index_name", INDEX_TYPE AS "index_type", IS_UNIQUE AS "is_unique"
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
        return [{"name": row["index_name"], "type": row["index_type"], "unique": row["is_unique"]} for row in results]
