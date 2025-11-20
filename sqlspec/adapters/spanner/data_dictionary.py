from typing import Any

from sqlspec.driver import SyncDataDictionaryBase


class SpannerDataDictionary(SyncDataDictionaryBase):
    """Query Spanner INFORMATION_SCHEMA for metadata."""

    def get_tables(self, schema: str | None = None) -> list[str]:
        """Query INFORMATION_SCHEMA.TABLES."""
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema"
        schema_val = schema or ""  # Spanner usually uses empty string or specific schema
        # If schema is None, maybe we want all user tables?
        # Spanner uses 'public' or empty string mostly.
        # Let's assume empty string is default schema if not provided.

        if schema is None:
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''"
            params = {}
        else:
            params = {"schema": schema}

        results = self.driver.select(sql, params)
        return [row["TABLE_NAME"] for row in results]  # type: ignore

    def get_columns(self, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Query INFORMATION_SCHEMA.COLUMNS."""
        sql = """
            SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @table
        """
        params: dict[str, Any] = {"table": table}
        if schema is not None:
            sql += " AND TABLE_SCHEMA = @schema"
            params["schema"] = schema
        else:
            sql += " AND TABLE_SCHEMA = ''"

        results = self.driver.select(sql, params)
        return [
            {"name": row["COLUMN_NAME"], "type": row["SPANNER_TYPE"], "nullable": row["IS_NULLABLE"] == "YES"}
            for row in results  # type: ignore
        ]

    def get_indexes(self, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Query INFORMATION_SCHEMA.INDEXES."""
        sql = """
            SELECT INDEX_NAME, INDEX_TYPE, IS_UNIQUE
            FROM INFORMATION_SCHEMA.INDEXES
            WHERE TABLE_NAME = @table
        """
        params: dict[str, Any] = {"table": table}
        if schema is not None:
            sql += " AND TABLE_SCHEMA = @schema"
            params["schema"] = schema
        else:
            sql += " AND TABLE_SCHEMA = ''"

        results = self.driver.select(sql, params)
        return [
            {"name": row["INDEX_NAME"], "type": row["INDEX_TYPE"], "unique": row["IS_UNIQUE"]}
            for row in results  # type: ignore
        ]
