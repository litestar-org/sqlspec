"""AsyncMy-specific session store handler."""

from typing import Any

from sqlspec import sql
from sqlspec.extensions.litestar.store import BaseStoreHandler

__all__ = ("StoreHandler",)


class StoreHandler(BaseStoreHandler):
    """AsyncMy-specific session store handler.

    MySQL stores JSON data as TEXT, so we need to serialize/deserialize JSON strings.
    Uses MySQL's ON DUPLICATE KEY UPDATE for upserts.
    """

    def build_upsert_sql(
        self,
        table_name: str,
        session_id_column: str,
        data_column: str,
        expires_at_column: str,
        created_at_column: str,
        session_id: str,
        data_value: Any,
        expires_at_value: Any,
        current_time_value: Any,
    ) -> "list[Any]":
        """Build MySQL UPSERT SQL using ON DUPLICATE KEY UPDATE.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: Serialized JSON string
            expires_at_value: Formatted datetime
            current_time_value: Formatted datetime

        Returns:
            Single UPSERT statement using MySQL ON DUPLICATE KEY UPDATE
        """
        upsert_sql = (
            sql.insert(table_name)
            .columns(session_id_column, data_column, expires_at_column, created_at_column)
            .values(session_id, data_value, expires_at_value, current_time_value)
            .on_duplicate_key_update(
                **{
                    data_column: sql.raw(f"VALUES({data_column})"),
                    expires_at_column: sql.raw(f"VALUES({expires_at_column})"),
                }
            )
        )

        return [upsert_sql]
