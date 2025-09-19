"""SQLite-specific session store handler."""

from datetime import datetime, timezone
from typing import Any

from sqlspec import sql
from sqlspec.extensions.litestar.store import BaseStoreHandler

__all__ = ("StoreHandler",)


class StoreHandler(BaseStoreHandler):
    """SQLite-specific session store handler.

    SQLite stores JSON data as TEXT, so we need to serialize/deserialize JSON strings.
    Datetime values need to be stored as ISO format strings.
    """

    def format_datetime(self, dt: datetime) -> str:
        """Format datetime for SQLite storage as ISO string.

        Args:
            dt: Datetime to format

        Returns:
            ISO format datetime string
        """
        return dt.isoformat()

    def get_current_time(self) -> str:
        """Get current time as ISO string for SQLite.

        Returns:
            Current timestamp as ISO string
        """
        return datetime.now(timezone.utc).isoformat()

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
        """Build SQLite UPSERT SQL using ON CONFLICT.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: Serialized JSON string
            expires_at_value: ISO datetime string
            current_time_value: ISO datetime string

        Returns:
            Single UPSERT statement using SQLite ON CONFLICT
        """
        upsert_sql = (
            sql.insert(table_name)
            .columns(session_id_column, data_column, expires_at_column, created_at_column)
            .values(session_id, data_value, expires_at_value, current_time_value)
            .on_conflict(session_id_column)
            .do_update(
                **{
                    data_column: sql.raw("EXCLUDED." + data_column),
                    expires_at_column: sql.raw("EXCLUDED." + expires_at_column),
                }
            )
        )

        return [upsert_sql]
