"""PSQLPy-specific session store handler."""

from typing import Any

from sqlspec import sql
from sqlspec.extensions.litestar.store import BaseStoreHandler

__all__ = ("StoreHandler",)


class StoreHandler(BaseStoreHandler):
    """PSQLPy-specific session store handler.

    PSQLPy expects native Python objects (dict/list) for JSONB columns.
    The driver handles the PyJSONB wrapping for complex data.
    """

    def serialize_data(self, data: Any) -> Any:
        """Serialize session data for PSQLPy JSONB storage.

        Args:
            data: Session data to serialize

        Returns:
            Raw Python data (driver handles PyJSONB wrapping)
        """
        return data

    def deserialize_data(self, data: Any) -> Any:
        """Deserialize session data from PSQLPy JSONB storage.

        Args:
            data: Raw data from database

        Returns:
            Raw Python data (PSQLPy returns JSONB as Python objects)
        """
        return data

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
        """Build PostgreSQL UPSERT SQL using ON CONFLICT.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: Session data (Python object for JSONB)
            expires_at_value: Formatted expiration time
            current_time_value: Formatted current time

        Returns:
            Single UPSERT statement using PostgreSQL ON CONFLICT
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
