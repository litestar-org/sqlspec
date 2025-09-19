"""ADBC-specific session store handler."""

from typing import Any

from sqlspec import sql
from sqlspec.extensions.litestar.store import BaseStoreHandler

__all__ = ("StoreHandler",)


class StoreHandler(BaseStoreHandler):
    """ADBC-specific session store handler.

    ADBC (Arrow Database Connectivity) handles PostgreSQL connections
    but requires JSON strings for JSONB columns unlike native PostgreSQL adapters.
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
        """Build PostgreSQL UPSERT SQL with JSONB casting for ADBC.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: JSON string that needs JSONB casting
            expires_at_value: Formatted datetime
            current_time_value: Formatted datetime

        Returns:
            Single UPSERT statement with explicit JSONB casting
        """
        # ADBC requires explicit JSONB casting for PostgreSQL JSONB columns
        upsert_sql = sql.raw(
            f"INSERT INTO {table_name} ({session_id_column}, {data_column}, "
            f"{expires_at_column}, {created_at_column}) "
            f"VALUES (:session_id, :data_value::jsonb, :expires_at_value, :current_time_value) "
            f"ON CONFLICT ({session_id_column}) DO UPDATE SET "
            f"{data_column} = EXCLUDED.{data_column}, "
            f"{expires_at_column} = EXCLUDED.{expires_at_column}",
            session_id=session_id,
            data_value=data_value,
            expires_at_value=expires_at_value,
            current_time_value=current_time_value,
        )

        return [upsert_sql]
