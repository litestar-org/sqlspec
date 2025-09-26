"""AsyncPG-specific session store handler.

Standalone handler with no inheritance - clean break implementation.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from sqlspec.driver._async import AsyncDriverAdapterBase
    from sqlspec.driver._sync import SyncDriverAdapterBase

from sqlspec import sql

__all__ = ("AsyncStoreHandler",)


class AsyncStoreHandler:
    """AsyncPG-specific session store handler.

    AsyncPG handles JSONB columns natively with Python dictionaries,
    so no JSON serialization/deserialization is needed.
    """

    def __init__(
        self, table_name: str = "litestar_sessions", data_column: str = "data", *args: Any, **kwargs: Any
    ) -> None:
        """Initialize AsyncPG store handler.

        Args:
            table_name: Name of the session table
            data_column: Name of the data column
            *args: Additional positional arguments (ignored)
            **kwargs: Additional keyword arguments (ignored)
        """

    def serialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Serialize session data for AsyncPG JSONB storage.

        Args:
            data: Session data to serialize
            driver: Database driver instance (unused, AsyncPG handles JSONB natively)

        Returns:
            Raw Python data (AsyncPG handles JSONB natively)
        """
        return data

    def deserialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Deserialize session data from AsyncPG JSONB storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (unused, AsyncPG returns JSONB as Python objects)

        Returns:
            Raw Python data (AsyncPG returns JSONB as Python objects)
        """
        return data

    def format_datetime(self, dt: datetime) -> Union[str, datetime, Any]:
        """Format datetime for database storage.

        Args:
            dt: Datetime to format

        Returns:
            Formatted datetime value
        """
        return dt

    def get_current_time(self) -> Union[str, datetime, Any]:
        """Get current time in database-appropriate format.

        Returns:
            Current timestamp for database queries
        """
        return datetime.now(timezone.utc)

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
        driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None,
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
            driver: Database driver instance (unused)

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
                    data_column: sql.raw(f"EXCLUDED.{data_column}"),
                    expires_at_column: sql.raw(f"EXCLUDED.{expires_at_column}"),
                }
            )
        )

        return [upsert_sql]

    def handle_column_casing(self, row: "dict[str, Any]", column: str) -> Any:
        """Handle database-specific column name casing.

        Args:
            row: Result row from database
            column: Column name to look up

        Returns:
            Column value handling different name casing
        """
        if column in row:
            return row[column]
        if column.upper() in row:
            return row[column.upper()]
        if column.lower() in row:
            return row[column.lower()]
        msg = f"Column {column} not found in result row"
        raise KeyError(msg)
