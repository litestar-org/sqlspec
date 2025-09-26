"""BigQuery-specific session store handler.

Standalone handler with no inheritance - clean break implementation.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from sqlspec.driver._async import AsyncDriverAdapterBase
    from sqlspec.driver._sync import SyncDriverAdapterBase

from sqlspec import sql
from sqlspec.utils.serializers import from_json, to_json

__all__ = ("SyncStoreHandler",)


class SyncStoreHandler:
    """BigQuery-specific session store handler.

    BigQuery has native JSON support but uses standard SQL features.
    Uses check-update-insert pattern since BigQuery doesn't support UPSERT syntax.
    """

    def __init__(
        self, table_name: str = "litestar_sessions", data_column: str = "data", *args: Any, **kwargs: Any
    ) -> None:
        """Initialize BigQuery store handler.

        Args:
            table_name: Name of the session table
            data_column: Name of the data column
            *args: Additional positional arguments (ignored)
            **kwargs: Additional keyword arguments (ignored)
        """

    def serialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Serialize session data for BigQuery JSON storage.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional)

        Returns:
            JSON string for BigQuery JSON columns
        """
        return to_json(data)

    def deserialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Deserialize session data from BigQuery storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (optional)

        Returns:
            Deserialized session data, or None if JSON is invalid
        """
        if isinstance(data, str):
            try:
                return from_json(data)
            except Exception:
                return None
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
        """Build SQL statements for upserting session data.

        BigQuery doesn't support UPSERT syntax, so use check-update-insert pattern.

        Args:
            table_name: Name of session table
            session_id_column: Session ID column name
            data_column: Data column name
            expires_at_column: Expires at column name
            created_at_column: Created at column name
            session_id: Session identifier
            data_value: Serialized session data
            expires_at_value: Formatted expiration time
            current_time_value: Formatted current time
            driver: Database driver instance (unused)

        Returns:
            List of SQL statements to execute (check, update, insert pattern)
        """
        check_exists = (
            sql.select(sql.count().as_("count")).from_(table_name).where(sql.column(session_id_column) == session_id)
        )

        update_sql = (
            sql.update(table_name)
            .set(data_column, data_value)
            .set(expires_at_column, expires_at_value)
            .where(sql.column(session_id_column) == session_id)
        )

        insert_sql = (
            sql.insert(table_name)
            .columns(session_id_column, data_column, expires_at_column, created_at_column)
            .values(session_id, data_value, expires_at_value, current_time_value)
        )

        return [check_exists, update_sql, insert_sql]

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
