"""ADBC-specific session store handler with multi-database support.

Standalone handler with no inheritance - clean break implementation.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from sqlspec.driver._async import AsyncDriverAdapterBase
    from sqlspec.driver._sync import SyncDriverAdapterBase

from sqlspec import sql
from sqlspec.utils.serializers import from_json, to_json

__all__ = ("SyncStoreHandler", "AsyncStoreHandler")


class SyncStoreHandler:
    """ADBC-specific session store handler with multi-database support.

    ADBC (Arrow Database Connectivity) supports multiple databases but has
    specific requirements for JSON/JSONB handling due to Arrow type mapping.

    This handler fixes the Arrow struct type mapping issue by serializing
    dicts to JSON strings and provides multi-database UPSERT support.
    """

    def __init__(
        self, table_name: str = "litestar_sessions", data_column: str = "data", *args: Any, **kwargs: Any
    ) -> None:
        """Initialize ADBC store handler.

        Args:
            table_name: Name of the session table
            data_column: Name of the data column
            *args: Additional positional arguments (ignored)
            **kwargs: Additional keyword arguments (ignored)
        """

    def serialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Serialize session data for ADBC storage.

        ADBC has automatic type coercion that converts dicts to JSON strings,
        preventing Arrow struct type conversion issues. We return the raw data
        and let the type coercion handle the JSON encoding automatically.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional)

        Returns:
            Raw data (dict) for ADBC type coercion to handle
        """
        return data

    def deserialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Deserialize session data from ADBC storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (optional)

        Returns:
            Deserialized session data, or original data if deserialization fails
        """
        if isinstance(data, str):
            try:
                return from_json(data)
            except Exception:
                return data
        return data

    def format_datetime(self, dt: datetime) -> datetime:
        """Format datetime for ADBC storage as ISO string.

        Args:
            dt: Datetime to format

        Returns:
            ISO format datetime string
        """
        return dt

    def get_current_time(self) -> datetime:
        """Get current time as ISO string for ADBC.

        Returns:
            Current timestamp as ISO string
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
        driver: Union["SyncDriverAdapterBase", None] = None,
    ) -> "list[Any]":
        """Build SQL statements for upserting session data.

        Uses dialect detection to determine whether to use UPSERT or check-update-insert pattern.
        PostgreSQL, SQLite, and DuckDB support UPSERT with ON CONFLICT.
        Other databases use the check-update-insert pattern.

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
            driver: Database driver instance (optional)

        Returns:
            List with single UPSERT statement or check-update-insert pattern
        """
        dialect = getattr(driver, "dialect", None) if driver else None

        upsert_supported = {"postgres", "postgresql", "sqlite", "duckdb"}

        if dialect in upsert_supported:
            # For PostgreSQL ADBC, we need explicit ::jsonb cast to make cast detection work
            if dialect in {"postgres", "postgresql"}:
                # Use raw SQL with explicit cast for the data parameter
                upsert_sql = sql.raw(f"""
                INSERT INTO {table_name} ({session_id_column}, {data_column}, {expires_at_column}, {created_at_column})
                VALUES (:session_id, :data_value::jsonb, :expires_at_value::timestamp, :current_time_value::timestamp)
                ON CONFLICT ({session_id_column})
                DO UPDATE SET {data_column} = EXCLUDED.{data_column}, {expires_at_column} = EXCLUDED.{expires_at_column}
                """, session_id=session_id, data_value=data_value, expires_at_value=expires_at_value, current_time_value=current_time_value)
                return [upsert_sql]

            # For other databases, use SQL builder
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


class AsyncStoreHandler:
    """ADBC-specific async session store handler with multi-database support.

    ADBC (Arrow Database Connectivity) supports multiple databases but has
    specific requirements for JSON/JSONB handling due to Arrow type mapping.

    This handler fixes the Arrow struct type mapping issue by serializing
    all data to JSON strings and provides multi-database UPSERT support.
    """

    def __init__(
        self, table_name: str = "litestar_sessions", data_column: str = "data", *args: Any, **kwargs: Any
    ) -> None:
        """Initialize ADBC async store handler.

        Args:
            table_name: Name of the session table
            data_column: Name of the data column
            *args: Additional positional arguments (ignored)
            **kwargs: Additional keyword arguments (ignored)
        """

    def serialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Serialize session data for ADBC storage.

        ADBC has automatic type coercion that converts dicts to JSON strings,
        preventing Arrow struct type conversion issues. We return the raw data
        and let the type coercion handle the JSON encoding automatically.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional)

        Returns:
            Raw data (dict) for ADBC type coercion to handle
        """
        return data

    def deserialize_data(
        self, data: Any, driver: Union["SyncDriverAdapterBase", "AsyncDriverAdapterBase", None] = None
    ) -> Any:
        """Deserialize session data from ADBC storage.

        Args:
            data: Raw data from database
            driver: Database driver instance (optional)

        Returns:
            Deserialized session data, or original data if deserialization fails
        """
        if isinstance(data, str):
            try:
                return from_json(data)
            except Exception:
                return data
        return data

    def format_datetime(self, dt: datetime) -> datetime:
        """Format datetime for ADBC storage as ISO string.

        Args:
            dt: Datetime to format

        Returns:
            ISO format datetime string
        """
        return dt

    def get_current_time(self) -> datetime:
        """Get current time as ISO string for ADBC.

        Returns:
            Current timestamp as ISO string
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
        driver: Union["AsyncDriverAdapterBase", None] = None,
    ) -> "list[Any]":
        """Build SQL statements for upserting session data.

        Uses dialect detection to determine whether to use UPSERT or check-update-insert pattern.
        PostgreSQL, SQLite, and DuckDB support UPSERT with ON CONFLICT.
        Other databases use the check-update-insert pattern.

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
            driver: Database driver instance (optional)

        Returns:
            List with single UPSERT statement or check-update-insert pattern
        """
        dialect = getattr(driver, "dialect", None) if driver else None

        upsert_supported = {"postgres", "postgresql", "sqlite", "duckdb"}

        if dialect in upsert_supported:
            # For PostgreSQL ADBC, we need explicit ::jsonb cast to make cast detection work
            if dialect in {"postgres", "postgresql"}:
                # Use raw SQL with explicit cast for the data parameter
                upsert_sql = sql.raw(f"""
                INSERT INTO {table_name} ({session_id_column}, {data_column}, {expires_at_column}, {created_at_column})
                VALUES (:session_id, :data_value::jsonb, :expires_at_value::timestamp, :current_time_value::timestamp)
                ON CONFLICT ({session_id_column})
                DO UPDATE SET {data_column} = EXCLUDED.{data_column}, {expires_at_column} = EXCLUDED.{expires_at_column}
                """, session_id=session_id, data_value=data_value, expires_at_value=expires_at_value, current_time_value=current_time_value)
                return [upsert_sql]

            # For other databases, use SQL builder
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
