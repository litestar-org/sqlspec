"""OracleDB-specific session store handler."""

from datetime import datetime, timezone
from typing import Any, Optional

from sqlspec import SQL, sql
from sqlspec.extensions.litestar.store import BaseStoreHandler
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import ensure_async_

__all__ = ("StoreHandler",)

logger = get_logger("adapters.oracledb.litestar.store")


class StoreHandler(BaseStoreHandler):
    """OracleDB-specific session store handler.

    Oracle requires special handling for:
    - Version-specific JSON storage (JSON type, BLOB with OSON, BLOB with JSON, or CLOB)
    - TO_DATE function for datetime values
    - Uppercase column names in results
    - LOB object handling for large data
    - Binary vs text JSON serialization based on storage type
    """

    def __init__(self, table_name: str = "litestar_sessions", data_column: str = "data", *args: Any, **kwargs: Any) -> None:
        """Initialize Oracle store handler.

        Args:
            table_name: Name of the session table
            data_column: Name of the data column
            *args: Positional arguments passed to parent
            **kwargs: Keyword arguments passed to parent
        """
        super().__init__(*args, **kwargs)
        self._table_name = table_name
        self._data_column = data_column
        self._json_storage_type: Optional[str] = None
        self._version_detected = False

    async def _detect_json_storage_type(self, driver: Any) -> str:
        """Detect the JSON storage type used in the session table.

        Args:
            driver: Database driver instance

        Returns:
            JSON storage type: 'json', 'blob_oson', 'blob_json', or 'clob'
        """
        if self._json_storage_type and self._version_detected:
            return self._json_storage_type

        try:
            # Query the table structure to determine JSON column type
            table_name = self._table_name
            data_column = self._data_column

            result = await ensure_async_(driver.execute)(f"""
                SELECT data_type, data_length, search_condition
                FROM user_tab_columns c
                LEFT JOIN user_constraints con ON c.table_name = con.table_name
                LEFT JOIN user_cons_columns cc ON con.constraint_name = cc.constraint_name
                    AND cc.column_name = c.column_name
                WHERE c.table_name = UPPER('{table_name}')
                    AND c.column_name = UPPER('{data_column}')
            """)

            if not result.data:
                self._json_storage_type = "blob"
                return self._json_storage_type

            row = result.data[0]
            data_type = self.handle_column_casing(row, "data_type")
            search_condition = self.handle_column_casing(row, "search_condition")

            # Determine storage type based on column definition
            if data_type == "JSON":
                self._json_storage_type = "json"
            elif data_type == "BLOB":
                if search_condition and "FORMAT OSON" in str(search_condition):
                    self._json_storage_type = "blob_oson"
                elif search_condition and "IS JSON" in str(search_condition):
                    self._json_storage_type = "blob_json"
                else:
                    # Assume regular BLOB with JSON validation
                    self._json_storage_type = "blob_json"
            else:
                # CLOB or other text type
                self._json_storage_type = "clob"

            self._version_detected = True

        except Exception:
            self._json_storage_type = "blob"
            return self._json_storage_type
        else:
            return self._json_storage_type

    def format_datetime(self, dt: datetime) -> Any:
        """Format datetime for Oracle using TO_DATE function.

        Args:
            dt: Datetime to format

        Returns:
            SQL raw expression with TO_DATE function
        """
        datetime_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        return sql.raw(f"TO_DATE('{datetime_str}', 'YYYY-MM-DD HH24:MI:SS')")

    def get_current_time(self) -> Any:
        """Get current time for Oracle using TO_DATE function.

        Returns:
            SQL raw expression with current timestamp
        """
        current_time = datetime.now(timezone.utc)
        datetime_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        return sql.raw(f"TO_DATE('{datetime_str}', 'YYYY-MM-DD HH24:MI:SS')")

    async def serialize_data(self, data: Any, driver: Any = None) -> Any:
        """Serialize session data for Oracle storage.

        Args:
            data: Session data to serialize
            driver: Database driver instance (optional, for storage type detection)

        Returns:
            Serialized data appropriate for the Oracle storage type
        """
        if driver is not None:
            await self._ensure_storage_type_detected(driver)
        storage_type = getattr(self, "_json_storage_type", None)

        print(f"Oracle serialize_data - storage_type: {storage_type}")
        print(f"Oracle serialize_data - data type: {type(data)}")

        if storage_type == "json":
            # Native JSON type - let Oracle handle it directly
            return data
        elif storage_type in ("blob_oson", "blob_json") or storage_type is None:
            # BLOB storage or fallback - use binary JSON
            try:
                return to_json(data, as_bytes=True)
            except (TypeError, ValueError):
                return str(data).encode("utf-8")
        else:
            # CLOB storage - use text JSON
            return super().serialize_data(data)

    async def deserialize_data(self, data: Any, driver: Any = None) -> Any:
        """Deserialize session data from Oracle storage.

        Args:
            data: Raw data from database (already processed by store layer)
            driver: Database driver instance (optional, for storage type detection)

        Returns:
            Deserialized session data
        """
        # LOB reading is now handled at the store level, so we only
        # process the already-read data here

        if driver is not None:
            await self._ensure_storage_type_detected(driver)
        storage_type = getattr(self, "_json_storage_type", None)

        if storage_type == "json":
            # Native JSON type - should be Python objects already
            if isinstance(data, (dict, list)):
                return data
            # Parse if still string
            if isinstance(data, str):
                try:
                    return from_json(data)
                except (ValueError, TypeError):
                    return data
        elif storage_type in ("blob_oson", "blob_json"):
            # BLOB storage with JSON format
            if isinstance(data, bytes):
                try:
                    data_str = data.decode("utf-8")
                    return from_json(data_str)
                except (UnicodeDecodeError, ValueError, TypeError):
                    return str(data)
            elif isinstance(data, str):
                try:
                    return from_json(data)
                except (ValueError, TypeError):
                    return data

        # For CLOB and fallback cases, use parent deserialization
        return super().deserialize_data(data)

    async def _ensure_storage_type_detected(self, driver: Any) -> None:
        """Ensure JSON storage type is detected before operations.

        Args:
            driver: Database driver instance
        """
        if not self._version_detected:
            await self._detect_json_storage_type(driver)

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
        """Build SQL statements for upserting session data using Oracle MERGE.

        Oracle has a 4000-character limit for string literals and TTC buffer limits
        for bind parameters. For large data, we use a check-update-insert pattern.

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

        Returns:
            List of SQL statements (single MERGE for small data, or check/update/insert for large data)
        """
        # Convert SQL expressions to their string representation
        expires_at_str = str(expires_at_value)
        current_time_str = str(current_time_value)

        # Debug: print the SQL and data size for troubleshooting
        print(f"\n=== ORACLE SQL DEBUG ===")
        print(f"Data value type: {type(data_value)}")
        if hasattr(data_value, '__len__'):
            print(f"Data value size: {len(data_value)} bytes/chars")
        print(f"Session ID: {session_id}")
        print(f"Table: {table_name}")
        print(f"Expires at: {expires_at_str}")
        print(f"Current time: {current_time_str}")
        print(f"========================\n")

        # Check data size to determine approach
        data_size = len(data_value) if hasattr(data_value, '__len__') else 0
        use_large_data_approach = data_size > 4000  # Oracle's literal size limit

        if use_large_data_approach:
            print("Using large data approach: check-update-insert pattern")

            # For large data, use separate check/update/insert to avoid TTC buffer issues
            check_sql = SQL(f"SELECT COUNT(*) as count FROM {table_name} WHERE {session_id_column} = :session_id",
                           session_id=session_id)

            update_sql = SQL(f"""
                UPDATE {table_name}
                SET {data_column} = :data_value, {expires_at_column} = {expires_at_str}
                WHERE {session_id_column} = :session_id
            """, session_id=session_id, data_value=data_value)

            insert_sql = SQL(f"""
                INSERT INTO {table_name} ({session_id_column}, {data_column}, {expires_at_column}, {created_at_column})
                VALUES (:session_id, :data_value, {expires_at_str}, {current_time_str})
            """, session_id=session_id, data_value=data_value)

            return [check_sql, update_sql, insert_sql]
        else:
            print("Using standard MERGE approach")

            # For smaller data, use improved MERGE statement structure
            merge_sql_text = f"""
                MERGE INTO {table_name} target
                USING (SELECT :session_id AS {session_id_column},
                              :data_value AS {data_column},
                              {expires_at_str} AS {expires_at_column},
                              {current_time_str} AS {created_at_column} FROM dual) source
                ON (target.{session_id_column} = source.{session_id_column})
                WHEN MATCHED THEN
                    UPDATE SET
                        {data_column} = source.{data_column},
                        {expires_at_column} = source.{expires_at_column}
                WHEN NOT MATCHED THEN
                    INSERT ({session_id_column}, {data_column}, {expires_at_column}, {created_at_column})
                    VALUES (source.{session_id_column}, source.{data_column}, source.{expires_at_column}, source.{created_at_column})
            """

            print(f"Oracle MERGE SQL: {merge_sql_text}")
            merge_sql = SQL(merge_sql_text, session_id=session_id, data_value=data_value)
            return [merge_sql]

    def handle_column_casing(self, row: "dict[str, Any]", column: str) -> Any:
        """Handle Oracle's uppercase column name preference.

        Args:
            row: Result row from database
            column: Column name to look up

        Returns:
            Column value, checking uppercase first for Oracle
        """
        # Oracle typically returns uppercase column names by default
        if column.upper() in row:
            return row[column.upper()]
        if column in row:
            return row[column]
        if column.lower() in row:
            return row[column.lower()]
        msg = f"Column {column} not found in result row"
        raise KeyError(msg)
