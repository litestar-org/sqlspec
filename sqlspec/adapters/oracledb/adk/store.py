"""Oracle ADK store for Google Agent Development Kit session/event storage."""

from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec import SQL
from sqlspec.adapters.oracledb._typing import DatabaseError as OracleDatabaseError
from sqlspec.adapters.oracledb.data_dictionary import (
    OracledbAsyncDataDictionary,
    OracledbSyncDataDictionary,
    OracleVersionInfo,
)
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_
from sqlspec.utils.type_guards import is_async_readable, is_readable

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.extensions.adk import MemoryRecord

logger = get_logger("sqlspec.adapters.oracledb.adk.store")

__all__ = (
    "JSONStorageType",
    "OracleAsyncADKMemoryStore",
    "OracleAsyncADKStore",
    "OracleSyncADKMemoryStore",
    "OracleSyncADKStore",
    "coerce_decimal_values",
    "storage_type_from_version",
)

ORACLE_TABLE_NOT_FOUND_ERROR: Final = 942
ORACLE_MIN_JSON_NATIVE_VERSION: Final = 21
ORACLE_MIN_JSON_NATIVE_COMPATIBLE: Final = 20
ORACLE_MIN_JSON_BLOB_VERSION: Final = 12
ORACLE_DEFAULT_HASH_PARTITIONS: Final = 16
ORACLE_MIN_HASH_PARTITIONS: Final = 2
ORACLE_RANGE_INTERVALS: Final[dict[str, str]] = {
    "day": "NUMTODSINTERVAL(1, 'DAY')",
    "week": "NUMTODSINTERVAL(7, 'DAY')",
    "month": "NUMTOYMINTERVAL(1, 'MONTH')",
    "year": "NUMTOYMINTERVAL(1, 'YEAR')",
}
ORACLE_COMPRESSION_CLAUSES: Final[dict[str, str]] = {
    "basic": "ROW STORE COMPRESS BASIC",
    "oltp": "ROW STORE COMPRESS ADVANCED",
    "advanced": "ROW STORE COMPRESS ADVANCED",
    "query_low": "COLUMN STORE COMPRESS FOR QUERY LOW",
    "query_high": "COLUMN STORE COMPRESS FOR QUERY HIGH",
    "archive_low": "COLUMN STORE COMPRESS FOR ARCHIVE LOW",
    "archive_high": "COLUMN STORE COMPRESS FOR ARCHIVE HIGH",
}
ORACLE_DUPLICATE_KEY_ERROR: Final = 1


class JSONStorageType(str, Enum):
    """JSON storage type based on Oracle version."""

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob_plain"


def coerce_decimal_values(value: Any) -> Any:
    return _coerce_decimal_values(value)


def storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    return _storage_type_from_version(version_info)


class OracleAsyncADKStore(BaseAsyncADKStore["OracleAsyncConfig"]):
    """Oracle async ADK store using oracledb async driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb async driver. Provides:
    - Session state management with version-specific JSON storage
    - Full-fidelity event storage via ``event_data`` column
    - Atomic ``append_event_and_update_state`` for durable session mutations
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleAsyncConfig with extension_config["adk"] settings.

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - event_data stored as JSON (21c+) or BLOB (older versions)
        - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
        - Named parameters using :param_name
        - State merging handled at application level
        - owner_id_column supports NUMBER, VARCHAR2, RAW for Oracle FK types
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        """Initialize Oracle ADK store.

        Args:
            config: OracleAsyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_session")
            - events_table: Events table name (default: "adk_event")
            - owner_id_column: Optional owner FK column DDL (default: None)
            - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)
        """
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None

        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist.

        Notes:
            Detects Oracle version to determine optimal JSON storage type.
            Uses version-appropriate table schema.
        """
        storage_type = await self._detect_json_storage_type()
        logger.debug("Creating ADK tables with storage type: %s", storage_type)

        async with self._config.provide_session() as driver:
            await driver.execute_script(self._get_create_sessions_table_sql_for_type(storage_type))

            await driver.execute_script(self._get_create_events_table_sql_for_type(storage_type))
            await driver.execute_script(self._get_create_app_states_table_sql_for_type(storage_type))
            await driver.execute_script(self._get_create_user_states_table_sql_for_type(storage_type))
            await driver.execute_script(await self._get_create_metadata_table_sql())
            await driver.execute_script(await self._get_seed_metadata_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Created session record.

        Notes:
            Uses SYSTIMESTAMP for create_time and update_time.
            State is serialized using version-appropriate format.
            owner_id is ignored if owner_id_column not configured.
        """
        state_data = await self._serialize_state(state)

        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time, {self._owner_id_column_name})
            VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP, :owner_id)
            """
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_data,
                "owner_id": owner_id,
            }
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)
            """
            params = {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_data}

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, params)
            await conn.commit()

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(
        self, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.
            renew_for: If positive, touch update_time while reading.

        Returns:
            Session record or None if not found.

        Notes:
            Oracle returns datetime objects for TIMESTAMP columns.
            State is deserialized using version-appropriate format.
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    await cursor.execute(
                        f"UPDATE {self._session_table} SET update_time = SYSTIMESTAMP WHERE id = :id",
                        {"id": session_id},
                    )
                    await conn.commit()

                await cursor.execute(
                    f"""
                    SELECT id, app_name, user_id, state, create_time, update_time
                    FROM {self._session_table}
                    WHERE id = :id
                    """,
                    {"id": session_id},
                )
                row = await cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_data, create_time, update_time = row

                state = await self._deserialize_state(state_data)

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=state,
                    create_time=create_time,
                    update_time=update_time,
                )
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Updates update_time to current timestamp.
            State is serialized using version-appropriate format.
        """
        state_data = await self._serialize_state(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"state": state_data, "id": session_id})
            await conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = :id"

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"id": session_id})
            await conn.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses composite index on (app_name, user_id) when user_id is provided.
            State is deserialized using version-appropriate format.
        """

        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = :app_name
            ORDER BY update_time DESC
            """
            params = {"app_name": app_name}
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = :app_name AND user_id = :user_id
            ORDER BY update_time DESC
            """
            params = {"app_name": app_name, "user_id": user_id}

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                results = []
                for row in rows:
                    state = await self._deserialize_state(row[3])

                    results.append(
                        SessionRecord(
                            id=row[0],
                            app_name=row[1],
                            user_id=row[2],
                            state=state,
                            create_time=row[4],
                            update_time=row[5],
                        )
                    )
                return results
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record with 5 keys: session_id, invocation_id,
                author, timestamp, event_data.
        """
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_data
        )
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                sql,
                {
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "author": event_record["author"],
                    "timestamp": event_record["timestamp"],
                    "event_data": await self._serialize_event_data(event_record["event_data"]),
                },
            )
            await conn.commit()

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_name: "str | None" = None,
        user_id: "str | None" = None,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically append an event and update session + scoped state.

        All writes are executed within a single transaction so they succeed or
        fail together. The refreshed SessionRecord is read inside the same
        transaction (Oracle's RETURNING INTO requires output bind variables
        which complicate async cursor handling, so SELECT-after-UPDATE is used).
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_data
        )
        """

        state_data = await self._serialize_state(state)
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
        """

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = :id
        """

        app_upsert_sql = f"""
        MERGE INTO {self._app_state_table} target
        USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, source.state, SYSTIMESTAMP)
        """

        user_upsert_sql = f"""
        MERGE INTO {self._user_state_table} target
        USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name AND target.user_id = source.user_id)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, source.state, SYSTIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                insert_sql,
                {
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "author": event_record["author"],
                    "timestamp": event_record["timestamp"],
                    "event_data": await self._serialize_event_data(event_record["event_data"]),
                },
            )
            await cursor.execute(update_sql, {"state": state_data, "id": session_id})
            await cursor.execute(select_sql, {"id": session_id})
            row = await cursor.fetchone()
            if app_state:
                if app_name is None:
                    msg = "app_name is required when app_state is provided."
                    raise ValueError(msg)
                await cursor.execute(
                    app_upsert_sql, {"app_name": app_name, "state": await self._serialize_state(app_state)}
                )
            if user_state:
                if app_name is None or user_id is None:
                    msg = "app_name and user_id are required when user_state is provided."
                    raise ValueError(msg)
                await cursor.execute(
                    user_upsert_sql,
                    {"app_name": app_name, "user_id": user_id, "state": await self._serialize_state(user_state)},
                )
            await conn.commit()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        session_id_val, row_app_name, row_user_id, state_data_row, create_time, update_time = row
        return SessionRecord(
            id=session_id_val,
            app_name=row_app_name,
            user_id=row_user_id,
            state=await self._deserialize_state(state_data_row),
            create_time=create_time,
            update_time=update_time,
        )

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.
        """

        where_clauses = ["session_id = :session_id"]
        params: dict[str, Any] = {"session_id": session_id}

        if after_timestamp is not None:
            where_clauses.append("timestamp > :after_timestamp")
            params["after_timestamp"] = after_timestamp

        where_clause = " AND ".join(where_clauses)
        limit_clause = ""
        if limit:
            limit_clause = f" FETCH FIRST {limit} ROWS ONLY"

        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_data
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    EventRecord(
                        session_id=row[0],
                        invocation_id=_oracle_text_value(row[1]),
                        author=_oracle_text_value(row[2]),
                        timestamp=row[3],
                        event_data=await self._deserialize_json_field(row[4]) or {},
                    )
                    for row in rows
                ]
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < :before"

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, {"before": before})
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < :updated_before"

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, {"updated_before": updated_before})
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = :app_name"

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, {"app_name": app_name})
                row = await cursor.fetchone()
                return await self._deserialize_state(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        sql = f"""
        SELECT state
        FROM {self._user_state_table}
        WHERE app_name = :app_name AND user_id = :user_id
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, {"app_name": app_name, "user_id": user_id})
                row = await cursor.fetchone()
                return await self._deserialize_state(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        sql = f"""
        MERGE INTO {self._app_state_table} target
        USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, source.state, SYSTIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"app_name": app_name, "state": await self._serialize_state(state)})
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        sql = f"""
        MERGE INTO {self._user_state_table} target
        USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name AND target.user_id = source.user_id)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, source.state, SYSTIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                sql, {"app_name": app_name, "user_id": user_id, "state": await self._serialize_state(state)}
            )
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = :key"

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, {"key": key})
                row = await cursor.fetchone()
                return str(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        sql = f"""
        MERGE INTO {self._metadata_table} target
        USING (SELECT :key AS key, :value AS value FROM DUAL) source
        ON (target.key = source.key)
        WHEN MATCHED THEN
            UPDATE SET target.value = source.value
        WHEN NOT MATCHED THEN
            INSERT (key, value)
            VALUES (source.key, source.value)
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"key": key, "value": value})
            await conn.commit()

    async def _get_create_sessions_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for sessions table.

        Auto-detects optimal JSON storage type based on Oracle version.
        Result is cached to minimize database queries.
        """
        storage_type = await self._detect_json_storage_type()
        return self._get_create_sessions_table_sql_for_type(storage_type)

    async def _get_create_events_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for events table.

        Auto-detects optimal JSON storage type based on Oracle version.
        Result is cached to minimize database queries.
        """
        storage_type = await self._detect_json_storage_type()
        return self._get_create_events_table_sql_for_type(storage_type)

    async def _get_create_app_states_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for app-scoped state."""
        storage_type = await self._detect_json_storage_type()
        return self._get_create_app_states_table_sql_for_type(storage_type)

    async def _get_create_user_states_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for user-scoped state."""
        storage_type = await self._detect_json_storage_type()
        return self._get_create_user_states_table_sql_for_type(storage_type)

    async def _get_create_metadata_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for ADK internal metadata."""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._metadata_table} (
                key VARCHAR2(128) PRIMARY KEY,
                value VARCHAR2(512) NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    async def _get_seed_metadata_sql(self) -> str:
        """Get Oracle SQL to seed the ADK schema-version metadata row."""
        return f"""
        BEGIN
            INSERT INTO {self._metadata_table} (key, value)
            SELECT 'schema_version', '1'
            FROM DUAL
            WHERE NOT EXISTS (
                SELECT 1 FROM {self._metadata_table} WHERE key = 'schema_version'
            );
        END;
        """

    async def _detect_json_storage_type(self) -> JSONStorageType:
        """Detect the appropriate JSON storage type based on Oracle version.

        Returns:
            Appropriate JSONStorageType for this Oracle version.

        Notes:
            Queries product_component_version to determine Oracle version.
            - Oracle 21c+ with compatible >= 20: Native JSON type
            - Oracle 12c+: BLOB with IS JSON constraint
            - Oracle 11g and earlier: plain BLOB

            Result is cached in self._json_storage_type.
        """
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = await self._get_version_info()
        self._json_storage_type = _storage_type_from_version(version_info)
        return self._json_storage_type

    async def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return cached Oracle version info using Oracle data dictionary."""

        if self._oracle_version_info is not None:
            return self._oracle_version_info

        async with self._config.provide_session() as driver:
            dictionary = OracledbAsyncDataDictionary()
            self._oracle_version_info = await dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    async def _serialize_state(self, state: "dict[str, Any]") -> "str | bytes":
        """Serialize state dictionary to appropriate format based on storage type.

        Args:
            state: State dictionary to serialize.

        Returns:
            JSON string for JSON_NATIVE, bytes for BLOB types.
        """
        storage_type = await self._detect_json_storage_type()

        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(state)

        return to_json(state, as_bytes=True)

    async def _deserialize_state(self, data: Any) -> "dict[str, Any]":
        """Deserialize state data from database format.

        Args:
            data: Data from database (may be LOB, str, bytes, or dict).

        Returns:
            Deserialized state dictionary.

        Notes:
            Handles LOB reading if data has read() method.
            Oracle JSON type may return dict directly.
        """
        if is_async_readable(data):
            data = await data.read()
        elif is_readable(data):
            data = data.read()

        if isinstance(data, dict):
            return cast("dict[str, Any]", _coerce_decimal_values(data))

        if isinstance(data, bytes):
            return from_json(data)  # type: ignore[no-any-return]

        if isinstance(data, str):
            return from_json(data)  # type: ignore[no-any-return]

        return from_json(str(data))  # type: ignore[no-any-return]

    async def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize JSON payloads from Oracle JSON/BLOB/LOB values."""
        if data is None:
            return None
        return await self._deserialize_state(data)

    async def _serialize_event_data(self, event_data: Any) -> "str | bytes":
        """Serialize event_data to the configured Oracle JSON storage format."""
        storage_type = await self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(event_data)
        return to_json(event_data, as_bytes=True)

    async def _read_event_data(self, data: Any) -> str:
        """Read event_data from database, handling LOB types.

        Args:
            data: Data from database (may be LOB, str, or dict).

        Returns:
            JSON string.
        """
        if is_async_readable(data):
            data = await data.read()
        elif is_readable(data):
            data = data.read()

        if isinstance(data, dict):
            return to_json(data)

        if isinstance(data, bytes):
            return data.decode("utf-8")

        return str(data)

    def _get_create_sessions_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for sessions with specified storage type.

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_session table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            state_column = "state JSON NOT NULL"
        elif storage_type == JSONStorageType.BLOB_JSON:
            state_column = "state BLOB CHECK (state IS JSON) NOT NULL"
        else:
            state_column = "state BLOB NOT NULL"

        owner_id_column_sql = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "session",
            in_memory=self._in_memory,
            hash_partition_key="id",
            range_partition_key="create_time",
        )

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._session_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL{owner_id_column_sql}
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._session_table}_app_user
                ON {self._session_table}(app_name, user_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._session_table}_update_time
                ON {self._session_table}(update_time DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_events_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for events with specified storage type.

        The events table uses the new 5-column contract: session_id, invocation_id,
        author, timestamp, and event_data. The event_data column stores the full
        ADK Event as JSON (21c+) or BLOB (older versions).

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_event table.
        """
        event_data_col = _event_data_column_ddl(storage_type)
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "events",
            in_memory=self._in_memory,
            hash_partition_key="session_id",
            range_partition_key="timestamp",
        )

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                session_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256),
                author VARCHAR2(256),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {event_data_col},
                CONSTRAINT fk_{self._events_table}_session FOREIGN KEY (session_id)
                    REFERENCES {self._session_table}(id) ON DELETE CASCADE
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._events_table}_session
                ON {self._events_table}(session_id, timestamp ASC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_app_states_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for app-scoped state with specified storage type."""
        state_column = _json_column_ddl("state", storage_type)

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._app_state_table} (
                app_name VARCHAR2(128) PRIMARY KEY,
                {state_column},
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_user_states_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for user-scoped state with specified storage type."""
        state_column = _json_column_ddl("state", storage_type)

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._user_state_table} (
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                PRIMARY KEY (app_name, user_id)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_app_states_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._app_state_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_user_states_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._user_state_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_metadata_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._metadata_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            Oracle automatically drops indexes when dropping tables.
        """
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._events_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._session_table}_update_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._session_table}_app_user';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._events_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._session_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]


class OracleSyncADKStore(BaseAsyncADKStore["OracleSyncConfig"]):
    """Oracle synchronous ADK store using oracledb sync driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb synchronous driver. Provides:
    - Session state management with version-specific JSON storage
    - Full-fidelity event storage via ``event_data`` column
    - Atomic ``create_event_and_update_state`` for durable session mutations
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleSyncConfig with extension_config["adk"] settings.

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - event_data stored as JSON (21c+) or BLOB (older versions)
        - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
        - Named parameters using :param_name
        - State merging handled at application level
        - owner_id_column supports NUMBER, VARCHAR2, RAW for Oracle FK types
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleSyncConfig") -> None:
        """Initialize Oracle synchronous ADK store.

        Args:
            config: OracleSyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_session")
            - events_table: Events table name (default: "adk_event")
            - owner_id_column: Optional owner FK column DDL (default: None)
            - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)
        """
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None

        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    async def get_session(
        self, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID."""
        return await async_(self._get_session)(session_id, renew_for)

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await async_(self._update_session_state)(session_id, state)

    async def delete_session(self, session_id: str) -> None:
        """Delete session and associated events."""
        await async_(self._delete_session)(session_id)

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return await async_(self._list_sessions)(app_name, user_id)

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_name: "str | None" = None,
        user_id: "str | None" = None,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically append an event and update session + scoped state."""
        return await async_(self._append_event_and_update_state)(
            event_record,
            session_id,
            state,
            app_name=app_name,
            user_id=user_id,
            app_state=app_state,
            user_state=user_state,
        )

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return await async_(self._get_events)(session_id, after_timestamp, limit)

    async def delete_expired_events(self, before: "datetime") -> int:
        """Delete events older than the given timestamp."""
        return await async_(self._delete_expired_events)(before)

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        """Delete sessions whose update_time predates the given threshold."""
        return await async_(self._delete_idle_sessions)(updated_before)

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        return await async_(self._get_app_state)(app_name)

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        return await async_(self._get_user_state)(app_name, user_id)

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        await async_(self._upsert_app_state)(app_name, state)

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        await async_(self._upsert_user_state)(app_name, user_id, state)

    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        return await async_(self._get_metadata)(key)

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        await async_(self._set_metadata)(key, value)

    async def _get_create_sessions_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for sessions table.

        Auto-detects optimal JSON storage type based on Oracle version.
        Result is cached to minimize database queries.
        """
        storage_type = self._detect_json_storage_type()
        return self._get_create_sessions_table_sql_for_type(storage_type)

    async def _get_create_events_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for events table.

        Auto-detects optimal JSON storage type based on Oracle version.
        Result is cached to minimize database queries.
        """
        storage_type = self._detect_json_storage_type()
        return self._get_create_events_table_sql_for_type(storage_type)

    async def _get_create_app_states_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for app-scoped state."""
        storage_type = self._detect_json_storage_type()
        return self._get_create_app_states_table_sql_for_type(storage_type)

    async def _get_create_user_states_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for user-scoped state."""
        storage_type = self._detect_json_storage_type()
        return self._get_create_user_states_table_sql_for_type(storage_type)

    async def _get_create_metadata_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for ADK internal metadata."""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._metadata_table} (
                key VARCHAR2(128) PRIMARY KEY,
                value VARCHAR2(512) NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    async def _get_seed_metadata_sql(self) -> str:
        """Get Oracle SQL to seed the ADK schema-version metadata row."""
        return f"""
        BEGIN
            INSERT INTO {self._metadata_table} (key, value)
            SELECT 'schema_version', '1'
            FROM DUAL
            WHERE NOT EXISTS (
                SELECT 1 FROM {self._metadata_table} WHERE key = 'schema_version'
            );
        END;
        """

    def _detect_json_storage_type(self) -> JSONStorageType:
        """Detect the appropriate JSON storage type based on Oracle version.

        Returns:
            Appropriate JSONStorageType for this Oracle version.

        Notes:
            Queries product_component_version to determine Oracle version.
            - Oracle 21c+ with compatible >= 20: Native JSON type
            - Oracle 12c+: BLOB with IS JSON constraint
            - Oracle 11g and earlier: plain BLOB

            Result is cached in self._json_storage_type.
        """
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = self._get_version_info()
        self._json_storage_type = _storage_type_from_version(version_info)
        return self._json_storage_type

    def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return cached Oracle version info using Oracle data dictionary."""

        if self._oracle_version_info is not None:
            return self._oracle_version_info

        with self._config.provide_session() as driver:
            dictionary = OracledbSyncDataDictionary()
            self._oracle_version_info = dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    def _serialize_state(self, state: "dict[str, Any]") -> "str | bytes":
        """Serialize state dictionary to appropriate format based on storage type.

        Args:
            state: State dictionary to serialize.

        Returns:
            JSON string for JSON_NATIVE, bytes for BLOB types.
        """
        storage_type = self._detect_json_storage_type()

        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(state)

        return to_json(state, as_bytes=True)

    def _deserialize_state(self, data: Any) -> "dict[str, Any]":
        """Deserialize state data from database format.

        Args:
            data: Data from database (may be LOB, str, bytes, or dict).

        Returns:
            Deserialized state dictionary.

        Notes:
            Handles LOB reading if data has read() method.
            Oracle JSON type may return dict directly.
        """
        if is_readable(data):
            data = data.read()

        if isinstance(data, dict):
            return cast("dict[str, Any]", _coerce_decimal_values(data))

        if isinstance(data, bytes):
            return from_json(data)  # type: ignore[no-any-return]

        if isinstance(data, str):
            return from_json(data)  # type: ignore[no-any-return]

        return from_json(str(data))  # type: ignore[no-any-return]

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize JSON payloads from Oracle JSON/BLOB/LOB values."""
        if data is None:
            return None
        return self._deserialize_state(data)

    def _serialize_event_data(self, event_data: Any) -> "str | bytes":
        """Serialize event_data to the configured Oracle JSON storage format."""
        storage_type = self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(event_data)
        return to_json(event_data, as_bytes=True)

    def _read_event_data(self, data: Any) -> str:
        """Read event_data from database, handling LOB types.

        Args:
            data: Data from database (may be LOB, str, or dict).

        Returns:
            JSON string.
        """
        if is_readable(data):
            data = data.read()

        if isinstance(data, dict):
            return to_json(data)

        if isinstance(data, bytes):
            return data.decode("utf-8")

        return str(data)

    def _get_create_sessions_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for sessions with specified storage type.

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_session table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            state_column = "state JSON NOT NULL"
        elif storage_type == JSONStorageType.BLOB_JSON:
            state_column = "state BLOB CHECK (state IS JSON) NOT NULL"
        else:
            state_column = "state BLOB NOT NULL"

        owner_id_column_sql = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "session",
            in_memory=self._in_memory,
            hash_partition_key="id",
            range_partition_key="create_time",
        )

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._session_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL{owner_id_column_sql}
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._session_table}_app_user
                ON {self._session_table}(app_name, user_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._session_table}_update_time
                ON {self._session_table}(update_time DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_events_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for events with specified storage type.

        The events table uses the new 5-column contract: session_id, invocation_id,
        author, timestamp, and event_data. The event_data column stores the full
        ADK Event as JSON (21c+) or BLOB (older versions).

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_event table.
        """
        event_data_col = _event_data_column_ddl(storage_type)
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "events",
            in_memory=self._in_memory,
            hash_partition_key="session_id",
            range_partition_key="timestamp",
        )

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                session_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256),
                author VARCHAR2(256),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {event_data_col},
                CONSTRAINT fk_{self._events_table}_session FOREIGN KEY (session_id)
                    REFERENCES {self._session_table}(id) ON DELETE CASCADE
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._events_table}_session
                ON {self._events_table}(session_id, timestamp ASC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_app_states_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for app-scoped state with specified storage type."""
        state_column = _json_column_ddl("state", storage_type)

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._app_state_table} (
                app_name VARCHAR2(128) PRIMARY KEY,
                {state_column},
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_create_user_states_table_sql_for_type(self, storage_type: JSONStorageType) -> str:
        """Get Oracle CREATE TABLE SQL for user-scoped state with specified storage type."""
        state_column = _json_column_ddl("state", storage_type)

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._user_state_table} (
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                PRIMARY KEY (app_name, user_id)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_app_states_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._app_state_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_user_states_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._user_state_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_metadata_table_sql(self) -> str:
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {self._metadata_table}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            Oracle automatically drops indexes when dropping tables.
        """
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._events_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._session_table}_update_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._session_table}_app_user';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._events_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._session_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    def _create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist.

        Notes:
            Detects Oracle version to determine optimal JSON storage type.
            Uses version-appropriate table schema.
        """
        storage_type = self._detect_json_storage_type()
        logger.info("Creating ADK tables with storage type: %s", storage_type)

        with self._config.provide_session() as driver:
            sessions_sql = SQL(self._get_create_sessions_table_sql_for_type(storage_type))
            driver.execute_script(sessions_sql)

            events_sql = SQL(self._get_create_events_table_sql_for_type(storage_type))
            driver.execute_script(events_sql)
            driver.execute_script(SQL(self._get_create_app_states_table_sql_for_type(storage_type)))
            driver.execute_script(SQL(self._get_create_user_states_table_sql_for_type(storage_type)))
            driver.execute_script(SQL(run_(self._get_create_metadata_table_sql)()))
            driver.execute_script(SQL(run_(self._get_seed_metadata_sql)()))

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Created session record.

        Notes:
            Uses SYSTIMESTAMP for create_time and update_time.
            State is serialized using version-appropriate format.
            owner_id is ignored if owner_id_column not configured.
        """
        state_data = self._serialize_state(state)

        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time, {self._owner_id_column_name})
            VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP, :owner_id)
            """
            params = {
                "id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "state": state_data,
                "owner_id": owner_id,
            }
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (:id, :app_name, :user_id, :state, SYSTIMESTAMP, SYSTIMESTAMP)
            """
            params = {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_data}

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()

        result = self._get_session(session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    def _get_session(self, session_id: str, renew_for: "int | timedelta | None" = None) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.
            renew_for: If positive, touch update_time while reading.

        Returns:
            Session record or None if not found.

        Notes:
            Oracle returns datetime objects for TIMESTAMP columns.
            State is deserialized using version-appropriate format.
        """

        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = :id
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    cursor.execute(
                        f"UPDATE {self._session_table} SET update_time = SYSTIMESTAMP WHERE id = :id",
                        {"id": session_id},
                    )
                    conn.commit()

                cursor.execute(sql, {"id": session_id})
                row = cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_data, create_time, update_time = row

                state = self._deserialize_state(state_data)

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=state,
                    create_time=create_time,
                    update_time=update_time,
                )
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Updates update_time to current timestamp.
            State is serialized using version-appropriate format.
        """
        state_data = self._serialize_state(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"state": state_data, "id": session_id})
            conn.commit()

    def _delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = :id"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"id": session_id})
            conn.commit()

    def _list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses composite index on (app_name, user_id) when user_id is provided.
            State is deserialized using version-appropriate format.
        """

        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = :app_name
            ORDER BY update_time DESC
            """
            params = {"app_name": app_name}
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = :app_name AND user_id = :user_id
            ORDER BY update_time DESC
            """
            params = {"app_name": app_name, "user_id": user_id}

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                results = []
                for row in rows:
                    state = self._deserialize_state(row[3])

                    results.append(
                        SessionRecord(
                            id=row[0],
                            app_name=row[1],
                            user_id=row[2],
                            state=state,
                            create_time=row[4],
                            update_time=row[5],
                        )
                    )
                return results
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    def _append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_name: "str | None" = None,
        user_id: "str | None" = None,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically create an event and update session + scoped state.

        All writes are executed within a single transaction so they succeed or
        fail together; the refreshed SessionRecord is read inside the same
        transaction.
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_data
        )
        """

        state_data = self._serialize_state(state)
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
        """

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = :id
        """

        app_upsert_sql = f"""
        MERGE INTO {self._app_state_table} target
        USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, source.state, SYSTIMESTAMP)
        """

        user_upsert_sql = f"""
        MERGE INTO {self._user_state_table} target
        USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name AND target.user_id = source.user_id)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, source.state, SYSTIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                insert_sql,
                {
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "author": event_record["author"],
                    "timestamp": event_record["timestamp"],
                    "event_data": self._serialize_event_data(event_record["event_data"]),
                },
            )
            cursor.execute(update_sql, {"state": state_data, "id": session_id})
            cursor.execute(select_sql, {"id": session_id})
            row = cursor.fetchone()
            if app_state:
                if app_name is None:
                    msg = "app_name is required when app_state is provided."
                    raise ValueError(msg)
                cursor.execute(app_upsert_sql, {"app_name": app_name, "state": self._serialize_state(app_state)})
            if user_state:
                if app_name is None or user_id is None:
                    msg = "app_name and user_id are required when user_state is provided."
                    raise ValueError(msg)
                cursor.execute(
                    user_upsert_sql,
                    {"app_name": app_name, "user_id": user_id, "state": self._serialize_state(user_state)},
                )
            conn.commit()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        session_id_val, row_app_name, row_user_id, state_data_row, create_time, update_time = row
        return SessionRecord(
            id=session_id_val,
            app_name=row_app_name,
            user_id=row_user_id,
            state=self._deserialize_state(state_data_row),
            create_time=create_time,
            update_time=update_time,
        )

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.
        """

        where_clauses = ["session_id = :session_id"]
        params: dict[str, Any] = {"session_id": session_id}

        if after_timestamp is not None:
            where_clauses.append("timestamp > :after_timestamp")
            params["after_timestamp"] = after_timestamp

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" FETCH FIRST {limit} ROWS ONLY" if limit else ""
        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_data
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                return [
                    EventRecord(
                        session_id=row[0],
                        invocation_id=_oracle_text_value(row[1]),
                        author=_oracle_text_value(row[2]),
                        timestamp=row[3],
                        event_data=self._deserialize_json_field(row[4]) or {},
                    )
                    for row in rows
                ]
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    def _delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < :before"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"before": before})
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    def _delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < :updated_before"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"updated_before": updated_before})
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_data
        )
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                sql,
                {
                    "session_id": event_record["session_id"],
                    "invocation_id": event_record["invocation_id"],
                    "author": event_record["author"],
                    "timestamp": event_record["timestamp"],
                    "event_data": self._serialize_event_data(event_record["event_data"]),
                },
            )
            conn.commit()

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Synchronous implementation of get_app_state."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = :app_name"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"app_name": app_name})
                row = cursor.fetchone()
                return self._deserialize_state(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Synchronous implementation of get_user_state."""
        sql = f"""
        SELECT state
        FROM {self._user_state_table}
        WHERE app_name = :app_name AND user_id = :user_id
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"app_name": app_name, "user_id": user_id})
                row = cursor.fetchone()
                return self._deserialize_state(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of upsert_app_state."""
        sql = f"""
        MERGE INTO {self._app_state_table} target
        USING (SELECT :app_name AS app_name, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, source.state, SYSTIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"app_name": app_name, "state": self._serialize_state(state)})
            conn.commit()

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of upsert_user_state."""
        sql = f"""
        MERGE INTO {self._user_state_table} target
        USING (SELECT :app_name AS app_name, :user_id AS user_id, :state AS state FROM DUAL) source
        ON (target.app_name = source.app_name AND target.user_id = source.user_id)
        WHEN MATCHED THEN
            UPDATE SET target.state = source.state, target.update_time = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, source.state, SYSTIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"app_name": app_name, "user_id": user_id, "state": self._serialize_state(state)})
            conn.commit()

    def _get_metadata(self, key: str) -> "str | None":
        """Synchronous implementation of get_metadata."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = :key"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"key": key})
                row = cursor.fetchone()
                return str(row[0]) if row is not None else None
        except OracleDatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _set_metadata(self, key: str, value: str) -> None:
        """Synchronous implementation of set_metadata."""
        sql = f"""
        MERGE INTO {self._metadata_table} target
        USING (SELECT :key AS key, :value AS value FROM DUAL) source
        ON (target.key = source.key)
        WHEN MATCHED THEN
            UPDATE SET target.value = source.value
        WHEN NOT MATCHED THEN
            INSERT (key, value)
            VALUES (source.key, source.value)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"key": key, "value": value})
            conn.commit()


class OracleAsyncADKMemoryStore(BaseAsyncADKMemoryStore["OracleAsyncConfig"]):
    """Oracle ADK memory store using async oracledb driver."""

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None
        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

    async def create_tables(self) -> None:
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", :owner_id" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, event_id, author{owner_column},
            timestamp, content_json, content_text, metadata_json, inserted_at
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :event_id, :author{owner_param},
            :timestamp, :content_json, :content_text, :metadata_json, :inserted_at
        )
        """

        inserted_count = 0
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            for entry in entries:
                content_json = await self._serialize_json_field(entry["content_json"])
                metadata_json = await self._serialize_json_field(entry["metadata_json"])
                params = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "event_id": entry["event_id"],
                    "author": entry["author"],
                    "timestamp": entry["timestamp"],
                    "content_json": content_json,
                    "content_text": entry["content_text"],
                    "metadata_json": metadata_json,
                    "inserted_at": entry["inserted_at"],
                }
                if self._owner_id_column_name:
                    params["owner_id"] = str(owner_id) if owner_id is not None else None
                if await self._execute_insert_entry(cursor, sql, params):
                    inserted_count += 1
            await conn.commit()

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                return await self._search_entries_fts(query, app_name, user_id, effective_limit)
            return await self._search_entries_simple(query, app_name, user_id, effective_limit)
        except OracleDatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = :session_id"
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": session_id})
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
        """
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"days": days})
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def _detect_json_storage_type(self) -> "JSONStorageType":
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = await self._get_version_info()
        self._json_storage_type = storage_type_from_version(version_info)
        return self._json_storage_type

    async def _get_version_info(self) -> "OracleVersionInfo | None":
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        async with self._config.provide_session() as driver:
            dictionary = OracledbAsyncDataDictionary()
            self._oracle_version_info = await dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    async def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        if value is None:
            return None

        storage_type = await self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)
        return to_json(value, as_bytes=True)

    async def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        if data is None:
            return None

        if is_async_readable(data) or is_readable(data):
            data = await _read_lob_async(data)

        return _extract_json_value(data)

    async def _get_create_memory_table_sql(self) -> str:
        storage_type = await self._detect_json_storage_type()
        return self._get_create_memory_table_sql_for_type(storage_type)

    def _get_create_memory_table_sql_for_type(self, storage_type: "JSONStorageType") -> str:
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content_json JSON,
                metadata_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content_json BLOB CHECK (content_json IS JSON),
                metadata_json BLOB CHECK (metadata_json IS JSON)
            """
        else:
            json_columns = """
                content_json BLOB,
                metadata_json BLOB
            """

        owner_id_line = f",\n                {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "memory",
            in_memory=self._in_memory,
            hash_partition_key="id",
            range_partition_key="inserted_at",
        )

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_fts
                ON {self._memory_table}(content_text) INDEXTYPE IS CTXSYS.CONTEXT';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
            """

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._memory_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                event_id VARCHAR2(128) NOT NULL UNIQUE,
                author VARCHAR2(256){owner_id_line},
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                {json_columns},
                content_text CLOB NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_app_user_time
                ON {self._memory_table}(app_name, user_id, timestamp DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_session
                ON {self._memory_table}(session_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_app_user_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._memory_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    async def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            await cursor.execute(sql, params)
        except OracleDatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_DUPLICATE_KEY_ERROR:
                return False
            raise
        return True

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at,
                   SCORE(1) AS score
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND CONTAINS(content_text, :query, 1) > 0
            ORDER BY score DESC, timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        params = {"app_name": app_name, "user_id": user_id, "query": query, "limit": limit}
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return await self._rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND LOWER(content_text) LIKE :pattern
            ORDER BY timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        pattern = f"%{query.lower()}%"
        params = {"app_name": app_name, "user_id": user_id, "pattern": pattern, "limit": limit}
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return await self._rows_to_records(rows)

    async def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = await self._deserialize_json_field(row[7]) if row[7] is not None else {}
            metadata_json = await self._deserialize_json_field(row[9])
            content_text = row[8]
            if is_async_readable(content_text) or is_readable(content_text):
                content_text = await _read_lob_async(content_text)
            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": row[6],
                "content_json": cast("dict[str, Any]", content_json),
                "content_text": str(content_text),
                "metadata_json": metadata_json,
                "inserted_at": row[10],
            })
        return records


class OracleSyncADKMemoryStore(BaseAsyncADKMemoryStore["OracleSyncConfig"]):
    """Oracle ADK memory store using sync oracledb driver."""

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None
        adk_config = config.extension_config.get("adk", {})
        self._in_memory = bool(adk_config.get("in_memory", False))

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return await async_(self._insert_memory_entries)(entries, owner_id)

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return await async_(self._search_entries)(query, app_name, user_id, limit)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)

    def _detect_json_storage_type(self) -> "JSONStorageType":
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = self._get_version_info()
        self._json_storage_type = storage_type_from_version(version_info)
        return self._json_storage_type

    def _get_version_info(self) -> "OracleVersionInfo | None":
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        with self._config.provide_session() as driver:
            dictionary = OracledbSyncDataDictionary()
            self._oracle_version_info = dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        if value is None:
            return None

        storage_type = self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)
        return to_json(value, as_bytes=True)

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        if data is None:
            return None

        if is_readable(data):
            data = _read_lob_sync(data)

        return _extract_json_value(data)

    async def _get_create_memory_table_sql(self) -> str:
        storage_type = self._detect_json_storage_type()
        return self._get_create_memory_table_sql_for_type(storage_type)

    def _get_create_memory_table_sql_for_type(self, storage_type: "JSONStorageType") -> str:
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content_json JSON,
                metadata_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content_json BLOB CHECK (content_json IS JSON),
                metadata_json BLOB CHECK (metadata_json IS JSON)
            """
        else:
            json_columns = """
                content_json BLOB,
                metadata_json BLOB
            """

        owner_id_line = f",\n                {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        table_clauses = _oracle_table_feature_clauses(
            self._config,
            "memory",
            in_memory=self._in_memory,
            hash_partition_key="id",
            range_partition_key="inserted_at",
        )

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_fts
                ON {self._memory_table}(content_text) INDEXTYPE IS CTXSYS.CONTEXT';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
            """

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._memory_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                event_id VARCHAR2(128) NOT NULL UNIQUE,
                author VARCHAR2(256){owner_id_line},
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                {json_columns},
                content_text CLOB NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){table_clauses}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_app_user_time
                ON {self._memory_table}(app_name, user_id, timestamp DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_session
                ON {self._memory_table}(session_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_app_user_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._memory_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_memory_table_sql)())

    def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            cursor.execute(sql, params)
        except OracleDatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_DUPLICATE_KEY_ERROR:
                return False
            raise
        return True

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", :owner_id" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, event_id, author{owner_column},
            timestamp, content_json, content_text, metadata_json, inserted_at
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :event_id, :author{owner_param},
            :timestamp, :content_json, :content_text, :metadata_json, :inserted_at
        )
        """

        inserted_count = 0
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            for entry in entries:
                content_json = self._serialize_json_field(entry["content_json"])
                metadata_json = self._serialize_json_field(entry["metadata_json"])
                params = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "event_id": entry["event_id"],
                    "author": entry["author"],
                    "timestamp": entry["timestamp"],
                    "content_json": content_json,
                    "content_text": entry["content_text"],
                    "metadata_json": metadata_json,
                    "inserted_at": entry["inserted_at"],
                }
                if self._owner_id_column_name:
                    params["owner_id"] = str(owner_id) if owner_id is not None else None
                if self._execute_insert_entry(cursor, sql, params):
                    inserted_count += 1
            conn.commit()

        return inserted_count

    def _search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                return self._search_entries_fts(query, app_name, user_id, effective_limit)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except OracleDatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at,
                   SCORE(1) AS score
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND CONTAINS(content_text, :query, 1) > 0
            ORDER BY score DESC, timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        params = {"app_name": app_name, "user_id": user_id, "query": query, "limit": limit}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return self._rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND LOWER(content_text) LIKE :pattern
            ORDER BY timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        pattern = f"%{query.lower()}%"
        params = {"app_name": app_name, "user_id": user_id, "pattern": pattern, "limit": limit}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return self._rows_to_records(rows)

    def _delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = :session_id"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": session_id})
            conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def _delete_entries_older_than(self, days: int) -> int:
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"days": days})
            conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = self._deserialize_json_field(row[7]) if row[7] is not None else {}
            metadata_json = self._deserialize_json_field(row[9])
            content_text = row[8]
            if is_readable(content_text):
                content_text = _read_lob_sync(content_text)
            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": row[6],
                "content_json": cast("dict[str, Any]", content_json),
                "content_text": str(content_text),
                "metadata_json": metadata_json,
                "inserted_at": row[10],
            })
        return records


def _coerce_decimal_values(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _coerce_decimal_values(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_coerce_decimal_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_coerce_decimal_values(item) for item in value)
    if isinstance(value, set):
        return {_coerce_decimal_values(item) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_coerce_decimal_values(item) for item in value)
    return value


def _storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    """Determine JSON storage type based on Oracle version metadata."""

    if version_info and version_info.supports_native_json():
        logger.debug("Detected Oracle %s with compatible >= 20, using JSON_NATIVE", version_info)
        return JSONStorageType.JSON_NATIVE

    if version_info and version_info.supports_json_blob():
        logger.debug("Detected Oracle %s, using BLOB_JSON (recommended)", version_info)
        return JSONStorageType.BLOB_JSON

    if version_info:
        logger.debug("Detected Oracle %s (pre-12c), using BLOB_PLAIN", version_info)
        return JSONStorageType.BLOB_PLAIN

    logger.warning("Oracle version could not be detected; defaulting to BLOB_JSON storage")
    return JSONStorageType.BLOB_JSON


def _oracle_text_value(value: Any) -> str:
    """Normalize Oracle VARCHAR2 values back to Python strings.

    Oracle stores empty strings as ``NULL``. The ADK event contract allows
    empty strings for fields like ``invocation_id``, so reads coerce ``NULL``
    back to ``""``.
    """
    return "" if value is None else str(value)


def _extract_json_value(data: Any) -> "dict[str, Any]":
    if isinstance(data, dict):
        return cast("dict[str, Any]", coerce_decimal_values(data))
    if isinstance(data, bytes):
        return from_json(data)  # type: ignore[no-any-return]
    if isinstance(data, str):
        return from_json(data)  # type: ignore[no-any-return]
    return from_json(str(data))  # type: ignore[no-any-return]


def _event_data_column_ddl(storage_type: JSONStorageType) -> str:
    """Return the DDL fragment for the event_data column."""
    if storage_type == JSONStorageType.JSON_NATIVE:
        return "event_data JSON NOT NULL"
    if storage_type == JSONStorageType.BLOB_JSON:
        return "event_data BLOB CHECK (event_data IS JSON) NOT NULL"
    return "event_data BLOB NOT NULL"


def _json_column_ddl(column_name: str, storage_type: JSONStorageType) -> str:
    """Return an Oracle JSON column DDL fragment for the configured storage type."""
    if storage_type == JSONStorageType.JSON_NATIVE:
        return f"{column_name} JSON NOT NULL"
    if storage_type == JSONStorageType.BLOB_JSON:
        return f"{column_name} BLOB CHECK ({column_name} IS JSON) NOT NULL"
    return f"{column_name} BLOB NOT NULL"


def _get_oracle_adk_config(config: Any) -> dict[str, Any]:
    adk_config = config.extension_config.get("adk", {})
    if isinstance(adk_config, dict):
        return adk_config
    return {}


def _validate_oracle_identifier(value: str, label: str) -> str:
    if not value or not (value[0].isalpha() or value[0] == "_"):
        msg = f"Invalid Oracle {label}: {value!r}"
        raise ValueError(msg)
    if any(not (char.isalnum() or char == "_") for char in value):
        msg = f"Invalid Oracle {label}: {value!r}"
        raise ValueError(msg)
    return value


def _oracle_compression_clause(adk_config: dict[str, Any]) -> str:
    compression = adk_config.get("compression")
    if not isinstance(compression, dict) or not compression.get("enabled"):
        return ""

    algorithm = str(compression.get("algorithm") or "advanced").lower()
    try:
        return ORACLE_COMPRESSION_CLAUSES[algorithm]
    except KeyError as exc:
        supported = ", ".join(sorted(ORACLE_COMPRESSION_CLAUSES))
        msg = f"Unsupported Oracle ADK compression algorithm {algorithm!r}. Supported values: {supported}"
        raise ValueError(msg) from exc


def _oracle_hash_partition_clause(partitioning: dict[str, Any], partition_key: str) -> str:
    partition_count = partitioning.get(
        "partition_count", partitioning.get("partitions", ORACLE_DEFAULT_HASH_PARTITIONS)
    )
    if not isinstance(partition_count, int) or partition_count < ORACLE_MIN_HASH_PARTITIONS:
        msg = "Oracle ADK hash partitioning requires partition_count >= 2"
        raise ValueError(msg)
    return f"PARTITION BY HASH ({partition_key}) PARTITIONS {partition_count}"


def _oracle_range_partition_clause(partitioning: dict[str, Any], partition_key: str) -> str:
    interval = str(partitioning.get("interval") or "month").lower()
    interval_sql = ORACLE_RANGE_INTERVALS.get(interval)
    if interval_sql is None:
        supported = ", ".join(sorted(ORACLE_RANGE_INTERVALS))
        msg = f"Unsupported Oracle ADK range partition interval {interval!r}. Supported values: {supported}"
        raise ValueError(msg)

    initial_less_than = str(partitioning.get("initial_less_than") or "TIMESTAMP '2000-01-01 00:00:00'")
    return (
        f"PARTITION BY RANGE ({partition_key}) INTERVAL ({interval_sql}) "
        f"(PARTITION p_initial VALUES LESS THAN ({initial_less_than}))"
    )


def _oracle_partition_clause(
    adk_config: dict[str, Any], table_kind: str, hash_partition_key: str, range_partition_key: str
) -> str:
    partitioning = adk_config.get("partitioning")
    if not isinstance(partitioning, dict):
        return ""

    strategy = str(partitioning.get("strategy") or "").lower()
    if not strategy:
        return ""

    table_key = partitioning.get(f"{table_kind}_partition_key")
    configured_key = table_key if table_key is not None else partitioning.get("partition_key")
    if strategy == "hash":
        partition_key = _validate_oracle_identifier(str(configured_key or hash_partition_key), "partition key")
        return _oracle_hash_partition_clause(partitioning, partition_key)
    if strategy == "range":
        partition_key = _validate_oracle_identifier(str(configured_key or range_partition_key), "partition key")
        return _oracle_range_partition_clause(partitioning, partition_key)

    msg = f"Unsupported Oracle ADK partitioning strategy {strategy!r}. Supported values: hash, range"
    raise ValueError(msg)


def _oracle_table_options_clause(adk_config: dict[str, Any], table_kind: str) -> str:
    option_key = "events_table_options" if table_kind == "events" else f"{table_kind}_table_options"
    options = adk_config.get(option_key)
    return str(options).strip() if options else ""


def _oracle_table_feature_clauses(
    config: Any, table_kind: str, *, in_memory: bool, hash_partition_key: str, range_partition_key: str
) -> str:
    adk_config = _get_oracle_adk_config(config)
    clauses = [
        clause
        for clause in (
            _oracle_compression_clause(adk_config),
            "INMEMORY PRIORITY HIGH" if in_memory else "",
            _oracle_table_options_clause(adk_config, table_kind),
            _oracle_partition_clause(adk_config, table_kind, hash_partition_key, range_partition_key),
        )
        if clause
    ]
    if not clauses:
        return ""
    return " " + " ".join(clauses).replace("'", "''")


async def _read_lob_async(data: Any) -> Any:
    if is_async_readable(data):
        return await data.read()
    if is_readable(data):
        return data.read()
    return data


def _read_lob_sync(data: Any) -> Any:
    if is_readable(data):
        return data.read()
    return data
