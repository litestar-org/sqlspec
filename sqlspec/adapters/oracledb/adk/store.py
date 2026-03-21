"""Oracle ADK store for Google Agent Development Kit session/event storage."""

from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, cast

import oracledb

from sqlspec import SQL
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
    from datetime import datetime

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


class JSONStorageType(str, Enum):
    """JSON storage type based on Oracle version."""

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob_plain"


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


def coerce_decimal_values(value: Any) -> Any:
    return _coerce_decimal_values(value)


def storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    return _storage_type_from_version(version_info)


def _event_json_column_ddl(storage_type: JSONStorageType) -> str:
    """Return the DDL fragment for the event_json column.

    For JSON_NATIVE (Oracle 21c+) we use the native JSON type.
    For older versions we use BLOB since Oracle recommends BLOB over CLOB for
    JSON storage. BLOB_JSON gets a CHECK constraint; BLOB_PLAIN does not.
    """
    if storage_type == JSONStorageType.JSON_NATIVE:
        return "event_json JSON NOT NULL"
    if storage_type == JSONStorageType.BLOB_JSON:
        return "event_json BLOB CHECK (event_json IS JSON) NOT NULL"
    return "event_json BLOB NOT NULL"


class OracleAsyncADKStore(BaseAsyncADKStore["OracleAsyncConfig"]):
    """Oracle async ADK store using oracledb async driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb async driver. Provides:
    - Session state management with version-specific JSON storage
    - Full-fidelity event storage via ``event_json`` column
    - Atomic ``append_event_and_update_state`` for durable session mutations
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleAsyncConfig with extension_config["adk"] settings.

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - event_json stored as JSON (21c+) or BLOB (older versions)
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
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
            - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)
        """
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None

        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

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

    async def _serialize_event_json(self, event_json: Any) -> "str | bytes":
        """Serialize event_json to the configured Oracle JSON storage format."""
        storage_type = await self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(event_json)
        return to_json(event_json, as_bytes=True)

    async def _read_event_json(self, data: Any) -> str:
        """Read event_json from database, handling LOB types.

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
            SQL statement to create adk_sessions table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            state_column = "state JSON NOT NULL"
        elif storage_type == JSONStorageType.BLOB_JSON:
            state_column = "state BLOB CHECK (state IS JSON) NOT NULL"
        else:
            state_column = "state BLOB NOT NULL"

        owner_id_column_sql = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._session_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL{owner_id_column_sql}
            ){inmemory_clause}';
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
        author, timestamp, and event_json. The event_json column stores the full
        ADK Event as JSON (21c+) or BLOB (older versions).

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_events table.
        """
        event_json_col = _event_json_column_ddl(storage_type)
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                session_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256) NOT NULL,
                author VARCHAR2(256) NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {event_json_col},
                CONSTRAINT fk_{self._events_table}_session FOREIGN KEY (session_id)
                    REFERENCES {self._session_table}(id) ON DELETE CASCADE
            ){inmemory_clause}';
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

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            Oracle automatically drops indexes when dropping tables.
        """
        return [
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

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            Oracle returns datetime objects for TIMESTAMP columns.
            State is deserialized using version-appropriate format.
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
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
        except oracledb.DatabaseError as e:
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
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record with 5 keys: session_id, invocation_id,
                author, timestamp, event_json.
        """
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_json
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
                    "event_json": await self._serialize_event_json(event_record["event_json"]),
                },
            )
            await conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state.

        Both the event insert and session state update are executed within a
        single transaction so they succeed or fail together.

        Args:
            event_record: Event record with 5 keys: session_id, invocation_id,
                author, timestamp, event_json.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_json
        )
        """

        state_data = await self._serialize_state(state)
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
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
                    "event_json": await self._serialize_event_json(event_record["event_json"]),
                },
            )
            await cursor.execute(update_sql, {"state": state_data, "id": session_id})
            await conn.commit()

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
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                results = []
                for row in rows:
                    event_json_str = await self._read_event_json(row[4])

                    results.append(
                        EventRecord(
                            session_id=row[0],
                            invocation_id=row[1],
                            author=row[2],
                            timestamp=row[3],
                            event_json=from_json(event_json_str) if isinstance(event_json_str, str) else event_json_str,
                        )
                    )
                return results
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise


class OracleSyncADKStore(BaseAsyncADKStore["OracleSyncConfig"]):
    """Oracle synchronous ADK store using oracledb sync driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb synchronous driver. Provides:
    - Session state management with version-specific JSON storage
    - Full-fidelity event storage via ``event_json`` column
    - Atomic ``create_event_and_update_state`` for durable session mutations
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleSyncConfig with extension_config["adk"] settings.

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - event_json stored as JSON (21c+) or BLOB (older versions)
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
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
            - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)
        """
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None

        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

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

    def _serialize_event_json(self, event_json: Any) -> "str | bytes":
        """Serialize event_json to the configured Oracle JSON storage format."""
        storage_type = self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(event_json)
        return to_json(event_json, as_bytes=True)

    def _read_event_json(self, data: Any) -> str:
        """Read event_json from database, handling LOB types.

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
            SQL statement to create adk_sessions table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            state_column = "state JSON NOT NULL"
        elif storage_type == JSONStorageType.BLOB_JSON:
            state_column = "state BLOB CHECK (state IS JSON) NOT NULL"
        else:
            state_column = "state BLOB NOT NULL"

        owner_id_column_sql = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._session_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                {state_column},
                create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL{owner_id_column_sql}
            ){inmemory_clause}';
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
        author, timestamp, and event_json. The event_json column stores the full
        ADK Event as JSON (21c+) or BLOB (older versions).

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_events table.
        """
        event_json_col = _event_json_column_ddl(storage_type)
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                session_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256) NOT NULL,
                author VARCHAR2(256) NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {event_json_col},
                CONSTRAINT fk_{self._events_table}_session FOREIGN KEY (session_id)
                    REFERENCES {self._session_table}(id) ON DELETE CASCADE
            ){inmemory_clause}';
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

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            Oracle automatically drops indexes when dropping tables.
        """
        return [
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

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

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

        return self._get_session(session_id)

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

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
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID."""
        return await async_(self._get_session)(session_id)

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

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await async_(self._update_session_state)(session_id, state)

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

    async def delete_session(self, session_id: str) -> None:
        """Delete session and associated events."""
        await async_(self._delete_session)(session_id)

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
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return await async_(self._list_sessions)(app_name, user_id)

    def _append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically create an event and update the session's durable state.

        Both the event insert and session state update are executed within a
        single transaction so they succeed or fail together.

        Args:
            event_record: Event record with 5 keys: session_id, invocation_id,
                author, timestamp, event_json.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_json
        )
        """

        state_data = self._serialize_state(state)
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = :state, update_time = SYSTIMESTAMP
        WHERE id = :id
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
                    "event_json": self._serialize_event_json(event_record["event_json"]),
                },
            )
            cursor.execute(update_sql, {"state": state_data, "id": session_id})
            conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state."""
        await async_(self._append_event_and_update_state)(event_record, session_id, state)

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
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                results = []
                for row in rows:
                    event_json_str = self._read_event_json(row[4])

                    results.append(
                        EventRecord(
                            session_id=row[0],
                            invocation_id=row[1],
                            author=row[2],
                            timestamp=row[3],
                            event_json=from_json(event_json_str) if isinstance(event_json_str, str) else event_json_str,
                        )
                    )
                return results
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return await async_(self._get_events)(session_id, after_timestamp, limit)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (
            :session_id, :invocation_id, :author, :timestamp, :event_json
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
                    "event_json": self._serialize_event_json(event_record["event_json"]),
                },
            )
            conn.commit()

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)


ORACLE_DUPLICATE_KEY_ERROR: Final = 1


def _extract_json_value(data: Any) -> "dict[str, Any]":
    if isinstance(data, dict):
        return cast("dict[str, Any]", coerce_decimal_values(data))
    if isinstance(data, bytes):
        return from_json(data)  # type: ignore[no-any-return]
    if isinstance(data, str):
        return from_json(data)  # type: ignore[no-any-return]
    return from_json(str(data))  # type: ignore[no-any-return]


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


class OracleAsyncADKMemoryStore(BaseAsyncADKMemoryStore["OracleAsyncConfig"]):
    """Oracle ADK memory store using async oracledb driver."""

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None
        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

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
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

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
            ){inmemory_clause}';
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

    async def create_tables(self) -> None:
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            await cursor.execute(sql, params)
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_DUPLICATE_KEY_ERROR:
                return False
            raise
        return True

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
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

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
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

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
            ){inmemory_clause}';
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

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            cursor.execute(sql, params)
        except oracledb.DatabaseError as exc:
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

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return await async_(self._insert_memory_entries)(entries, owner_id)

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
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return await async_(self._search_entries)(query, app_name, user_id, limit)

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

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

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

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)

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
