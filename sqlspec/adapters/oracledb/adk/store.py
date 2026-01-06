"""Oracle ADK store for Google Agent Development Kit session/event storage."""

from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, cast

import oracledb

from sqlspec import SQL
from sqlspec.adapters.oracledb.data_dictionary import (
    OracleAsyncDataDictionary,
    OracleSyncDataDictionary,
    OracleVersionInfo,
)
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import is_async_readable, is_readable

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

logger = get_logger("adapters.oracledb.adk.store")

__all__ = (
    "JSONStorageType",
    "OracleAsyncADKStore",
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


def _to_oracle_bool(value: "bool | None") -> "int | None":
    """Convert Python boolean to Oracle NUMBER(1).

    Args:
        value: Python boolean value or None.

    Returns:
        1 for True, 0 for False, None for None.
    """
    if value is None:
        return None
    return 1 if value else 0


def _from_oracle_bool(value: "int | None") -> "bool | None":
    """Convert Oracle NUMBER(1) to Python boolean.

    Args:
        value: Oracle NUMBER value (0, 1, or None).

    Returns:
        Python boolean or None.
    """
    if value is None:
        return None
    return bool(value)


def _coerce_bytes_payload(value: Any) -> bytes:
    """Coerce a LOB payload into bytes."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


class OracleAsyncADKStore(BaseAsyncADKStore["OracleAsyncConfig"]):
    """Oracle async ADK store using oracledb async driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb async driver. Provides:
    - Session state management with version-specific JSON storage
    - Event history tracking with BLOB-serialized actions
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleAsyncConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig
        from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore

        config = OracleAsyncConfig(
            connection_config={"dsn": "oracle://..."},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id NUMBER(10) REFERENCES tenants(id)"
                }
            }
        )
        store = OracleAsyncADKStore(config)
        await store.create_tables()

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - BLOB for pre-serialized actions from Google ADK
        - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
        - NUMBER(1) for booleans (0/1/NULL)
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
            - Oracle 12c+: BLOB with IS JSON constraint (preferred)
            - Oracle 11g and earlier: BLOB without constraint

            BLOB is preferred over CLOB for 12c+ as per Oracle recommendations.
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
            dictionary = OracleAsyncDataDictionary()
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

    async def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        """Serialize optional JSON field for event storage.

        Args:
            value: Value to serialize (dict or None).

        Returns:
            Serialized JSON or None.
        """
        if value is None:
            return None

        storage_type = await self._detect_json_storage_type()

        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)

        return to_json(value, as_bytes=True)

    async def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize optional JSON field from database.

        Args:
            data: Data from database (may be LOB, str, bytes, dict, or None).

        Returns:
            Deserialized dictionary or None.

        Notes:
            Oracle JSON type may return dict directly.
        """
        if data is None:
            return None

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

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_events table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content JSON,
                grounding_metadata JSON,
                custom_metadata JSON,
                long_running_tool_ids_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content BLOB CHECK (content IS JSON),
                grounding_metadata BLOB CHECK (grounding_metadata IS JSON),
                custom_metadata BLOB CHECK (custom_metadata IS JSON),
                long_running_tool_ids_json BLOB CHECK (long_running_tool_ids_json IS JSON)
            """
        else:
            json_columns = """
                content BLOB,
                grounding_metadata BLOB,
                custom_metadata BLOB,
                long_running_tool_ids_json BLOB
            """

        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256),
                author VARCHAR2(256),
                actions BLOB,
                branch VARCHAR2(256),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {json_columns},
                partial NUMBER(1),
                turn_complete NUMBER(1),
                interrupted NUMBER(1),
                error_code VARCHAR2(256),
                error_message VARCHAR2(1024),
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

        logger.debug("Created ADK tables: %s, %s", self._session_table, self._events_table)

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
            event_record: Event record to store.

        Notes:
            Uses SYSTIMESTAMP for timestamp if not provided.
            JSON fields are serialized using version-appropriate format.
            Boolean fields are converted to NUMBER(1).
        """
        content_data = await self._serialize_json_field(event_record.get("content"))
        grounding_metadata_data = await self._serialize_json_field(event_record.get("grounding_metadata"))
        custom_metadata_data = await self._serialize_json_field(event_record.get("custom_metadata"))

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :invocation_id, :author, :actions,
            :long_running_tool_ids_json, :branch, :timestamp, :content,
            :grounding_metadata, :custom_metadata, :partial, :turn_complete,
            :interrupted, :error_code, :error_message
        )
        """

        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(
                sql,
                {
                    "id": event_record["id"],
                    "session_id": event_record["session_id"],
                    "app_name": event_record["app_name"],
                    "user_id": event_record["user_id"],
                    "invocation_id": event_record["invocation_id"],
                    "author": event_record["author"],
                    "actions": event_record["actions"],
                    "long_running_tool_ids_json": event_record.get("long_running_tool_ids_json"),
                    "branch": event_record.get("branch"),
                    "timestamp": event_record["timestamp"],
                    "content": content_data,
                    "grounding_metadata": grounding_metadata_data,
                    "custom_metadata": custom_metadata_data,
                    "partial": _to_oracle_bool(event_record.get("partial")),
                    "turn_complete": _to_oracle_bool(event_record.get("turn_complete")),
                    "interrupted": _to_oracle_bool(event_record.get("interrupted")),
                    "error_code": event_record.get("error_code"),
                    "error_message": event_record.get("error_message"),
                },
            )
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

        Notes:
            Uses index on (session_id, timestamp ASC).
            JSON fields deserialized using version-appropriate format.
            Converts BLOB actions to bytes and NUMBER(1) booleans to Python bool.
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
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
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
                    actions_blob = row[6]
                    if is_async_readable(actions_blob):
                        actions_data = await actions_blob.read()
                    elif is_readable(actions_blob):
                        actions_data = actions_blob.read()
                    else:
                        actions_data = actions_blob

                    content = await self._deserialize_json_field(row[10])
                    grounding_metadata = await self._deserialize_json_field(row[11])
                    custom_metadata = await self._deserialize_json_field(row[12])

                    results.append(
                        EventRecord(
                            id=row[0],
                            session_id=row[1],
                            app_name=row[2],
                            user_id=row[3],
                            invocation_id=row[4],
                            author=row[5],
                            actions=_coerce_bytes_payload(actions_data),
                            long_running_tool_ids_json=row[7],
                            branch=row[8],
                            timestamp=row[9],
                            content=content,
                            grounding_metadata=grounding_metadata,
                            custom_metadata=custom_metadata,
                            partial=_from_oracle_bool(row[13]),
                            turn_complete=_from_oracle_bool(row[14]),
                            interrupted=_from_oracle_bool(row[15]),
                            error_code=row[16],
                            error_message=row[17],
                        )
                    )
                return results
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise


class OracleSyncADKStore(BaseSyncADKStore["OracleSyncConfig"]):
    """Oracle synchronous ADK store using oracledb sync driver.

    Implements session and event storage for Google Agent Development Kit
    using Oracle Database via the python-oracledb synchronous driver. Provides:
    - Session state management with version-specific JSON storage
    - Event history tracking with BLOB-serialized actions
    - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using MERGE statement

    Args:
        config: OracleSyncConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.oracledb import OracleSyncConfig
        from sqlspec.adapters.oracledb.adk import OracleSyncADKStore

        config = OracleSyncConfig(
            connection_config={"dsn": "oracle://..."},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "account_id NUMBER(19) REFERENCES accounts(id)"
                }
            }
        )
        store = OracleSyncADKStore(config)
        store.create_tables()

    Notes:
        - JSON storage type detected based on Oracle version (21c+, 12c+, legacy)
        - BLOB for pre-serialized actions from Google ADK
        - TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
        - NUMBER(1) for booleans (0/1/NULL)
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

    def _get_create_sessions_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL for sessions table.

        Auto-detects optimal JSON storage type based on Oracle version.
        Result is cached to minimize database queries.
        """
        storage_type = self._detect_json_storage_type()
        return self._get_create_sessions_table_sql_for_type(storage_type)

    def _get_create_events_table_sql(self) -> str:
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
            - Oracle 12c+: BLOB with IS JSON constraint (preferred)
            - Oracle 11g and earlier: BLOB without constraint

            BLOB is preferred over CLOB for 12c+ as per Oracle recommendations.
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
            dictionary = OracleSyncDataDictionary()
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

    def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        """Serialize optional JSON field for event storage.

        Args:
            value: Value to serialize (dict or None).

        Returns:
            Serialized JSON or None.
        """
        if value is None:
            return None

        storage_type = self._detect_json_storage_type()

        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)

        return to_json(value, as_bytes=True)

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize optional JSON field from database.

        Args:
            data: Data from database (may be LOB, str, bytes, dict, or None).

        Returns:
            Deserialized dictionary or None.

        Notes:
            Oracle JSON type may return dict directly.
        """
        if data is None:
            return None

        if is_readable(data):
            data = data.read()

        if isinstance(data, dict):
            return cast("dict[str, Any]", _coerce_decimal_values(data))

        if isinstance(data, bytes):
            return from_json(data)  # type: ignore[no-any-return]

        if isinstance(data, str):
            return from_json(data)  # type: ignore[no-any-return]

        return from_json(str(data))  # type: ignore[no-any-return]

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

        Args:
            storage_type: JSON storage type to use.

        Returns:
            SQL statement to create adk_events table.
        """
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content JSON,
                grounding_metadata JSON,
                custom_metadata JSON,
                long_running_tool_ids_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content BLOB CHECK (content IS JSON),
                grounding_metadata BLOB CHECK (grounding_metadata IS JSON),
                custom_metadata BLOB CHECK (custom_metadata IS JSON),
                long_running_tool_ids_json BLOB CHECK (long_running_tool_ids_json IS JSON)
            """
        else:
            json_columns = """
                content BLOB,
                grounding_metadata BLOB,
                custom_metadata BLOB,
                long_running_tool_ids_json BLOB
            """

        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256),
                author VARCHAR2(256),
                actions BLOB,
                branch VARCHAR2(256),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                {json_columns},
                partial NUMBER(1),
                turn_complete NUMBER(1),
                interrupted NUMBER(1),
                error_code VARCHAR2(256),
                error_message VARCHAR2(1024),
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

    def create_tables(self) -> None:
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

        logger.debug("Created ADK tables: %s, %s", self._session_table, self._events_table)

    def create_session(
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

        return self.get_session(session_id)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> "SessionRecord | None":
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

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
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

    def delete_session(self, session_id: str) -> None:
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

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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

    def create_event(
        self,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: "str | None" = None,
        actions: "bytes | None" = None,
        content: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> "EventRecord":
        """Create a new event.

        Args:
            event_id: Unique event identifier.
            session_id: Session identifier.
            app_name: Application name.
            user_id: User identifier.
            author: Event author (user/assistant/system).
            actions: Pickled actions object.
            content: Event content (JSONB/JSON).
            **kwargs: Additional optional fields.

        Returns:
            Created event record.

        Notes:
            Uses SYSTIMESTAMP for timestamp if not provided.
            JSON fields are serialized using version-appropriate format.
            Boolean fields are converted to NUMBER(1).
        """
        content_data = self._serialize_json_field(content)
        grounding_metadata_data = self._serialize_json_field(kwargs.get("grounding_metadata"))
        custom_metadata_data = self._serialize_json_field(kwargs.get("custom_metadata"))

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :invocation_id, :author, :actions,
            :long_running_tool_ids_json, :branch, :timestamp, :content,
            :grounding_metadata, :custom_metadata, :partial, :turn_complete,
            :interrupted, :error_code, :error_message
        )
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                sql,
                {
                    "id": event_id,
                    "session_id": session_id,
                    "app_name": app_name,
                    "user_id": user_id,
                    "invocation_id": kwargs.get("invocation_id"),
                    "author": author,
                    "actions": actions,
                    "long_running_tool_ids_json": kwargs.get("long_running_tool_ids_json"),
                    "branch": kwargs.get("branch"),
                    "timestamp": kwargs.get("timestamp"),
                    "content": content_data,
                    "grounding_metadata": grounding_metadata_data,
                    "custom_metadata": custom_metadata_data,
                    "partial": _to_oracle_bool(kwargs.get("partial")),
                    "turn_complete": _to_oracle_bool(kwargs.get("turn_complete")),
                    "interrupted": _to_oracle_bool(kwargs.get("interrupted")),
                    "error_code": kwargs.get("error_code"),
                    "error_message": kwargs.get("error_message"),
                },
            )
            conn.commit()

        events = self.list_events(session_id)
        for event in events:
            if event["id"] == event_id:
                return event

        msg = f"Failed to retrieve created event {event_id}"
        raise RuntimeError(msg)

    def list_events(self, session_id: str) -> "list[EventRecord]":
        """List events for a session ordered by timestamp.

        Args:
            session_id: Session identifier.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses index on (session_id, timestamp ASC).
            JSON fields deserialized using version-appropriate format.
            Converts BLOB actions to bytes and NUMBER(1) booleans to Python bool.
        """

        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE session_id = :session_id
        ORDER BY timestamp ASC
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"session_id": session_id})
                rows = cursor.fetchall()

                results = []
                for row in rows:
                    actions_blob = row[6]
                    actions_data = actions_blob.read() if is_readable(actions_blob) else actions_blob

                    content = self._deserialize_json_field(row[10])
                    grounding_metadata = self._deserialize_json_field(row[11])
                    custom_metadata = self._deserialize_json_field(row[12])

                    results.append(
                        EventRecord(
                            id=row[0],
                            session_id=row[1],
                            app_name=row[2],
                            user_id=row[3],
                            invocation_id=row[4],
                            author=row[5],
                            actions=_coerce_bytes_payload(actions_data),
                            long_running_tool_ids_json=row[7],
                            branch=row[8],
                            timestamp=row[9],
                            content=content,
                            grounding_metadata=grounding_metadata,
                            custom_metadata=custom_metadata,
                            partial=_from_oracle_bool(row[13]),
                            turn_complete=_from_oracle_bool(row[14]),
                            interrupted=_from_oracle_bool(row[15]),
                            error_code=row[16],
                            error_message=row[17],
                        )
                    )
                return results
        except oracledb.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise
