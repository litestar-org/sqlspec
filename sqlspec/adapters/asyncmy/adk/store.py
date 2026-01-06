"""AsyncMy ADK store for Google Agent Development Kit session/event storage."""

import json
import re
from typing import TYPE_CHECKING, Any, Final

import asyncmy

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

logger = get_logger("adapters.asyncmy.adk.store")

__all__ = ("AsyncmyADKStore",)

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


class AsyncmyADKStore(BaseAsyncADKStore["AsyncmyConfig"]):
    """MySQL/MariaDB ADK store using AsyncMy driver.

    Implements session and event storage for Google Agent Development Kit
    using MySQL/MariaDB via the AsyncMy driver. Provides:
    - Session state management with JSON storage
    - Event history tracking with BLOB-serialized actions
    - Microsecond-precision timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using ON DUPLICATE KEY UPDATE

    Args:
        config: AsyncmyConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.asyncmy import AsyncmyConfig
        from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore

        config = AsyncmyConfig(
            connection_config={"host": "localhost", ...},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
                }
            }
        )
        store = AsyncmyADKStore(config)
        await store.create_tables()

    Notes:
        - MySQL JSON type used (not JSONB) - requires MySQL 5.7.8+
        - TIMESTAMP(6) provides microsecond precision
        - InnoDB engine required for foreign key support
        - State merging handled at application level
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "AsyncmyConfig") -> None:
        """Initialize AsyncMy ADK store.

        Args:
            config: AsyncmyConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        """Parse owner ID column DDL for MySQL FOREIGN KEY syntax.

        MySQL ignores inline REFERENCES syntax in column definitions.
        This method extracts the column definition and creates a separate
        FOREIGN KEY constraint.

        Args:
            column_ddl: Column DDL like "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"

        Returns:
            Tuple of (column_definition, foreign_key_constraint)

        Example:
            Input: "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"
            Output: ("tenant_id BIGINT NOT NULL", "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE")
        """
        references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)

        if not references_match:
            return (column_ddl.strip(), "")

        col_def = column_ddl[: references_match.start()].strip()
        fk_clause = references_match.group(1).strip()
        col_name = col_def.split()[0]
        fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"

        return (col_def, fk_constraint)

    async def _get_create_sessions_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table with indexes.

        Notes:
            - VARCHAR(128) for IDs and names (sufficient for UUIDs and app names)
            - JSON type for state storage (MySQL 5.7.8+)
            - TIMESTAMP(6) with microsecond precision
            - AUTO-UPDATE on update_time
            - Composite index on (app_name, user_id) for listing
            - Index on update_time DESC for recent session queries
            - Optional owner ID column for multi-tenancy
            - MySQL requires explicit FOREIGN KEY syntax (inline REFERENCES is ignored)
        """
        owner_id_col = ""
        fk_constraint = ""

        if self._owner_id_column_ddl:
            col_def, fk_def = self._parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_col = f"{col_def},"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            {owner_id_col}
            state JSON NOT NULL,
            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{self._session_table}_app_user (app_name, user_id),
            INDEX idx_{self._session_table}_update_time (update_time DESC){fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_create_events_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table with indexes.

        Notes:
            - VARCHAR sizes: id(128), session_id(128), invocation_id(256), author(256),
              branch(256), error_code(256), error_message(1024)
            - BLOB for pickled actions (up to 64KB)
            - JSON for content, grounding_metadata, custom_metadata, long_running_tool_ids_json
            - BOOLEAN for partial, turn_complete, interrupted
            - Foreign key to sessions with CASCADE delete
            - Index on (session_id, timestamp ASC) for ordered event retrieval
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            actions BLOB NOT NULL,
            long_running_tool_ids_json JSON,
            branch VARCHAR(256),
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,
            INDEX idx_{self._events_table}_session (session_id, timestamp ASC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.

        Notes:
            Order matters: drop events table (child) before sessions (parent).
            MySQL automatically drops indexes when dropping tables.
        """
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())
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
            Uses INSERT with UTC_TIMESTAMP(6) for create_time and update_time.
            State is JSON-serialized before insertion.
            If owner_id_column is configured, owner_id must be provided.
        """
        state_json = json.dumps(state)

        params: "tuple[Any", ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, state_json)

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
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
            MySQL returns datetime objects for TIMESTAMP columns.
            JSON is parsed from database storage.
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cursor:
                await cursor.execute(sql, (session_id,))
                row = await cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_json, create_time, update_time = row

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=json.loads(state_json) if isinstance(state_json, str) else state_json,
                    create_time=create_time,
                    update_time=update_time,
                )
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore[reportAttributeAccessIssue][reportAttributeAccessIssue]
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            This replaces the entire state dictionary.
            Uses update_time auto-update trigger.
        """
        state_json = json.dumps(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (state_json, session_id))
            await conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.

        Notes:
            Foreign key constraint ensures events are cascade-deleted.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(sql, (session_id,))
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
        """
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params: "tuple[str", ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cursor:
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    SessionRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=json.loads(row[3]) if isinstance(row[3], str) else row[3],
                        create_time=row[4],
                        update_time=row[5],
                    )
                    for row in rows
                ]
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore[reportAttributeAccessIssue]
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.

        Notes:
            Uses UTC_TIMESTAMP(6) for timestamp if not provided.
            JSON fields are serialized before insertion.
        """
        content_json = json.dumps(event_record.get("content")) if event_record.get("content") else None
        grounding_metadata_json = (
            json.dumps(event_record.get("grounding_metadata")) if event_record.get("grounding_metadata") else None
        )
        custom_metadata_json = (
            json.dumps(event_record.get("custom_metadata")) if event_record.get("custom_metadata") else None
        )

        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                sql,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["actions"],
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    event_record["timestamp"],
                    content_json,
                    grounding_metadata_json,
                    custom_metadata_json,
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ),
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
            Parses JSON fields and converts BLOB actions to bytes.
        """
        where_clauses = ["session_id = %s"]
        params: "list[Any]"= [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

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
            async with self._config.provide_connection() as conn, conn.cursor() as cursor:
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    EventRecord(
                        id=row[0],
                        session_id=row[1],
                        app_name=row[2],
                        user_id=row[3],
                        invocation_id=row[4],
                        author=row[5],
                        actions=bytes(row[6]),
                        long_running_tool_ids_json=row[7],
                        branch=row[8],
                        timestamp=row[9],
                        content=json.loads(row[10]) if row[10] and isinstance(row[10], str) else row[10],
                        grounding_metadata=json.loads(row[11]) if row[11] and isinstance(row[11], str) else row[11],
                        custom_metadata=json.loads(row[12]) if row[12] and isinstance(row[12], str) else row[12],
                        partial=row[13],
                        turn_complete=row[14],
                        interrupted=row[15],
                        error_code=row[16],
                        error_message=row[17],
                    )
                    for row in rows
                ]
        except asyncmy.errors.ProgrammingError as e:  # pyright: ignore[reportAttributeAccessIssue]
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise
