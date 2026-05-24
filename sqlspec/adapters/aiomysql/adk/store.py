"""aiomysql ADK store for Google Agent Development Kit session/event storage."""

import re
from typing import TYPE_CHECKING, Any, Final, cast

import pymysql.err

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor, AiomysqlRawCursor
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.aiomysql.config import AiomysqlConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("AiomysqlADKMemoryStore", "AiomysqlADKStore")

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


def _parse_owner_id_column_for_mysql(column_ddl: str) -> "tuple[str, str]":
    """Parse owner ID column DDL for MySQL FOREIGN KEY syntax.

    Args:
        column_ddl: Column DDL like "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE".

    Returns:
        Tuple of (column_definition, foreign_key_constraint).
    """
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


class AiomysqlADKStore(BaseAsyncADKStore["AiomysqlConfig"]):
    """MySQL/MariaDB ADK store using aiomysql driver.

    Implements session and event storage for Google Agent Development Kit
    using MySQL/MariaDB via the aiomysql driver. Provides:
    - Session state management with JSON storage
    - Full-event JSON storage (single ``event_data`` column)
    - Atomic event-append + state-update in one transaction
    - Microsecond-precision timestamps
    - Foreign key constraints with cascade delete
    - Efficient upserts using ON DUPLICATE KEY UPDATE

    Notes:
        - MySQL JSON type used (not JSONB) - requires MySQL 5.7.8+
        - TIMESTAMP(6) provides microsecond precision
        - InnoDB engine required for foreign key support
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "AiomysqlConfig") -> None:
        """Initialize aiomysql ADK store.

        Args:
            config: AiomysqlConfig instance.
        """
        super().__init__(config)

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())
            await driver.execute_script(await self._get_create_app_states_table_sql())
            await driver.execute_script(await self._get_create_user_states_table_sql())
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
        """
        state_json = to_json(state)

        params: tuple[Any, ...]
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

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
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
        """
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    update_sql = f"UPDATE {self._session_table} SET update_time = UTC_TIMESTAMP(6) WHERE id = %s"
                    await cursor.execute(update_sql, (session_id,))
                    await conn.commit()

                await cursor.execute(sql, (session_id,))
                row = await cursor.fetchone()

                if row is None:
                    return None

                session_id_val, app_name, user_id, state_json, create_time, update_time = row

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=from_json(state_json) if isinstance(state_json, str) else state_json,
                    create_time=create_time,
                    update_time=update_time,
                )
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).
        """
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (state_json, session_id))
            await conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            session_id: Session identifier.
        """
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (session_id,))
            await conn.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.
        """
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params: tuple[str, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    SessionRecord(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=from_json(row[3]) if isinstance(row[3], str) else row[3],
                        create_time=row[4],
                        update_time=row[5],
                    )
                    for row in rows
                ]
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record with 5 keys (session_id, invocation_id,
                author, timestamp, event_data).
        """
        event_data = event_record["event_data"]
        event_data_str = to_json(event_data) if not isinstance(event_data, str) else event_data

        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(
                sql,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_data_str,
                ),
            )
            await conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> SessionRecord:
        """Atomically append an event and update the session's durable state.

        MySQL doesn't support UPDATE...RETURNING; we follow the UPDATE with a
        SELECT inside the same transaction so callers get the refreshed row
        in a single round-trip pair (no separate connection acquisition).

        Args:
            event_record: Event record to store.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot.
        """
        event_data = event_record["event_data"]
        event_data_str = to_json(event_data) if not isinstance(event_data, str) else event_data
        state_json = to_json(state)

        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """

        update_sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(
                insert_sql,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    event_data_str,
                ),
            )
            await cursor.execute(update_sql, (state_json, session_id))
            await cursor.execute(select_sql, (session_id,))
            row = await cursor.fetchone()
            await conn.commit()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        state_value = row[3]
        return SessionRecord(
            id=row[0],
            app_name=row[1],
            user_id=row[2],
            state=from_json(state_value) if isinstance(state_value, str) else state_value,
            create_time=row[4],
            update_time=row[5],
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
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_data
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

                return [
                    EventRecord(
                        session_id=row[0],
                        invocation_id=row[1],
                        author=row[2],
                        timestamp=row[3],
                        event_data=from_json(row[4]) if isinstance(row[4], str) else row[4],
                    )
                    for row in rows
                ]
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (before,))
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (updated_before,))
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (app_name,))
                row = await cursor.fetchone()
                return from_json(row[0]) if row is not None and isinstance(row[0], str) else (row[0] if row else None)
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = %s AND user_id = %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (app_name, user_id))
                row = await cursor.fetchone()
                return from_json(row[0]) if row is not None and isinstance(row[0], str) else (row[0] if row else None)
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (app_name, to_json(state)))
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (app_name, user_id, to_json(state)))
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE `key` = %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (key,))
                row = await cursor.fetchone()
                return row[0] if row is not None else None
        except pymysql.err.ProgrammingError as e:
            if "doesn't exist" in str(e) or e.args[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        sql = f"""
        INSERT INTO {self._metadata_table} (`key`, value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (key, value))
            await conn.commit()

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        """Parse owner ID column DDL for MySQL FOREIGN KEY syntax.

        MySQL ignores inline REFERENCES syntax in column definitions.
        This method extracts the column definition and creates a separate
        FOREIGN KEY constraint.

        Args:
            column_ddl: Column DDL like "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE"

        Returns:
            Tuple of (column_definition, foreign_key_constraint)
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
            SQL statement to create adk_session table with indexes.
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
            SQL statement to create adk_event table with indexes.

        Notes:
            Post clean-break schema: 5 columns only.
            - session_id, invocation_id, author: indexed scalars
            - timestamp: microsecond-precision TIMESTAMP
            - event_data: full Event as native JSON
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(128) NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            event_data JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,
            INDEX idx_{self._events_table}_session (session_id, timestamp ASC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_create_app_states_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for app-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_create_user_states_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for user-scoped state."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            PRIMARY KEY (app_name, user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_create_metadata_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for ADK internal metadata."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            `key` VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    async def _get_seed_metadata_sql(self) -> str:
        """Get MySQL SQL that seeds the ADK schema version metadata row."""
        return f"""
        INSERT IGNORE INTO {self._metadata_table} (`key`, value)
        VALUES ('schema_version', '1')
        """

    def _get_drop_app_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _get_drop_user_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _get_drop_metadata_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for ADK internal metadata."""
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.
        """
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class AiomysqlADKMemoryStore(BaseAsyncADKMemoryStore["AiomysqlConfig"]):
    """MySQL/MariaDB ADK memory store using aiomysql driver."""

    __slots__ = ()

    def __init__(self, config: "AiomysqlConfig") -> None:
        """Initialize aiomysql memory store."""
        super().__init__(config)

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {self._memory_table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        async with self._config.provide_connection() as conn:
            async with AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor:
                for entry in entries:
                    params: tuple[Any, ...]
                    if self._owner_id_column_name:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            owner_id,
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        )
                    else:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry["event_id"],
                            entry["author"],
                            entry["timestamp"],
                            to_json(entry["content_json"]),
                            entry["content_text"],
                            to_json(entry["metadata_json"]),
                            entry["inserted_at"],
                        )
                    await cursor.execute(sql, params)
                    inserted_count += cursor.rowcount
            await conn.commit()
        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        if self._use_fts:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (app_name, user_id, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (app_name, user_id, f"%{query}%", limit_value)

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
            columns = [col[0] for col in cursor.description or []]

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (session_id,))
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)
        """
        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (days,))
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def _get_create_memory_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _parse_owner_id_column_for_mysql(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        fts_index = ""
        if self._use_fts:
            fts_index = f",\n            FULLTEXT INDEX idx_{self._memory_table}_fts (content_text)"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            INDEX idx_{self._memory_table}_app_user_time (app_name, user_id, timestamp),
            INDEX idx_{self._memory_table}_session (session_id){fts_index}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]
