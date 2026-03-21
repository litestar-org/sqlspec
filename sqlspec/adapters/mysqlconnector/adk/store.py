"""MysqlConnector ADK store for Google Agent Development Kit session/event storage."""

import re
from typing import TYPE_CHECKING, Any, Final, cast

import mysql.connector

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = (
    "MysqlConnectorAsyncADKMemoryStore",
    "MysqlConnectorAsyncADKStore",
    "MysqlConnectorSyncADKMemoryStore",
    "MysqlConnectorSyncADKStore",
)

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


def _parse_owner_id_column_for_mysql(column_ddl: str) -> "tuple[str, str]":
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


def _mysql_sessions_ddl(session_table: str, owner_id_column_ddl: "str | None") -> str:
    """Generate shared MySQL sessions CREATE TABLE DDL."""
    owner_id_col = ""
    fk_constraint = ""

    if owner_id_column_ddl:
        col_def, fk_def = _parse_owner_id_column_for_mysql(owner_id_column_ddl)
        owner_id_col = f"{col_def},"
        if fk_def:
            fk_constraint = f",\n            {fk_def}"

    return f"""
    CREATE TABLE IF NOT EXISTS {session_table} (
        id VARCHAR(128) PRIMARY KEY,
        app_name VARCHAR(128) NOT NULL,
        user_id VARCHAR(128) NOT NULL,
        {owner_id_col}
        state JSON NOT NULL,
        create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        INDEX idx_{session_table}_app_user (app_name, user_id),
        INDEX idx_{session_table}_update_time (update_time DESC){fk_constraint}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """


def _mysql_events_ddl(events_table: str, session_table: str) -> str:
    """Generate shared MySQL events CREATE TABLE DDL (post clean-break, 5 columns)."""
    return f"""
    CREATE TABLE IF NOT EXISTS {events_table} (
        session_id VARCHAR(128) NOT NULL,
        invocation_id VARCHAR(256) NOT NULL,
        author VARCHAR(128) NOT NULL,
        timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        event_json JSON NOT NULL,
        FOREIGN KEY (session_id) REFERENCES {session_table}(id) ON DELETE CASCADE,
        INDEX idx_{events_table}_session (session_id, timestamp ASC)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """


class MysqlConnectorAsyncADKStore(BaseAsyncADKStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB ADK store using mysql-connector async driver.

    Provides:
    - Session state management with JSON storage
    - Full-event JSON storage (single ``event_json`` column)
    - Atomic event-append + state-update in one transaction
    - Microsecond-precision timestamps
    - Foreign key constraints with cascade delete

    Notes:
        - Uses ``cast()`` extensively because mysql-connector returns ``Any`` types
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        super().__init__(config)

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        return _parse_owner_id_column_for_mysql(column_ddl)

    async def _get_create_sessions_table_sql(self) -> str:
        return _mysql_sessions_ddl(self._session_table, self._owner_id_column_ddl)

    async def _get_create_events_table_sql(self) -> str:
        return _mysql_events_ddl(self._events_table, self._session_table)

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
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

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, params)
            finally:
                await cursor.close()
            await conn.commit()

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, (session_id,))
                    row = await cursor.fetchone()
                finally:
                    await cursor.close()

                if row is None:
                    return None

                session_id_val, app_name_val, user_id_val, state_json, create_time_val, update_time_val = row

                return SessionRecord(
                    id=cast("str", session_id_val),
                    app_name=cast("str", app_name_val),
                    user_id=cast("str", user_id_val),
                    state=from_json(state_json) if isinstance(state_json, str) else cast("dict[str, Any]", state_json),
                    create_time=cast("datetime", create_time_val),
                    update_time=cast("datetime", update_time_val),
                )
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (state_json, session_id))
            finally:
                await cursor.close()
            await conn.commit()

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (session_id,))
            finally:
                await cursor.close()
            await conn.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, params)
                    rows = await cursor.fetchall()
                finally:
                    await cursor.close()

                return [
                    SessionRecord(
                        id=cast("str", row[0]),
                        app_name=cast("str", row[1]),
                        user_id=cast("str", row[2]),
                        state=from_json(row[3]) if isinstance(row[3], str) else cast("dict[str, Any]", row[3]),
                        create_time=cast("datetime", row[4]),
                        update_time=cast("datetime", row[5]),
                    )
                    for row in rows
                ]
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record with 5 keys (session_id, invocation_id,
                author, timestamp, event_json).
        """
        event_json = event_record["event_json"]
        event_json_str = to_json(event_json) if not isinstance(event_json, str) else event_json

        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(
                    sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_json_str,
                    ),
                )
            finally:
                await cursor.close()
            await conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state.

        The event insert and state update succeed together or fail together
        within a single transaction.

        Args:
            event_record: Event record to store.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot.
        """
        event_json = event_record["event_json"]
        event_json_str = to_json(event_json) if not isinstance(event_json, str) else event_json
        state_json = to_json(state)

        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """

        update_sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(
                    insert_sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_json_str,
                    ),
                )
                await cursor.execute(update_sql, (state_json, session_id))
            finally:
                await cursor.close()
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
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, params)
                    rows = await cursor.fetchall()
                finally:
                    await cursor.close()

                return [
                    EventRecord(
                        session_id=cast("str", row[0]),
                        invocation_id=cast("str", row[1]),
                        author=cast("str", row[2]),
                        timestamp=cast("datetime", row[3]),
                        event_json=from_json(row[4]) if isinstance(row[4], str) else cast("dict[str, Any]", row[4]),
                    )
                    for row in rows
                ]
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise


class MysqlConnectorSyncADKStore(BaseAsyncADKStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB ADK store using mysql-connector sync driver.

    Provides:
    - Session state management with JSON storage
    - Full-event JSON storage (single ``event_json`` column)
    - Atomic event-create + state-update in one transaction
    - Microsecond-precision timestamps
    - Foreign key constraints with cascade delete

    Notes:
        - Uses ``cast()`` extensively because mysql-connector returns ``Any`` types
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        return _parse_owner_id_column_for_mysql(column_ddl)

    async def _get_create_sessions_table_sql(self) -> str:
        return _mysql_sessions_ddl(self._session_table, self._owner_id_column_ddl)

    async def _get_create_events_table_sql(self) -> str:
        return _mysql_events_ddl(self._events_table, self._session_table)

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def _create_tables(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_sessions_table_sql)())
            driver.execute_script(run_(self._get_create_events_table_sql)())

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
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

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
            finally:
                cursor.close()
            conn.commit()

        result = self._get_session(session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (session_id,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()

                if row is None:
                    return None

                session_id_val, app_name_val, user_id_val, state_json, create_time_val, update_time_val = row

                return SessionRecord(
                    id=cast("str", session_id_val),
                    app_name=cast("str", app_name_val),
                    user_id=cast("str", user_id_val),
                    state=from_json(state_json) if isinstance(state_json, str) else cast("dict[str, Any]", state_json),
                    create_time=cast("datetime", create_time_val),
                    update_time=cast("datetime", update_time_val),
                )
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID."""
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        state_json = to_json(state)

        sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (state_json, session_id))
            finally:
                cursor.close()
            conn.commit()

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await async_(self._update_session_state)(session_id, state)

    def _delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
            finally:
                cursor.close()
            conn.commit()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and associated events."""
        await async_(self._delete_session)(session_id)

    def _list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                finally:
                    cursor.close()

                return [
                    SessionRecord(
                        id=cast("str", row[0]),
                        app_name=cast("str", row[1]),
                        user_id=cast("str", row[2]),
                        state=from_json(row[3]) if isinstance(row[3], str) else cast("dict[str, Any]", row[3]),
                        create_time=cast("datetime", row[4]),
                        update_time=cast("datetime", row[5]),
                    )
                    for row in rows
                ]
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return await async_(self._list_sessions)(app_name, user_id)

    def _append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically create an event and update the session's durable state.

        The event insert and state update succeed together or fail together
        within a single transaction.

        Args:
            event_record: Event record to store.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot.
        """
        event_json = event_record["event_json"]
        event_json_str = to_json(event_json) if not isinstance(event_json, str) else event_json
        state_json = to_json(state)

        insert_sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """

        update_sql = f"""
        UPDATE {self._session_table}
        SET state = %s
        WHERE id = %s
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    insert_sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_json_str,
                    ),
                )
                cursor.execute(update_sql, (state_json, session_id))
            finally:
                cursor.close()
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

        Returns:
            List of event records ordered by timestamp ASC.
        """
        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {self._events_table}
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (session_id,))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()

                return [
                    EventRecord(
                        session_id=cast("str", row[0]),
                        invocation_id=cast("str", row[1]),
                        author=cast("str", row[2]),
                        timestamp=cast("datetime", row[3]),
                        event_json=from_json(row[4]) if isinstance(row[4], str) else cast("dict[str, Any]", row[4]),
                    )
                    for row in rows
                ]
        except mysql.connector.Error as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "errno", None) == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return await async_(self._get_events)(session_id, after_timestamp, limit)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._append_event_and_update_state(event_record, event_record["session_id"], {})

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)


class MysqlConnectorAsyncADKMemoryStore(BaseAsyncADKMemoryStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB ADK memory store using mysql-connector async driver."""

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
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
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

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
            cursor = await conn.cursor()
            try:
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
            finally:
                await cursor.close()
            await conn.commit()
        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
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

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()
                columns = [col[0] for col in cursor.description or []]
            finally:
                await cursor.close()

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (session_id,))
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                await cursor.close()

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)
        """
        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (days,))
                await conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                await cursor.close()


class MysqlConnectorSyncADKMemoryStore(BaseAsyncADKMemoryStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB ADK memory store using mysql-connector sync driver."""

    __slots__ = ()

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
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
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_memory_table_sql)())

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
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
                    cursor.execute(sql, cast("tuple[Any, ...]", params))
                    inserted_count += cursor.rowcount
            finally:
                cursor.close()
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

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description or []]
            finally:
                cursor.close()

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return await async_(self._search_entries)(query, app_name, user_id, limit)

    def _delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

    def _delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (days,))
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)
