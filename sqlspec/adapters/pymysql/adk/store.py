"""PyMySQL ADK store for Google Agent Development Kit session/event storage."""

import re
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.adapters.pymysql._typing import PYMYSQL_MODULE
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.pymysql.config import PyMysqlConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PyMysqlADKMemoryStore", "PyMysqlADKStore")

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146
_PYMYSQL_ERROR = cast("type[BaseException]", getattr(PYMYSQL_MODULE, "MySQLError", Exception))


def _parse_owner_id_column_for_mysql(column_ddl: str) -> "tuple[str, str]":
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


class PyMysqlADKStore(BaseAsyncADKStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK store using PyMySQL.

    Implements session and event storage for Google Agent Development Kit
    using MySQL/MariaDB via the PyMySQL sync driver. Provides:
    - Session state management with JSON storage
    - Full-event JSON storage (single ``event_data`` column)
    - Atomic event-create + state-update in one transaction
    - Microsecond-precision timestamps
    - Foreign key constraints with cascade delete

    Notes:
        - MySQL JSON type used - requires MySQL 5.7.8+
        - TIMESTAMP(6) provides microsecond precision
        - InnoDB engine required for foreign key support
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ()

    def __init__(self, config: "PyMysqlConfig") -> None:
        super().__init__(config)

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

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        return _parse_owner_id_column_for_mysql(column_ddl)

    async def _get_create_sessions_table_sql(self) -> str:
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

        Post clean-break schema: 5 columns only.
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
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]

    def _create_tables(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_sessions_table_sql)())
            driver.execute_script(run_(self._get_create_events_table_sql)())
            driver.execute_script(run_(self._get_create_app_states_table_sql)())
            driver.execute_script(run_(self._get_create_user_states_table_sql)())
            driver.execute_script(run_(self._get_create_metadata_table_sql)())
            driver.execute_script(run_(self._get_seed_metadata_sql)())

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

    def _get_session(self, session_id: str, renew_for: "int | timedelta | None" = None) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                        update_sql = f"UPDATE {self._session_table} SET update_time = UTC_TIMESTAMP(6) WHERE id = %s"
                        cursor.execute(update_sql, (session_id,))
                        conn.commit()

                    cursor.execute(sql, (session_id,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()

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
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

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

    def _delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
            finally:
                cursor.close()
            conn.commit()

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
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=from_json(row[3]) if isinstance(row[3], str) else row[3],
                        create_time=row[4],
                        update_time=row[5],
                    )
                    for row in rows
                ]
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
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

        MySQL doesn't support UPDATE...RETURNING; the UPDATE is followed by a
        SELECT inside the same transaction so callers get the refreshed row
        without acquiring a second connection.
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

        app_upsert_sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """

        user_upsert_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
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
                        event_data_str,
                    ),
                )
                cursor.execute(update_sql, (state_json, session_id))
                cursor.execute(select_sql, (session_id,))
                row = cursor.fetchone()
                if app_state:
                    if app_name is None:
                        msg = "app_name is required when app_state is provided."
                        raise ValueError(msg)
                    cursor.execute(app_upsert_sql, (app_name, to_json(app_state)))
                if user_state:
                    if app_name is None or user_id is None:
                        msg = "app_name and user_id are required when user_state is provided."
                        raise ValueError(msg)
                    cursor.execute(user_upsert_sql, (app_name, user_id, to_json(user_state)))
            finally:
                cursor.close()
            conn.commit()

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

    def _insert_event(self, event_record: EventRecord) -> None:
        event_data = event_record["event_data"]
        event_data_str = to_json(event_data) if not isinstance(event_data, str) else event_data

        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["author"],
                        event_record["timestamp"],
                        event_data_str,
                    ),
                )
            finally:
                cursor.close()
            conn.commit()

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
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = " LIMIT %s" if limit else ""
        sql = f"""
        SELECT session_id, invocation_id, author, timestamp, event_data
        FROM {self._events_table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """
        if limit:
            params.append(limit)

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, tuple(params))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()

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
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    def _delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < %s"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (before,))
                    conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    cursor.close()
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    def _delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < %s"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (updated_before,))
                    conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    cursor.close()
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return 0
            raise

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = %s"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (app_name,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                return from_json(row[0]) if row is not None and isinstance(row[0], str) else (row[0] if row else None)
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = %s AND user_id = %s"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (app_name, user_id))
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                return from_json(row[0]) if row is not None and isinstance(row[0], str) else (row[0] if row else None)
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (app_name, to_json(state)))
            finally:
                cursor.close()
            conn.commit()

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (app_name, user_id, to_json(state)))
            finally:
                cursor.close()
            conn.commit()

    def _get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE `key` = %s"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (key,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                return row[0] if row is not None else None
        except PYMYSQL_MODULE.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def _set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        INSERT INTO {self._metadata_table} (`key`, value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key, value))
            finally:
                cursor.close()
            conn.commit()

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)


class PyMysqlADKMemoryStore(BaseAsyncADKMemoryStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK memory store using PyMySQL."""

    __slots__ = ()

    def __init__(self, config: "PyMysqlConfig") -> None:
        super().__init__(config)

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
                    cursor.execute(sql, params)
                    inserted_count += cursor.rowcount
            finally:
                cursor.close()
            conn.commit()
        return inserted_count

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
