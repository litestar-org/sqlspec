"""PyMySQL ADK store for Google Agent Development Kit session/event storage."""

import re
from typing import TYPE_CHECKING, Any, Final, cast

import pymysql

from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.pymysql.config import PyMysqlConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PyMysqlADKMemoryStore", "PyMysqlADKStore")

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


class PyMysqlADKStore(BaseSyncADKStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK store using PyMySQL.

    Implements session and event storage for Google Agent Development Kit
    using MySQL/MariaDB via the PyMySQL sync driver. Provides:
    - Session state management with JSON storage
    - Full-event JSON storage (single ``event_json`` column)
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

    def _parse_owner_id_column_for_mysql(self, column_ddl: str) -> "tuple[str, str]":
        return _parse_owner_id_column_for_mysql(column_ddl)

    def _get_create_sessions_table_sql(self) -> str:
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

    def _get_create_events_table_sql(self) -> str:
        """Get MySQL CREATE TABLE SQL for events.

        Post clean-break schema: 5 columns only.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(128) NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            event_json JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE,
            INDEX idx_{self._events_table}_session (session_id, timestamp ASC)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_sessions_table_sql())
            driver.execute_script(self._get_create_events_table_sql())

    def create_session(
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

        result = self.get_session(session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    def get_session(self, session_id: str) -> "SessionRecord | None":
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

                session_id_val, app_name, user_id, state_json, create_time, update_time = row

                return SessionRecord(
                    id=session_id_val,
                    app_name=app_name,
                    user_id=user_id,
                    state=from_json(state_json) if isinstance(state_json, str) else state_json,
                    create_time=create_time,
                    update_time=update_time,
                )
        except pymysql.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return None
            raise

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
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

    def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (session_id,))
            finally:
                cursor.close()
            conn.commit()

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
        except pymysql.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
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
    ) -> EventRecord:
        """Create a new event.

        Constructs an EventRecord from the provided fields and inserts it.

        Args:
            event_id: Unique event identifier (unused in new schema, kept for contract).
            session_id: Session identifier.
            app_name: Application name (unused in new schema, kept for contract).
            user_id: User identifier (unused in new schema, kept for contract).
            author: Event author.
            actions: Unused in new contract (kept for interface compatibility).
            content: Event content dictionary.
            **kwargs: Additional fields including invocation_id, timestamp, event_json.

        Returns:
            Created event record.
        """
        from datetime import datetime, timezone

        invocation_id = kwargs.get("invocation_id", "")
        timestamp = kwargs.get("timestamp", datetime.now(tz=timezone.utc))
        event_json = kwargs.get("event_json", content or {})
        event_json_str = to_json(event_json) if not isinstance(event_json, str) else event_json

        sql = f"""
        INSERT INTO {self._events_table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (session_id, invocation_id, author or "", timestamp, event_json_str),
                )
            finally:
                cursor.close()
            conn.commit()

        return EventRecord(
            session_id=session_id,
            invocation_id=invocation_id,
            author=author or "",
            timestamp=timestamp,
            event_json=event_json,
        )

    def create_event_and_update_state(
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

    def list_events(self, session_id: str) -> "list[EventRecord]":
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
                        session_id=row[0],
                        invocation_id=row[1],
                        author=row[2],
                        timestamp=row[3],
                        event_json=from_json(row[4]) if isinstance(row[4], str) else row[4],
                    )
                    for row in rows
                ]
        except pymysql.MySQLError as exc:
            if "doesn't exist" in str(exc) or getattr(exc, "args", [None])[0] == MYSQL_TABLE_NOT_FOUND_ERROR:
                return []
            raise


class PyMysqlADKMemoryStore(BaseSyncADKMemoryStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK memory store using PyMySQL."""

    __slots__ = ()

    def __init__(self, config: "PyMysqlConfig") -> None:
        super().__init__(config)

    def _get_create_memory_table_sql(self) -> str:
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

    def create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_memory_table_sql())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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

    def search_entries(
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

    def delete_entries_by_session(self, session_id: str) -> int:
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

    def delete_entries_older_than(self, days: int) -> int:
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
