"""PyMySQL ADK store for Google Agent Development Kit session/event storage."""

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast

import pymysql
from typing_extensions import NotRequired

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.pymysql.config import PyMysqlConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PyMysqlADKConfig", "PyMysqlADKMemoryStore", "PyMysqlADKStore")

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146


class PyMysqlADKConfig(ADKConfig):
    """PyMySQL-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the PyMySQL ADK store.
    """

    enable_event_generated_columns: NotRequired[bool]
    """Create MySQL generated columns and indexes for common ADK event JSON paths."""

    enable_covering_indexes: NotRequired[bool]
    """Add hot-path payload columns to MySQL ADK event replay indexes."""

    session_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK session table."""

    events_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK events table."""

    app_state_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK app state table."""

    user_state_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK user state table."""

    memory_table_options: NotRequired[str]
    """Raw MySQL table options appended to the ADK memory table."""


class PyMysqlADKStore(BaseSyncADKStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK store using PyMySQL."""

    __slots__ = ()

    def __init__(self, config: "PyMysqlConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create all ADK session tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        _create_tables(self)

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return _create_session(self, session_id, app_name, user_id, state, owner_id)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by scoped identifiers."""
        return _select_session(self, app_name, user_id, session_id, renew_for=renew_for)

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        _update_session_state(self, app_name, user_id, session_id, state)

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return _list_sessions(self, app_name, user_id)

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        _delete_session(self, app_name, user_id, session_id)

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        _append_event(self, event_record)

    def append_event_and_update_state(
        self,
        event_record: EventRecord,
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically append an event and update session + scoped state."""
        return _append_event_and_update_state(
            self, event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return _select_events(self, app_name, user_id, session_id, after_timestamp, limit)

    def delete_expired_events(self, before: "datetime") -> int:
        """Delete events older than the given timestamp."""
        return _delete_expired_events(self, before)

    def delete_idle_sessions(self, updated_before: "datetime") -> int:
        """Delete sessions whose update_time predates the threshold."""
        return _delete_idle_sessions(self, updated_before)

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        return _app_state(self, app_name)

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        return _user_state(self, app_name, user_id)

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        _upsert_app_state(self, app_name, state)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        _upsert_user_state(self, app_name, user_id, state)

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        return _metadata(self, key)

    def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        _set_metadata(self, key, value)

    def _sessions_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for sessions."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "session_table_options")
        return _mysql_sessions_ddl(self._session_table, self._owner_id_column_ddl, table_options)

    def _events_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for events."""
        return _mysql_events_ddl(self._events_table, self._session_table, _adk_config(self._config))

    def _app_states_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for app-scoped state."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "app_state_table_options")
        return _mysql_app_state_ddl(self._app_state_table, table_options)

    def _user_states_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for user-scoped state."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "user_state_table_options")
        return _mysql_user_state_ddl(self._user_state_table, table_options)

    def _metadata_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for ADK metadata."""
        return _mysql_metadata_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for ADK metadata."""
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements."""
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class PyMysqlADKMemoryStore(BaseSyncADKMemoryStore["PyMysqlConfig"]):
    """MySQL/MariaDB ADK memory store using PyMySQL."""

    __slots__ = ()

    def __init__(self, config: "PyMysqlConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return self._insert_memory_entries(entries, owner_id)

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return self._search_entries(query, app_name, user_id, limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return self._delete_entries_by_session(session_id)

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return self._delete_entries_older_than(days)

    def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _mysql_owner_id_column_parts(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        fts_index = ""
        if self._use_fts:
            fts_index = f",\n            FULLTEXT INDEX idx_{self._memory_table}_fts (content_text)"
        table_options = _mysql_table_options(adk_config, "memory_table_options")

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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())

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


def _create_tables(store: PyMysqlADKStore) -> None:
    with store._config.provide_session() as driver:
        driver.execute_script(store._sessions_table_ddl())
        driver.execute_script(store._events_table_ddl())
        driver.execute_script(store._app_states_table_ddl())
        driver.execute_script(store._user_states_table_ddl())
        driver.execute_script(store._metadata_table_ddl())


def _create_session(
    store: PyMysqlADKStore,
    session_id: str,
    app_name: str,
    user_id: str,
    state: "dict[str, Any]",
    owner_id: "Any | None" = None,
) -> SessionRecord:
    params: tuple[Any, ...]
    if store._owner_id_column_name:
        sql = f"""
        INSERT INTO {store._session_table} (id, app_name, user_id, {store._owner_id_column_name}, state, create_time, update_time)
        VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
        """
        params = (session_id, app_name, user_id, owner_id, to_json(state))
    else:
        sql = f"""
        INSERT INTO {store._session_table} (id, app_name, user_id, state, create_time, update_time)
        VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
        """
        params = (session_id, app_name, user_id, to_json(state))

    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
        finally:
            cursor.close()
        conn.commit()

    result = _select_session(store, app_name, user_id, session_id)
    if result is None:
        msg = "Failed to fetch created session"
        raise RuntimeError(msg)
    return result


def _select_session(
    store: PyMysqlADKStore, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
) -> "SessionRecord | None":
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                if renew_for is not None and store._calculate_expires_at(renew_for) is not None:
                    cursor.execute(
                        f"""
                        UPDATE {store._session_table}
                        SET update_time = UTC_TIMESTAMP(6)
                        WHERE app_name = %s AND user_id = %s AND id = %s
                        """,
                        (app_name, user_id, session_id),
                    )
                    conn.commit()

                cursor.execute(
                    f"""
                    SELECT id, app_name, user_id, state, create_time, update_time
                    FROM {store._session_table}
                    WHERE app_name = %s AND user_id = %s AND id = %s
                    """,
                    (app_name, user_id, session_id),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()
        return _session_record_from_row(row) if row is not None else None
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return None
        raise


def _update_session_state(
    store: PyMysqlADKStore, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]"
) -> None:
    sql = f"""
    UPDATE {store._session_table}
    SET state = %s, update_time = UTC_TIMESTAMP(6)
    WHERE app_name = %s AND user_id = %s AND id = %s
    """
    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (to_json(state), app_name, user_id, session_id))
        finally:
            cursor.close()
        conn.commit()


def _list_sessions(store: PyMysqlADKStore, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
    if user_id is None:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {store._session_table}
        WHERE app_name = %s
        ORDER BY update_time DESC
        """
        params: tuple[Any, ...] = (app_name,)
    else:
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {store._session_table}
        WHERE app_name = %s AND user_id = %s
        ORDER BY update_time DESC
        """
        params = (app_name, user_id)

    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return [_session_record_from_row(row) for row in rows]
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return []
        raise


def _delete_session(store: PyMysqlADKStore, app_name: str, user_id: str, session_id: str) -> None:
    sql = f"DELETE FROM {store._session_table} WHERE app_name = %s AND user_id = %s AND id = %s"
    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (app_name, user_id, session_id))
        finally:
            cursor.close()
        conn.commit()


def _append_event(store: PyMysqlADKStore, event_record: EventRecord) -> None:
    sql = f"""
    INSERT INTO {store._events_table} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, _event_insert_params(event_record))
        finally:
            cursor.close()
        conn.commit()


def _append_event_and_update_state(
    store: PyMysqlADKStore,
    event_record: EventRecord,
    app_name: str,
    user_id: str,
    session_id: str,
    state: "dict[str, Any]",
    *,
    app_state: "dict[str, Any] | None" = None,
    user_state: "dict[str, Any] | None" = None,
) -> SessionRecord:
    insert_sql = f"""
    INSERT INTO {store._events_table} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    update_sql = f"""
    UPDATE {store._session_table}
    SET state = %s, update_time = UTC_TIMESTAMP(6)
    WHERE app_name = %s AND user_id = %s AND id = %s
    """
    select_sql = f"""
    SELECT id, app_name, user_id, state, create_time, update_time
    FROM {store._session_table}
    WHERE app_name = %s AND user_id = %s AND id = %s
    """

    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(update_sql, (to_json(state), app_name, user_id, session_id))
            cursor.execute(select_sql, (app_name, user_id, session_id))
            row = cursor.fetchone()
            if row is None:
                _raise_session_not_found(session_id)
            cursor.execute(
                insert_sql,
                (
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    _json_for_storage(event_record["event_data"]),
                ),
            )
            if app_state is not None:
                cursor.execute(_mysql_upsert_app_state_sql(store._app_state_table), (app_name, to_json(app_state)))
            if user_state is not None:
                cursor.execute(
                    _mysql_upsert_user_state_sql(store._user_state_table), (app_name, user_id, to_json(user_state))
                )
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
        conn.commit()

    return _session_record_from_row(row)


def _select_events(
    store: PyMysqlADKStore,
    app_name: str,
    user_id: str,
    session_id: str,
    after_timestamp: "datetime | None" = None,
    limit: "int | None" = None,
) -> "list[EventRecord]":
    if limit == 0:
        return []

    where_clauses = ["app_name = %s", "user_id = %s", "session_id = %s"]
    params: list[Any] = [app_name, user_id, session_id]
    if after_timestamp is not None:
        where_clauses.append("timestamp > %s")
        params.append(after_timestamp)
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT %s"
        params.append(limit)

    sql = f"""
    SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    FROM {store._events_table}
    WHERE {" AND ".join(where_clauses)}
    ORDER BY timestamp ASC{limit_clause}
    """

    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return [_event_record_from_row(row) for row in rows]
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return []
        raise


def _delete_expired_events(store: PyMysqlADKStore, before: "datetime") -> int:
    return _delete_before(store, store._events_table, "timestamp", before)


def _delete_idle_sessions(store: PyMysqlADKStore, updated_before: "datetime") -> int:
    return _delete_before(store, store._session_table, "update_time", updated_before)


def _delete_before(store: PyMysqlADKStore, table_name: str, column_name: str, threshold: "datetime") -> int:
    sql = f"DELETE FROM {table_name} WHERE {column_name} < %s"
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (threshold,))
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return 0
        raise


def _app_state(store: PyMysqlADKStore, app_name: str) -> "dict[str, Any] | None":
    return _state(store, store._app_state_table, "app_name = %s", (app_name,))


def _user_state(store: PyMysqlADKStore, app_name: str, user_id: str) -> "dict[str, Any] | None":
    return _state(store, store._user_state_table, "app_name = %s AND user_id = %s", (app_name, user_id))


def _state(
    store: PyMysqlADKStore, table_name: str, where_clause: str, params: "tuple[Any, ...]"
) -> "dict[str, Any] | None":
    sql = f"SELECT state FROM {table_name} WHERE {where_clause}"
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            finally:
                cursor.close()
        return _json_dict(row[0]) if row is not None else None
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return None
        raise


def _upsert_app_state(store: PyMysqlADKStore, app_name: str, state: "dict[str, Any]") -> None:
    _execute_commit(store, _mysql_upsert_app_state_sql(store._app_state_table), (app_name, to_json(state)))


def _upsert_user_state(store: PyMysqlADKStore, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
    _execute_commit(store, _mysql_upsert_user_state_sql(store._user_state_table), (app_name, user_id, to_json(state)))


def _metadata(store: PyMysqlADKStore, key: str) -> "str | None":
    sql = f"SELECT value FROM {store._metadata_table} WHERE `key` = %s"
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (key,))
                row = cursor.fetchone()
            finally:
                cursor.close()
        return str(row[0]) if row is not None else None
    except pymysql.MySQLError as exc:
        if _is_mysql_table_missing(exc):
            return None
        raise


def _set_metadata(store: PyMysqlADKStore, key: str, value: str) -> None:
    _execute_commit(store, _mysql_upsert_metadata_sql(store._metadata_table), (key, value))


def _execute_commit(store: PyMysqlADKStore, sql: str, params: "tuple[Any, ...]") -> None:
    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
        finally:
            cursor.close()
        conn.commit()


def _mysql_owner_id_column_parts(column_ddl: str) -> "tuple[str, str]":
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


def _adk_config(config: Any) -> PyMysqlADKConfig:
    """Return PyMySQL ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("PyMysqlADKConfig", adk_config)


def _mysql_table_options(adk_config: Mapping[str, Any], key: str) -> str:
    value = adk_config.get(key)
    if not isinstance(value, str):
        return ""
    value = value.strip()
    return f" {value}" if value else ""


def _is_mysql_table_missing(exc: BaseException) -> bool:
    args = getattr(exc, "args", ())
    errno = getattr(exc, "errno", None)
    return (
        errno == MYSQL_TABLE_NOT_FOUND_ERROR
        or "doesn't exist" in str(exc)
        or bool(args and args[0] == MYSQL_TABLE_NOT_FOUND_ERROR)
    )


def _json_for_storage(value: Any) -> str:
    return value if isinstance(value, str) else to_json(value)


def _json_dict(value: Any) -> "dict[str, Any]":
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        return cast("dict[str, Any]", from_json(value))
    return cast("dict[str, Any]", value)


def _session_record_from_row(row: Any) -> SessionRecord:
    return SessionRecord(
        id=row[0], app_name=row[1], user_id=row[2], state=_json_dict(row[3]), create_time=row[4], update_time=row[5]
    )


def _event_record_from_row(row: Any) -> EventRecord:
    return EventRecord(
        id=row[0],
        app_name=row[1],
        user_id=row[2],
        session_id=row[3],
        invocation_id=row[4],
        timestamp=row[5],
        event_data=_json_dict(row[6]),
    )


def _event_insert_params(event_record: EventRecord) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        event_record["timestamp"],
        _json_for_storage(event_record["event_data"]),
    )


def _mysql_sessions_ddl(session_table: str, owner_id_column_ddl: "str | None", table_options: str = "") -> str:
    owner_id_line = ""
    fk_constraint = ""
    if owner_id_column_ddl:
        col_def, fk_def = _mysql_owner_id_column_parts(owner_id_column_ddl)
        owner_id_line = f"\n            {col_def},"
        if fk_def:
            fk_constraint = f",\n            {fk_def}"

    return f"""
        CREATE TABLE IF NOT EXISTS {session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,{owner_id_line}
            state JSON NOT NULL,
            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{session_table}_app_user (app_name, user_id),
            INDEX idx_{session_table}_update_time (update_time DESC){fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_events_ddl(events_table: str, session_table: str, adk_config: Mapping[str, Any] | None = None) -> str:
    adk_config = adk_config or {}
    generated_columns = ""
    generated_indexes = ""
    if adk_config.get("enable_event_generated_columns", False):
        generated_columns = """,
            author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED,
            node_path_gc VARCHAR(512) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED"""
        generated_indexes = f""",
            INDEX idx_{events_table}_author_gc (session_id, author_gc, timestamp ASC),
            INDEX idx_{events_table}_node_path_gc (session_id, node_path_gc, timestamp ASC)"""

    covering_column = ", invocation_id" if adk_config.get("enable_covering_indexes", False) else ""
    table_options = _mysql_table_options(adk_config, "events_table_options")

    return f"""
        CREATE TABLE IF NOT EXISTS {events_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            event_data JSON NOT NULL{generated_columns},
            FOREIGN KEY (session_id) REFERENCES {session_table}(id) ON DELETE CASCADE,
            INDEX idx_{events_table}_scope (app_name, user_id, session_id, timestamp ASC{covering_column}),
            INDEX idx_{events_table}_session (session_id, timestamp ASC{covering_column}){generated_indexes}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_app_state_ddl(app_state_table: str, table_options: str = "") -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_user_state_ddl(user_state_table: str, table_options: str = "") -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            PRIMARY KEY (app_name, user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_metadata_ddl(metadata_table: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {metadata_table} (
            `key` VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """


def _mysql_upsert_app_state_sql(app_state_table: str) -> str:
    return f"""
        INSERT INTO {app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _mysql_upsert_user_state_sql(user_state_table: str) -> str:
    return f"""
        INSERT INTO {user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _mysql_upsert_metadata_sql(metadata_table: str) -> str:
    return f"""
        INSERT INTO {metadata_table} (`key`, value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)
