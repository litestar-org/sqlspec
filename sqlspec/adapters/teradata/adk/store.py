"""Teradata Vantage ADK stores for Google Agent Development Kit session storage."""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, cast

from sqlspec.adapters.teradata._typing import TeradataCursor
from sqlspec.adapters.teradata.core import (
    TABLE_EXISTS_SQL,
    extract_error_code,
    format_identifier,
    format_table_ref,
    resolve_rowcount,
    split_schema_and_table,
)
from sqlspec.adapters.teradata.migrations import build_guarded_drop_table
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.migrations.schema import DropStatement, SchemaTarget, metadata_has_index, run_drop_statements_sync
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.teradata.config import TeradataConfig
    from sqlspec.extensions.adk import SessionOrderBy
    from sqlspec.extensions.adk.memory._types import StoredMemory

__all__ = ("TeradataADKMemoryStore", "TeradataADKStore")

TERADATA_TABLE_NOT_FOUND_SQLCODE: Final[int] = 3807
JSON_COLUMN_TYPE: Final[str] = "JSON"
_DUPLICATE_KEY_CODES: Final[frozenset[int]] = frozenset({2801, 2802, 2803})
_EVENT_ID_CHUNK_SIZE: Final[int] = 500


class TeradataADKStore(BaseSyncADKStore["TeradataConfig"]):
    """Synchronous Teradata Vantage ADK session/event store."""

    connector_name: ClassVar[str] = "teradata"
    __slots__ = ()

    _max_table_name_parts = 2

    def drop_tables(self) -> None:
        """Drop existing ADK tables in FK-safe order using catalog guards."""
        _drop_existing_tables(self._config, self.downgrade_drops())
        self._log_tables_dropped()

    def downgrade_drops(self) -> "list[DropStatement]":
        """Return catalog-probed drops for the ADK tables in FK-safe order."""
        return [
            build_guarded_drop_table(table)
            for table in (
                self._metadata_table,
                self._user_state_table,
                self._app_state_table,
                self._events_table,
                self._session_table,
            )
        ]

    def create_tables(self) -> None:
        """Create ADK tables and indexes using catalog guards and MULTISET DDL."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            _create_missing_table(driver, self._session_table, self._sessions_table_ddl())
            _create_missing_table(driver, self._events_table, self._events_table_ddl())
            _create_missing_table(driver, self._app_state_table, self._app_states_table_ddl())
            _create_missing_table(driver, self._user_state_table, self._user_states_table_ddl())
            _create_missing_table(driver, self._metadata_table, self._metadata_table_ddl())
            for index_name, index_table, columns in self._index_specs():
                _create_missing_index(driver, index_table, index_name, columns)
                driver.commit()
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new ADK session."""
        owner_column = f", {format_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_param = ", ?" if self._owner_id_column_name else ""
        insert_sql = f"""
        INSERT INTO {format_table_ref(self._session_table)} (
            id, app_name, user_id{owner_column}, state, create_time, update_time
        )
        VALUES (?, ?, ?{owner_param}, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
        """
        select_sql = f"""
        SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
        FROM {format_table_ref(self._session_table)}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            params = (session_id, app_name, user_id, owner_id, to_json(state))
        else:
            params = (session_id, app_name, user_id, to_json(state))
        self._execute(insert_sql, params, commit=True)
        row = self._execute_fetchone(select_sql, (app_name, user_id, session_id))
        if row is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return _session_record_from_row(row)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Return a scoped session or ``None`` if absent."""
        try:
            if self._should_renew(renew_for):
                self._execute(
                    f"""
                    UPDATE {format_table_ref(self._session_table)}
                    SET update_time = CURRENT_TIMESTAMP(6)
                    WHERE app_name = ? AND user_id = ? AND id = ?
                    """,
                    (app_name, user_id, session_id),
                    commit=True,
                    read=renew_for is None,
                )
            row = self._execute_fetchone(
                f"""
                SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
                FROM {format_table_ref(self._session_table)}
                WHERE app_name = ? AND user_id = ? AND id = ?
                """,
                (app_name, user_id, session_id),
                read=renew_for is None,
            )
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return None
            raise
        return _session_record_from_row(row) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Replace a session's durable state."""
        self._execute(
            f"""
            UPDATE {format_table_ref(self._session_table)}
            SET state = ?, update_time = CURRENT_TIMESTAMP(6)
            WHERE app_name = ? AND user_id = ? AND id = ?
            """,
            (to_json(state), app_name, user_id, session_id),
            commit=True,
        )

    def list_sessions(
        self,
        app_name: str,
        user_id: "str | None" = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: "int | None" = None,
        offset: "int | None" = None,
    ) -> "list[StoredSession]":
        """List ADK sessions for an application, optionally scoped to a user."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            rows = self._execute_fetchall(sql, params, read=True)
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete tenant-scoped child events before their session atomically."""
        with _transaction(self._config) as (conn, cursor):
            cursor.execute(
                f"DELETE FROM {format_table_ref(self._events_table)} WHERE app_name = ? AND user_id = ? AND session_id = ?",
                (app_name, user_id, session_id),
            )
            cursor.execute(
                f"DELETE FROM {format_table_ref(self._session_table)} WHERE app_name = ? AND user_id = ? AND id = ?",
                (app_name, user_id, session_id),
            )
            conn.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session after verifying session ownership."""
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        ownership_sql = (
            f"SELECT TOP 1 1 FROM {format_table_ref(self._session_table)} WHERE id = ? AND app_name = ? AND user_id = ?"
        )
        with _transaction(self._config) as (conn, cursor):
            cursor.execute(ownership_sql, (session_id, app_name, user_id))
            if cursor.fetchone() is None:
                msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
                raise ValueError(msg)
            cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            conn.commit()

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Atomically append an event and update durable session/scoped state."""
        _validate_event_record_scope(event_record, app_name, user_id, session_id)
        update_sql = f"""
        UPDATE {format_table_ref(self._session_table)}
        SET state = ?, update_time = CURRENT_TIMESTAMP(6)
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        select_sql = f"""
        SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
        FROM {format_table_ref(self._session_table)}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        with _transaction(self._config) as (conn, cursor):
            cursor.execute(update_sql, (to_json(state), app_name, user_id, session_id))
            cursor.execute(select_sql, (app_name, user_id, session_id))
            row = cursor.fetchone()
            if row is None:
                _raise_session_not_found(session_id)
            cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            if app_state is not None:
                app_json = to_json(app_state)
                cursor.execute(self._upsert_app_state_sql(), (app_name, app_json))
            if user_state is not None:
                user_json = to_json(user_state)
                cursor.execute(self._upsert_user_state_sql(), (app_name, user_id, user_json))
            conn.commit()
        return _session_record_from_row(row)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        """Return events for a scoped session ordered by event timestamp."""
        if limit == 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = self._execute_fetchall(sql, params, read=True)
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than ``before``."""
        sql = f"DELETE FROM {format_table_ref(self._events_table)} WHERE timestamp < ?"
        params: list[Any] = [before]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            return self._execute(sql, tuple(params), commit=True)
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete events before expired parent sessions in one transaction."""
        where = "update_time < ?"
        params: tuple[Any, ...] = (updated_before,)
        if app_name is not None:
            where += " AND app_name = ?"
            params = (*params, app_name)
        try:
            with _transaction(self._config) as (conn, cursor):
                cursor.execute(
                    f"DELETE FROM {format_table_ref(self._events_table)} WHERE session_id IN (SELECT id FROM {format_table_ref(self._session_table)} WHERE {where})",
                    params,
                )
                cursor.execute(f"DELETE FROM {format_table_ref(self._session_table)} WHERE {where}", params)
                count = resolve_rowcount(cursor)
                conn.commit()
                return count
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user state rows whose update_time is older than ``updated_before``."""
        sql = f"DELETE FROM {format_table_ref(self._user_state_table)} WHERE update_time < ?"
        params: list[Any] = [updated_before]
        if app_name is not None:
            sql += " AND app_name = ?"
            params.append(app_name)
        try:
            return self._execute(sql, tuple(params), commit=True)
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP 1 state FROM {format_table_ref(self._app_state_table)} WHERE app_name = ?", (app_name,)
            )
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        try:
            row = self._execute_fetchone(
                f"""
                SELECT TOP 1 state
                FROM {format_table_ref(self._user_state_table)}
                WHERE app_name = ? AND user_id = ?
                """,
                (app_name, user_id),
            )
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        encoded = to_json(state)
        self._execute(self._upsert_app_state_sql(), (app_name, encoded), commit=True)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        encoded = to_json(state)
        self._execute(self._upsert_user_state_sql(), (app_name, user_id, encoded), commit=True)

    def get_metadata(self, key: str) -> "str | None":
        """Return an ADK metadata value."""
        try:
            row = self._execute_fetchone(
                f'SELECT TOP 1 value FROM {format_table_ref(self._metadata_table)} WHERE "key" = ?', (key,)
            )
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return None
            raise
        return str(_row_value(row, "value", 0)) if row is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Set an ADK metadata value."""
        self._execute(_upsert_metadata_sql(self._metadata_table), (key, value), commit=True)

    def get_schema_targets_sync(self, driver: Any) -> "list[SchemaTarget]":
        """Return ordered Teradata ADK session-store SchemaTarget definitions."""
        del driver
        return self._schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing Teradata ADK session and event companion indexes without committing."""
        for index_name, index_table, columns in self._index_specs():
            _create_missing_index(driver, index_table, index_name, columns)

    def _index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for session and event indexes."""
        return [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]

    def _schema_targets(self) -> "list[SchemaTarget]":
        """Construct Teradata SchemaTarget definitions in FK dependency order."""
        ddl_targets = (
            (self._session_table, self._sessions_table_ddl()),
            (self._events_table, self._events_table_ddl()),
            (self._app_state_table, self._app_states_table_ddl()),
            (self._user_state_table, self._user_states_table_ddl()),
            (self._metadata_table, self._metadata_table_ddl()),
        )
        return [_schema_target(table, ddl) for table, ddl in ddl_targets]

    def _sessions_table_ddl(self) -> str:
        """Return Teradata DDL for the ADK session table."""
        return _sessions_table_ddl(self._session_table, JSON_COLUMN_TYPE, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return Teradata DDL for the ADK event table."""
        return _events_table_ddl(self._events_table, self._session_table, JSON_COLUMN_TYPE)

    def _app_states_table_ddl(self) -> str:
        """Return Teradata DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table, JSON_COLUMN_TYPE)

    def _user_states_table_ddl(self) -> str:
        """Return Teradata DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table, JSON_COLUMN_TYPE)

    def _metadata_table_ddl(self) -> str:
        """Return Teradata DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE {format_table_ref(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE {format_table_ref(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE {format_table_ref(self._metadata_table)}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE {format_table_ref(self._events_table)}",
            f"DROP TABLE {format_table_ref(self._session_table)}",
        ]

    def _upsert_app_state_sql(self) -> str:
        return _upsert_state_sql(self._app_state_table, ("app_name",))

    def _upsert_user_state_sql(self) -> str:
        return _upsert_state_sql(self._user_state_table, ("app_name", "user_id"))

    def _events_query(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "tuple[str, tuple[Any, ...]]":
        return _events_query(
            self._events_table, self._session_table, app_name, user_id, session_id, after_timestamp, limit
        )

    def _execute_fetchone(self, sql: str, params: "tuple[Any, ...]" = (), *, read: bool = False) -> "Any | None":
        with self._provide_read_connection(primary=not read) as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = (), *, read: bool = False) -> "list[Any]":
        with self._provide_read_connection(primary=not read) as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False, read: bool = False) -> int:
        with self._provide_read_connection(primary=not read) as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, params)
            rowcount = resolve_rowcount(cursor)
            if commit and getattr(conn, "autocommit", True) is False:
                conn.commit()
            return rowcount


class TeradataADKMemoryStore(BaseSyncADKMemoryStore["TeradataConfig"]):
    """Teradata Vantage ADK memory store using teradatasql."""

    __slots__ = ()

    _max_table_name_parts = 2

    def drop_tables(self) -> None:
        """Drop an existing memory table using a catalog guard."""
        _drop_existing_tables(self._config, self.downgrade_drops())

    def downgrade_drops(self) -> "list[DropStatement]":
        """Return the catalog-probed drop for the memory table."""
        return [build_guarded_drop_table(self._memory_table)]

    def create_tables(self) -> None:
        """Create the memory table and indexes using catalog guards and MULTISET DDL."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            _create_missing_table(driver, self._memory_table, self._memory_table_ddl())
            for index_name, index_table, columns in self._memory_index_specs():
                _create_missing_index(driver, index_table, index_name, columns)
                driver.commit()
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with event-id deduplication and TERA rollback recovery."""
        if not self._enabled:
            msg = "ADK memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0

        deduped_entries: list[StoredMemory] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for entry in entries:
            key = (entry["app_name"], entry["user_id"], entry["event_id"])
            if key not in seen_keys:
                seen_keys.add(key)
                deduped_entries.append(entry)

        table_ref = format_table_ref(self._memory_table)
        owner_column = f", {format_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_value = ", ?" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {table_ref} (
            id, session_id, app_name, user_id, scope, event_id, author, timestamp,
            content_json, content_text, metadata_json{owner_column}
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_value})
        """

        existing_keys: set[tuple[str, str, str]] = set()
        all_event_ids = list(dict.fromkeys(entry["event_id"] for entry in deduped_entries))
        for start in range(0, len(all_event_ids), _EVENT_ID_CHUNK_SIZE):
            chunk = all_event_ids[start : start + _EVENT_ID_CHUNK_SIZE]
            placeholders = ", ".join("?" for _ in chunk)
            rows = self._execute_fetchall(
                f"SELECT app_name, user_id, event_id FROM {table_ref} WHERE event_id IN ({placeholders})", tuple(chunk)
            )
            for row in rows:
                app_val = _row_value(row, "app_name", 0)
                user_val = _row_value(row, "user_id", 1)
                ev_val = _row_value(row, "event_id", 2)
                if app_val is not None and user_val is not None and ev_val is not None:
                    existing_keys.add((str(app_val), str(user_val), str(ev_val)))
        candidates = [
            entry
            for entry in deduped_entries
            if (entry["app_name"], entry["user_id"], entry["event_id"]) not in existing_keys
        ]
        if not candidates:
            return 0

        try:
            inserted = 0
            with _transaction(self._config) as (conn, cursor):
                for entry in candidates:
                    cursor.execute(sql, self._memory_insert_params(entry, owner_id))
                    raw_count = getattr(cursor, "rowcount", None)
                    inserted += resolve_rowcount(cursor) if isinstance(raw_count, int) and raw_count >= 0 else 1
                conn.commit()
        except Exception as exc:
            if extract_error_code(exc) not in _DUPLICATE_KEY_CODES:
                raise
        else:
            return inserted

        inserted = 0
        for entry in candidates:
            try:
                with _transaction(self._config) as (conn, cursor):
                    cursor.execute(sql, self._memory_insert_params(entry, owner_id))
                    raw_count = getattr(cursor, "rowcount", None)
                    row_delta = resolve_rowcount(cursor) if isinstance(raw_count, int) and raw_count >= 0 else 1
                    conn.commit()
                    inserted += row_delta
            except Exception as item_exc:
                if extract_error_code(item_exc) not in _DUPLICATE_KEY_CODES or not self._event_id_exists(
                    table_ref, entry["app_name"], entry["user_id"], entry["event_id"]
                ):
                    raise
        return inserted

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        """Search memory entries by text query."""
        _ = embedding
        if not self._enabled:
            msg = "ADK memory store is disabled"
            raise RuntimeError(msg)
        if limit == 0:
            return []
        if limit is not None and (isinstance(limit, bool) or limit < 0):
            msg = "limit must be a non-negative integer"
            raise ValueError(msg)
        limit_value = limit if limit is not None else self._max_results
        where_scope, scope_params = _build_teradata_scope_where(app_name, user_id, scope_filter)
        escaped_query = _escape_like_pattern(query)
        sql = f"""
        SELECT TOP {int(limit_value)}
            id, session_id, app_name, user_id, scope, event_id, author, timestamp,
            content_json, content_text, metadata_json, inserted_at
        FROM {format_table_ref(self._memory_table)}
        WHERE {where_scope} AND content_text LIKE ? ESCAPE '\\'
        ORDER BY timestamp DESC
        """
        try:
            rows = self._execute_fetchall(sql, (*scope_params, f"%{escaped_query}%"), read=True)
        except Exception as exc:
            if _is_teradata_table_missing(exc):
                return []
            raise
        return [_memory_record_from_row(row) for row in rows]

    def delete_entries_by_session(
        self, session_id: str, app_name: "str | None" = None, user_id: "str | None" = None
    ) -> int:
        """Delete memory entries for a specific session, optionally scoped by owner."""
        clauses = ["session_id = ?"]
        params: list[Any] = [session_id]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(user_id)
        where_sql = " AND ".join(clauses)
        return self._execute(
            f"DELETE FROM {format_table_ref(self._memory_table)} WHERE {where_sql}", tuple(params), commit=True
        )

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than the retention window."""
        clauses = ["inserted_at < ?"]
        params: list[Any] = [datetime.now(UTC) - timedelta(days=days)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        where_sql = " AND ".join(clauses)
        return self._execute(
            f"DELETE FROM {format_table_ref(self._memory_table)} WHERE {where_sql}", tuple(params), commit=True
        )

    def get_schema_targets_sync(self, driver: Any) -> "list[SchemaTarget]":
        """Return Teradata ADK memory-store SchemaTarget definitions when enabled."""
        del driver
        return self._schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing Teradata ADK memory companion indexes without committing."""
        if not self._enabled:
            return
        for index_name, index_table, columns in self._memory_index_specs():
            _create_missing_index(driver, index_table, index_name, columns)

    def _memory_insert_params(self, entry: "StoredMemory", owner_id: "object | None") -> "tuple[Any, ...]":
        params: tuple[Any, ...] = (
            entry["id"],
            entry["session_id"],
            entry["app_name"],
            entry["user_id"],
            entry.get("scope", "user"),
            entry["event_id"],
            entry.get("author"),
            entry["timestamp"],
            to_json(entry["content_json"]),
            entry["content_text"],
            to_json(entry["metadata_json"]) if entry.get("metadata_json") is not None else None,
        )
        if self._owner_id_column_name:
            return (*params, owner_id)
        return params

    def _event_id_exists(self, table_ref: str, app_name: str, user_id: str, event_id: str) -> bool:
        """Return whether a memory row already stores ``(app_name, user_id, event_id)``."""
        return (
            self._execute_fetchall(
                f"SELECT app_name, user_id, event_id FROM {table_ref} WHERE app_name = ? AND user_id = ? AND event_id = ?",
                (app_name, user_id, event_id),
            )
            != []
        )

    def _memory_table_ddl(self) -> str:
        owner_line = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        table_ref = format_table_ref(self._memory_table)
        return (
            f"CREATE MULTISET TABLE {table_ref} ("
            "id VARCHAR(128) NOT NULL, "
            "session_id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, "
            "scope VARCHAR(16) DEFAULT 'user' NOT NULL, "
            "event_id VARCHAR(128) NOT NULL, "
            "author VARCHAR(256) NULL, "
            "timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL, "
            f"content_json {JSON_COLUMN_TYPE} NOT NULL, "
            "content_text CLOB NOT NULL, "
            f"metadata_json {JSON_COLUMN_TYPE} NULL, "
            f"inserted_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL{owner_line}, "
            f"CONSTRAINT {_constraint_ref('pk', self._memory_table, 'id')} PRIMARY KEY (id), "
            f"CONSTRAINT {_constraint_ref('uq', self._memory_table, 'event_id')} UNIQUE (app_name, user_id, event_id)"
            ");"
        )

    def _memory_index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for memory-table indexes."""
        _, table_name = split_schema_and_table(self._memory_table)
        return [
            (f"idx_{table_name}_app_scope_user_time", self._memory_table, "app_name, scope, user_id, timestamp DESC"),
            (f"idx_{table_name}_scope", self._memory_table, "app_name, scope"),
            (f"idx_{table_name}_session", self._memory_table, "session_id"),
            (f"idx_{table_name}_timestamp", self._memory_table, "timestamp DESC"),
        ]

    def _schema_targets(self) -> "list[SchemaTarget]":
        """Construct the Teradata ADK memory-table SchemaTarget when enabled."""
        if not self._enabled:
            return []
        return [_schema_target(self._memory_table, self._memory_table_ddl())]

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE {format_table_ref(self._memory_table)}"]

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = (), *, read: bool = False) -> "list[Any]":
        with self._provide_read_connection(primary=not read) as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_connection() as conn, TeradataCursor(conn) as cursor:
            cursor.execute(sql, params)
            rowcount = resolve_rowcount(cursor)
            if commit and getattr(conn, "autocommit", True) is False:
                conn.commit()
            return rowcount


def _sessions_table_ddl(table: str, json_column_type: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f", {owner_id_column_ddl}" if owner_id_column_ddl else ""
    table_ref = format_table_ref(table)
    return (
        f"CREATE MULTISET TABLE {table_ref} ("
        "id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        f"user_id VARCHAR(128) NOT NULL{owner_line}, "
        f"state {json_column_type} NOT NULL, "
        "create_time TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
        "update_time TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'id')} PRIMARY KEY (id)"
        ");"
    )


def _sessions_index_specs(table: str) -> "list[tuple[str, str, str]]":
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_app_user", table, "app_name, user_id"),
        (f"idx_{table_name}_update_time", table, "update_time DESC"),
    ]


def _events_table_ddl(table: str, session_table: str, json_column_type: str) -> str:
    table_ref = format_table_ref(table)
    session_ref = format_table_ref(session_table)
    return (
        f"CREATE MULTISET TABLE {table_ref} ("
        "id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, "
        "invocation_id VARCHAR(256) NOT NULL, "
        "timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL, "
        f"event_data {json_column_type} NOT NULL, "
        "actions BLOB NULL, "
        "partial BYTEINT NULL, "
        "turn_complete BYTEINT NULL, "
        "interrupted BYTEINT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'id')} PRIMARY KEY (id), "
        f"CONSTRAINT {_constraint_ref('fk', table, 'session')} FOREIGN KEY (session_id) REFERENCES {session_ref}(id)"
        ");"
    )


def _events_index_specs(table: str) -> "list[tuple[str, str, str]]":
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_scope", table, "app_name, user_id, session_id, timestamp ASC"),
        (f"idx_{table_name}_session", table, "session_id, timestamp ASC"),
        (f"idx_{table_name}_invocation", table, "invocation_id"),
        (f"idx_{table_name}_timestamp", table, "timestamp ASC"),
        (f"idx_{table_name}_app_timestamp", table, "app_name, timestamp ASC"),
    ]


def _app_states_table_ddl(table: str, json_column_type: str) -> str:
    table_ref = format_table_ref(table)
    return (
        f"CREATE MULTISET TABLE {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        f"state {json_column_type} NOT NULL, "
        "update_time TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'app_name')} PRIMARY KEY (app_name)"
        ");"
    )


def _user_states_table_ddl(table: str, json_column_type: str) -> str:
    table_ref = format_table_ref(table)
    return (
        f"CREATE MULTISET TABLE {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        f"state {json_column_type} NOT NULL, "
        "update_time TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'app_user')} PRIMARY KEY (app_name, user_id)"
        ");"
    )


def _metadata_table_ddl(table: str) -> str:
    table_ref = format_table_ref(table)
    return (
        f"CREATE MULTISET TABLE {table_ref} ("
        '"key" VARCHAR(128) NOT NULL, '
        "value VARCHAR(512) NOT NULL, "
        f'CONSTRAINT {_constraint_ref("pk", table, "key")} PRIMARY KEY ("key")'
        ");"
    )


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    columns = re.sub(r"\s+(?:ASC|DESC)\b", "", columns, flags=re.IGNORECASE)
    return f"CREATE INDEX {format_identifier(index_name)} ({columns}) ON {format_table_ref(table)}"


def _insert_event_sql(table: str) -> str:
    return f"""
    INSERT INTO {format_table_ref(table)} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """


def _upsert_state_sql(table: str, key_columns: tuple[str, ...]) -> str:
    columns = (*key_columns, "state")
    source = ", ".join(
        f"CAST(? AS {'JSON' if col == 'state' else 'VARCHAR(128)'}) AS {format_identifier(col)}" for col in columns
    )
    join = " AND ".join(f"t.{format_identifier(col)} = s.{format_identifier(col)}" for col in key_columns)
    names = ", ".join(format_identifier(col) for col in (*columns, "update_time"))
    values = ", ".join(f"s.{format_identifier(col)}" for col in columns)
    return (
        f"MERGE INTO {format_table_ref(table)} AS t USING (SELECT {source}) AS s ON {join} "
        "WHEN MATCHED THEN UPDATE SET state = s.state, update_time = CURRENT_TIMESTAMP(6) "
        f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values}, CURRENT_TIMESTAMP(6))"
    )


def _upsert_metadata_sql(table: str) -> str:
    return (
        f'MERGE INTO {format_table_ref(table)} AS t USING (SELECT CAST(? AS VARCHAR(128)) AS "key", '
        'CAST(? AS VARCHAR(512)) AS value) AS s ON t."key" = s."key" '
        "WHEN MATCHED THEN UPDATE SET value = s.value "
        'WHEN NOT MATCHED THEN INSERT ("key", value) VALUES (s."key", s.value)'
    )


def _events_query(
    table: str,
    session_table: str,
    app_name: str,
    user_id: str,
    session_id: str,
    after_timestamp: "datetime | None",
    limit: "int | None",
) -> "tuple[str, tuple[Any, ...]]":
    params: list[Any] = [app_name, user_id, session_id]
    after_clause = ""
    if after_timestamp is not None:
        after_clause = " AND e.timestamp > ?"
        params.append(after_timestamp)
    sql = f"""
    SELECT {f"TOP {int(limit)} " if limit is not None else ""}e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data
    FROM {format_table_ref(table)} e
    JOIN {format_table_ref(session_table)} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
    WHERE s.app_name = ? AND s.user_id = ? AND s.id = ?{after_clause}
    ORDER BY e.timestamp ASC
    """
    return sql, tuple(params)


def _event_insert_params(event_record: StoredEvent) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        event_record["timestamp"],
        to_json(event_record["event_data"]),
    )


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, dict):
        if key in row:
            return row[key]
        upper_key = key.upper()
        if upper_key in row:
            return row[upper_key]
        return None
    if isinstance(row, (list, tuple)) and len(row) > index:
        return row[index]
    return getattr(row, key, None)


def _session_record_from_row(row: Any) -> StoredSession:
    return StoredSession(
        id=_row_value(row, "id", 0),
        app_name=_row_value(row, "app_name", 1),
        user_id=_row_value(row, "user_id", 2),
        state=_json_dict(_row_value(row, "state", 3)),
        create_time=_row_value(row, "create_time", 4),
        update_time=_row_value(row, "update_time", 5),
    )


def _event_record_from_row(row: Any) -> StoredEvent:
    return StoredEvent(
        id=_row_value(row, "id", 0),
        app_name=_row_value(row, "app_name", 1),
        user_id=_row_value(row, "user_id", 2),
        session_id=_row_value(row, "session_id", 3),
        invocation_id=_row_value(row, "invocation_id", 4),
        timestamp=_row_value(row, "timestamp", 5),
        event_data=_json_dict(_row_value(row, "event_data", 6)),
    )


def _memory_record_from_row(row: Any) -> "StoredMemory":
    metadata_raw = _row_value(row, "metadata_json", 10)
    return cast(
        "StoredMemory",
        {
            "id": _row_value(row, "id", 0),
            "session_id": _row_value(row, "session_id", 1),
            "app_name": _row_value(row, "app_name", 2),
            "user_id": _row_value(row, "user_id", 3),
            "scope": _row_value(row, "scope", 4),
            "event_id": _row_value(row, "event_id", 5),
            "author": _row_value(row, "author", 6),
            "timestamp": _row_value(row, "timestamp", 7),
            "content_json": _json_dict(_row_value(row, "content_json", 8)),
            "content_text": _row_value(row, "content_text", 9),
            "metadata_json": _json_dict(metadata_raw) if metadata_raw is not None else None,
            "inserted_at": _row_value(row, "inserted_at", 11),
            "embedding": None,
        },
    )


def _json_dict(value: Any) -> "dict[str, Any]":
    if value is None:
        return {}
    if isinstance(value, dict):
        return cast("dict[str, Any]", value)
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        return cast("dict[str, Any]", from_json(value))
    return cast("dict[str, Any]", from_json(str(value)))


def _is_teradata_table_missing(exc: BaseException) -> bool:
    return extract_error_code(exc) == TERADATA_TABLE_NOT_FOUND_SQLCODE


def _constraint_ref(prefix: str, table: str, suffix: str) -> str:
    _, table_name = split_schema_and_table(table)
    return format_identifier(f"{prefix}_{table_name}_{suffix}")


def _validate_event_record_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
    record_scope = (event_record["app_name"], event_record["user_id"], event_record["session_id"])
    expected_scope = (app_name, user_id, session_id)
    if record_scope != expected_scope:
        msg = f"event_record scope {record_scope!r} does not match {expected_scope!r}"
        raise ValueError(msg)


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _build_teradata_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = 'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = 'app'", (app_name,)


def _escape_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _session_list_query(
    table: str, app_name: str, user_id: str | None, column: str, direction: str, limit: int | None, offset: int
) -> tuple[str, tuple[Any, ...]]:
    params: list[Any] = [app_name]
    where = "app_name = ?"
    if user_id is not None:
        where += " AND user_id = ?"
        params.append(user_id)
    order = f"{column} {direction}, id {direction}"
    top = f"TOP {int(limit)} " if limit is not None and not offset else ""
    page = ""
    if offset:
        if limit is None:
            page = f" QUALIFY ROW_NUMBER() OVER (ORDER BY {order}) >= ?"
            params.append(offset + 1)
        else:
            page = f" QUALIFY ROW_NUMBER() OVER (ORDER BY {order}) BETWEEN ? AND ?"
            params.extend((offset + 1, offset + limit))
    return (
        (
            f"SELECT {top}id, app_name, user_id, state, create_time, update_time FROM {format_table_ref(table)} "
            f"WHERE {where}{page} ORDER BY {order}"
        ),
        tuple(params),
    )


def _create_missing_table(driver: Any, table_name: str, ddl: str) -> None:
    schema, table = split_schema_and_table(table_name)
    if driver.select_one_or_none(TABLE_EXISTS_SQL, (schema, table)) is None:
        driver.execute(ddl)
        driver.commit()


def _drop_existing_tables(config: "TeradataConfig", drops: "Sequence[DropStatement]") -> None:
    """Run catalog-probed drops in one session; the driver commits each DDL step in manual mode."""
    with config.provide_session() as driver:
        run_drop_statements_sync(driver, drops)


def _create_missing_index(driver: Any, table_name: str, index_name: str, columns: str) -> None:
    schema, table = split_schema_and_table(table_name)
    rows = driver.data_dictionary.get_indexes(driver, table=table, schema=schema)
    if not metadata_has_index(rows, table, index_name, schema_name=schema):
        driver.execute(_create_index_sql(table_name, index_name, columns))


@contextmanager
def _transaction(config: "TeradataConfig") -> Iterator[tuple[Any, Any]]:
    """Keep compound DML atomic and restore autocommit.

    Yields:
        The connection and cursor for the atomic mutation.
    """
    with config.provide_connection() as conn:
        previous = getattr(conn, "autocommit", True)
        conn.autocommit = False
        try:
            with TeradataCursor(conn) as cursor:
                yield conn, cursor
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.autocommit = previous


def _schema_target(table_name: str, ddl: str) -> SchemaTarget:
    schema, table = split_schema_and_table(table_name)
    return SchemaTarget.from_ddl(table, ddl, schema=schema, dialect="vantage")
