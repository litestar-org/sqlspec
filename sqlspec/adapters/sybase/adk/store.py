"""Sybase ASE ADK stores for Google Agent Development Kit session storage."""

import contextlib
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NoReturn, cast

from sqlspec.adapters.sybase._typing import SybaseCursor, SybaseError
from sqlspec.adapters.sybase.core import (
    create_mapped_exception,
    escape_sql_literal,
    extract_error_number,
    quote_bracket_identifier,
    resolve_rowcount,
    split_schema_and_table,
)
from sqlspec.exceptions import NotFoundError, SQLSpecError
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.migrations.schema import SchemaTarget, metadata_has_index
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import timedelta

    from sqlspec.adapters.sybase.config import SybaseConfig
    from sqlspec.adapters.sybase.driver import SybaseDriver
    from sqlspec.extensions.adk import SessionOrderBy
    from sqlspec.extensions.adk.memory._types import StoredMemory

__all__ = ("SybaseADKMemoryStore", "SybaseADKStore")

SYBASE_TABLE_NOT_FOUND_ERROR: Final[int] = 208
SYBASE_SCHEMA: Final[str] = "dbo"
JSON_COLUMN_TYPE: Final[str] = "TEXT"


class SybaseADKStore(BaseSyncADKStore["SybaseConfig"]):
    """Synchronous Sybase ASE ADK session/event store."""

    connector_name: ClassVar[str] = "sybase"
    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(self, config: "SybaseConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create ADK tables (idempotent Sybase ASE DDL) and DD-gated indexes."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            index_specs = self._index_specs()
            existing_indexes = _existing_index_names(driver, index_specs)
            for index_name, index_table, columns in index_specs:
                if _bare_name(index_name) not in existing_indexes:
                    driver.execute(_create_index_sql(index_table, index_name, columns))
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new ADK session."""
        owner_column = f", {quote_bracket_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_param = ", %s" if self._owner_id_column_name else ""
        insert_sql = f"""
        INSERT INTO {_table_ref(self._session_table)} (
            id, app_name, user_id{owner_column}, state, create_time, update_time
        )
        VALUES (%s, %s, %s{owner_param}, %s, GETUTCDATE(), GETUTCDATE())
        """
        select_sql = f"""
        SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
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
                    UPDATE {_table_ref(self._session_table)}
                    SET update_time = GETUTCDATE()
                    WHERE app_name = %s AND user_id = %s AND id = %s
                    """,
                    (app_name, user_id, session_id),
                    commit=True,
                )
            row = self._execute_fetchone(
                f"""
                SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
                FROM {_table_ref(self._session_table)}
                WHERE app_name = %s AND user_id = %s AND id = %s
                """,
                (app_name, user_id, session_id),
            )
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return None
            raise
        return _session_record_from_row(row) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Replace a session's durable state."""
        self._execute(
            f"""
            UPDATE {_table_ref(self._session_table)}
            SET state = %s, update_time = GETUTCDATE()
            WHERE app_name = %s AND user_id = %s AND id = %s
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
            rows = self._execute_fetchall(sql, params)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events."""
        events_sql = (
            f"DELETE FROM {_table_ref(self._events_table)} WHERE app_name = %s AND user_id = %s AND session_id = %s"
        )
        session_sql = f"DELETE FROM {_table_ref(self._session_table)} WHERE app_name = %s AND user_id = %s AND id = %s"
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(events_sql, (app_name, user_id, session_id))
                cursor.execute(session_sql, (app_name, user_id, session_id))
                conn.commit()
            except Exception as exc:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise create_mapped_exception(exc) from exc

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event after verifying session ownership."""
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        ownership_sql = (
            f"SELECT TOP 1 1 FROM {_table_ref(self._session_table)} WHERE id = %s AND app_name = %s AND user_id = %s"
        )
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(ownership_sql, (session_id, app_name, user_id))
                row = cursor.fetchone()
                if row is None:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                    _raise_session_not_found_for_scope(session_id, app_name, user_id)
                cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
                conn.commit()
            except ValueError:
                raise
            except Exception as exc:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise create_mapped_exception(exc) from exc

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
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        select_sql = f"""
        SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(update_sql, (to_json(state), app_name, user_id, session_id))
                cursor.execute(select_sql, (app_name, user_id, session_id))
                row = cursor.fetchone()
                if row is None:
                    _raise_session_not_found(session_id)
                cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
                if app_state is not None:
                    app_json = to_json(app_state)
                    cursor.execute(self._upsert_app_state_sql(), (app_name, app_json, app_name, app_name, app_json))
                if user_state is not None:
                    user_json = to_json(user_state)
                    cursor.execute(
                        self._upsert_user_state_sql(),
                        (app_name, user_id, user_json, app_name, user_id, app_name, user_id, user_json),
                    )
            except Exception:
                conn.rollback()
                raise
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
            rows = self._execute_fetchall(sql, params)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than ``before``."""
        sql = f"DELETE FROM {_table_ref(self._events_table)} WHERE timestamp < %s"
        params: list[Any] = [before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)
        try:
            return self._execute(sql, tuple(params), commit=True)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete sessions whose update_time is older than ``updated_before``."""
        sql = f"DELETE FROM {_table_ref(self._session_table)} WHERE update_time < %s"
        params: list[Any] = [updated_before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)
        try:
            return self._execute(sql, tuple(params), commit=True)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return 0
            raise

    def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user state rows whose update_time is older than ``updated_before``."""
        sql = f"DELETE FROM {_table_ref(self._user_state_table)} WHERE update_time < %s"
        params: list[Any] = [updated_before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)
        try:
            return self._execute(sql, tuple(params), commit=True)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP 1 state FROM {_table_ref(self._app_state_table)} WHERE app_name = %s", (app_name,)
            )
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        try:
            row = self._execute_fetchone(
                f"""
                SELECT TOP 1 state
                FROM {_table_ref(self._user_state_table)}
                WHERE app_name = %s AND user_id = %s
                """,
                (app_name, user_id),
            )
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        encoded = to_json(state)
        self._execute(self._upsert_app_state_sql(), (app_name, encoded, app_name, app_name, encoded), commit=True)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        encoded = to_json(state)
        self._execute(
            self._upsert_user_state_sql(),
            (app_name, user_id, encoded, app_name, user_id, app_name, user_id, encoded),
            commit=True,
        )

    def get_metadata(self, key: str) -> "str | None":
        """Return an ADK metadata value."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP 1 value FROM {_table_ref(self._metadata_table)} WHERE [key] = %s", (key,)
            )
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return None
            raise
        return str(_row_value(row, "value", 0)) if row is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Set an ADK metadata value."""
        self._execute(_upsert_metadata_sql(self._metadata_table), (key, value, key, key, value), commit=True)

    def get_schema_targets_sync(self, driver: Any) -> "list[SchemaTarget]":
        """Return ordered Sybase ASE ADK session-store SchemaTarget definitions."""
        del driver
        return self._schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing Sybase ASE ADK session and event companion indexes without committing."""
        _reconcile_sybase_indexes(driver, self._index_specs())

    def _index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for session and event indexes."""
        return [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]

    def _schema_targets(self) -> "list[SchemaTarget]":
        """Construct Sybase ASE SchemaTarget definitions in FK dependency order."""
        ddl_targets = (
            (
                self._session_table,
                _sessions_create_table_sql(self._session_table, JSON_COLUMN_TYPE, self._owner_id_column_ddl),
                self._sessions_table_ddl(),
            ),
            (
                self._events_table,
                _events_create_table_sql(self._events_table, self._session_table, JSON_COLUMN_TYPE),
                self._events_table_ddl(),
            ),
            (
                self._app_state_table,
                _app_states_create_table_sql(self._app_state_table, JSON_COLUMN_TYPE),
                self._app_states_table_ddl(),
            ),
            (
                self._user_state_table,
                _user_states_create_table_sql(self._user_state_table, JSON_COLUMN_TYPE),
                self._user_states_table_ddl(),
            ),
            (self._metadata_table, _metadata_create_table_sql(self._metadata_table), self._metadata_table_ddl()),
        )
        return [
            _build_sybase_target(table, inner_ddl, create_statement)
            for table, inner_ddl, create_statement in ddl_targets
        ]

    def _sessions_table_ddl(self) -> str:
        """Return Sybase ASE DDL for the ADK session table."""
        return _sessions_table_ddl(self._session_table, JSON_COLUMN_TYPE, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return Sybase ASE DDL for the ADK event table."""
        return _events_table_ddl(self._events_table, self._session_table, JSON_COLUMN_TYPE)

    def _app_states_table_ddl(self) -> str:
        """Return Sybase ASE DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table, JSON_COLUMN_TYPE)

    def _user_states_table_ddl(self) -> str:
        """Return Sybase ASE DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table, JSON_COLUMN_TYPE)

    def _metadata_table_ddl(self) -> str:
        """Return Sybase ASE DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        ref = escape_sql_literal(_table_ref(self._app_state_table))
        return f"IF OBJECT_ID('{ref}') IS NOT NULL EXECUTE('DROP TABLE {ref}')"

    def _drop_user_states_table_sql(self) -> str:
        ref = escape_sql_literal(_table_ref(self._user_state_table))
        return f"IF OBJECT_ID('{ref}') IS NOT NULL EXECUTE('DROP TABLE {ref}')"

    def _drop_metadata_table_sql(self) -> str:
        ref = escape_sql_literal(_table_ref(self._metadata_table))
        return f"IF OBJECT_ID('{ref}') IS NOT NULL EXECUTE('DROP TABLE {ref}')"

    def _drop_tables_sql(self) -> "list[str]":
        events_ref = escape_sql_literal(_table_ref(self._events_table))
        sessions_ref = escape_sql_literal(_table_ref(self._session_table))
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"IF OBJECT_ID('{events_ref}') IS NOT NULL EXECUTE('DROP TABLE {events_ref}')",
            f"IF OBJECT_ID('{sessions_ref}') IS NOT NULL EXECUTE('DROP TABLE {sessions_ref}')",
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

    def _execute_fetchone(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> "Any | None":
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                if commit:
                    conn.commit()
            except Exception as exc:
                if commit:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                raise create_mapped_exception(exc) from exc
            return row

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                return list(cursor.fetchall())
            except Exception as exc:
                raise create_mapped_exception(exc) from exc

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                rowcount = resolve_rowcount(cursor)
                if commit:
                    conn.commit()
            except Exception as exc:
                if commit:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                raise create_mapped_exception(exc) from exc
            return rowcount


class SybaseADKMemoryStore(BaseSyncADKMemoryStore["SybaseConfig"]):
    """Sybase ASE ADK memory store using pymssql."""

    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(self, config: "SybaseConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create the memory table (idempotent Sybase ASE DDL) and DD-gated indexes."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            index_specs = self._memory_index_specs()
            existing_indexes = _existing_index_names(driver, index_specs)
            for index_name, index_table, columns in index_specs:
                if _bare_name(index_name) not in existing_indexes:
                    driver.execute(_create_index_sql(index_table, index_name, columns))
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with event-id deduplication."""
        if not self._enabled:
            msg = "ADK memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0

        owner_column = f", {quote_bracket_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_value = ", %s" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {_table_ref(self._memory_table)} (
            id, session_id, app_name, user_id, scope, event_id, author, timestamp,
            content_json, content_text, metadata_json{owner_column}
        )
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s{owner_value}
        WHERE NOT EXISTS (
            SELECT 1 FROM {_table_ref(self._memory_table)} HOLDLOCK
            WHERE app_name = %s AND user_id = %s AND event_id = %s
        )
        """
        inserted = 0
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                for entry in entries:
                    params: list[Any] = [
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
                    ]
                    if self._owner_id_column_name:
                        params.append(owner_id)
                    params.extend((entry["app_name"], entry["user_id"], entry["event_id"]))
                    cursor.execute(sql, tuple(params))
                    inserted += resolve_rowcount(cursor)
            except Exception as exc:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise create_mapped_exception(exc) from exc
            conn.commit()
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
        limit_value = limit or self._max_results
        where_scope, scope_params = _build_sybase_scope_where(app_name, user_id, scope_filter)
        escaped_query = _escape_like_pattern(query)
        sql = f"""
        SELECT TOP %s
            id, session_id, app_name, user_id, scope, event_id, author, timestamp,
            content_json, content_text, metadata_json, inserted_at
        FROM {_table_ref(self._memory_table)}
        WHERE {where_scope} AND content_text LIKE %s ESCAPE '\\'
        ORDER BY timestamp DESC
        """
        rows = self._execute_fetchall(sql, (limit_value, *scope_params, f"%{escaped_query}%"))
        return [_memory_record_from_row(row) for row in rows]

    def delete_entries_by_session(
        self, session_id: str, app_name: "str | None" = None, user_id: "str | None" = None
    ) -> int:
        """Delete memory entries for a specific session, optionally scoped by owner."""
        clauses = ["session_id = %s"]
        params: list[Any] = [session_id]
        if app_name is not None:
            clauses.append("app_name = %s")
            params.append(app_name)
        if user_id is not None:
            clauses.append("user_id = %s")
            params.append(user_id)
        where_sql = " AND ".join(clauses)
        return self._execute(
            f"DELETE FROM {_table_ref(self._memory_table)} WHERE {where_sql}", tuple(params), commit=True
        )

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than the retention window."""
        clauses = ["inserted_at < DATEADD(day, -%s, GETUTCDATE())"]
        params: list[Any] = [days]
        if app_name is not None:
            clauses.append("app_name = %s")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = %s")
            params.append(scope)
        where_sql = " AND ".join(clauses)
        return self._execute(
            f"DELETE FROM {_table_ref(self._memory_table)} WHERE {where_sql}", tuple(params), commit=True
        )

    def get_schema_targets_sync(self, driver: Any) -> "list[SchemaTarget]":
        """Return ordered Sybase ASE ADK memory SchemaTarget definitions."""
        del driver
        return self._schema_targets()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing Sybase ASE ADK memory companion indexes without committing."""
        if not self._enabled:
            return
        _reconcile_sybase_indexes(driver, self._memory_index_specs())

    def _schema_targets(self) -> "list[SchemaTarget]":
        """Construct the Sybase ASE ADK memory SchemaTarget when memory is enabled."""
        if not self._enabled:
            return []
        inner_ddl = _memory_create_table_sql(self._memory_table, JSON_COLUMN_TYPE, self._owner_id_column_ddl)
        return [_build_sybase_target(self._memory_table, inner_ddl, self._memory_table_ddl())]

    def _memory_table_ddl(self) -> str:
        table_ref = _table_ref(self._memory_table)
        escaped_ref = escape_sql_literal(table_ref)
        create_sql = escape_sql_literal(
            _memory_create_table_sql(self._memory_table, JSON_COLUMN_TYPE, self._owner_id_column_ddl)
        )
        return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"

    def _memory_index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for memory-table indexes."""
        _, table_name = split_schema_and_table(self._memory_table)
        return [
            (f"idx_{table_name}_app_scope_user_time", self._memory_table, "app_name, scope, user_id, timestamp DESC"),
            (f"idx_{table_name}_scope", self._memory_table, "app_name, scope"),
            (f"idx_{table_name}_session", self._memory_table, "session_id"),
            (f"idx_{table_name}_timestamp", self._memory_table, "timestamp DESC"),
        ]

    def _drop_memory_table_sql(self) -> "list[str]":
        ref = escape_sql_literal(_table_ref(self._memory_table))
        return [f"IF OBJECT_ID('{ref}') IS NOT NULL EXECUTE('DROP TABLE {ref}')"]

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                return list(cursor.fetchall())
            except Exception as exc:
                raise create_mapped_exception(exc) from exc

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                rowcount = resolve_rowcount(cursor)
                if commit:
                    conn.commit()
            except Exception as exc:
                if commit:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                raise create_mapped_exception(exc) from exc
            return rowcount


def _build_sybase_target(table: str, inner_ddl: str, create_statement: str) -> SchemaTarget:
    """Build a Sybase ASE SchemaTarget from unwrapped DDL while preserving the guarded CREATE statement."""
    schema_name, bare_table = split_schema_and_table(table)
    parsed = SchemaTarget.from_ddl(bare_table, inner_ddl, schema=schema_name, dialect="sybase")
    return SchemaTarget(
        parsed.table_name,
        parsed.create_table,
        schema=parsed.schema,
        create_statement=create_statement,
        constraint_columns=parsed.constraint_columns,
        unsafe_add_columns=parsed.unsafe_add_columns,
    )


def _reconcile_sybase_indexes(driver: Any, index_specs: "list[tuple[str, str, str]]") -> None:
    """Create missing Sybase ASE ADK companion indexes via catalog probes without committing."""
    checked_tables: dict[tuple[str, str], list[Any]] = {}
    for index_name, index_table, columns in index_specs:
        schema_name, table_name = split_schema_and_table(index_table)
        table_key = (schema_name, table_name)
        rows = checked_tables.get(table_key)
        if rows is None:
            rows = list(driver.data_dictionary.get_indexes(driver, table=table_name, schema=schema_name))
            checked_tables[table_key] = rows
        if not metadata_has_index(rows, table_name, index_name, schema_name=schema_name):
            driver.execute(_create_index_sql(index_table, index_name, columns))


def _existing_index_names(driver: "SybaseDriver", index_specs: "list[tuple[str, str, str]]") -> "set[str]":
    schemas = {split_schema_and_table(index_table)[0] for _, index_table, _ in index_specs} or {SYBASE_SCHEMA}
    existing: set[str] = set()
    for schema_name in schemas:
        existing.update(_casefold_names(driver.data_dictionary.get_indexes(driver, schema=schema_name), "index_name"))
    return existing


def _sessions_create_table_sql(table: str, json_column_type: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f", {owner_id_column_ddl}" if owner_id_column_ddl else ""
    table_ref = _table_ref(table)
    return (
        f"CREATE TABLE {table_ref} ("
        "id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        f"user_id VARCHAR(128) NOT NULL{owner_line}, "
        f"state {json_column_type} NOT NULL, "
        "create_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        "update_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'id')} PRIMARY KEY (id)"
        ")"
    )


def _sessions_table_ddl(table: str, json_column_type: str, owner_id_column_ddl: "str | None") -> str:
    escaped_ref = escape_sql_literal(_table_ref(table))
    create_sql = escape_sql_literal(_sessions_create_table_sql(table, json_column_type, owner_id_column_ddl))
    return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"


def _sessions_index_specs(table: str) -> "list[tuple[str, str, str]]":
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_app_user", table, "app_name, user_id"),
        (f"idx_{table_name}_update_time", table, "update_time DESC"),
    ]


def _events_create_table_sql(table: str, session_table: str, json_column_type: str) -> str:
    table_ref = _table_ref(table)
    session_ref = _table_ref(session_table)
    return (
        f"CREATE TABLE {table_ref} ("
        "id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, "
        "invocation_id VARCHAR(256) NOT NULL, "
        "timestamp BIGDATETIME NOT NULL, "
        f"event_data {json_column_type} NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'id')} PRIMARY KEY (id), "
        f"CONSTRAINT {_constraint_ref('fk', table, 'session')} FOREIGN KEY (session_id) REFERENCES {session_ref}(id)"
        ")"
    )


def _events_table_ddl(table: str, session_table: str, json_column_type: str) -> str:
    escaped_ref = escape_sql_literal(_table_ref(table))
    create_sql = escape_sql_literal(_events_create_table_sql(table, session_table, json_column_type))
    return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"


def _events_index_specs(table: str) -> "list[tuple[str, str, str]]":
    _, table_name = split_schema_and_table(table)
    return [
        (f"idx_{table_name}_scope", table, "app_name, user_id, session_id, timestamp ASC"),
        (f"idx_{table_name}_session", table, "session_id, timestamp ASC"),
        (f"idx_{table_name}_invocation", table, "invocation_id"),
        (f"idx_{table_name}_timestamp", table, "timestamp ASC"),
        (f"idx_{table_name}_app_timestamp", table, "app_name, timestamp ASC"),
    ]


def _app_states_create_table_sql(table: str, json_column_type: str) -> str:
    table_ref = _table_ref(table)
    return (
        f"CREATE TABLE {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        f"state {json_column_type} NOT NULL, "
        "update_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'app_name')} PRIMARY KEY (app_name)"
        ")"
    )


def _app_states_table_ddl(table: str, json_column_type: str) -> str:
    escaped_ref = escape_sql_literal(_table_ref(table))
    create_sql = escape_sql_literal(_app_states_create_table_sql(table, json_column_type))
    return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"


def _user_states_create_table_sql(table: str, json_column_type: str) -> str:
    table_ref = _table_ref(table)
    return (
        f"CREATE TABLE {table_ref} ("
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        f"state {json_column_type} NOT NULL, "
        "update_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'app_user')} PRIMARY KEY (app_name, user_id)"
        ")"
    )


def _user_states_table_ddl(table: str, json_column_type: str) -> str:
    escaped_ref = escape_sql_literal(_table_ref(table))
    create_sql = escape_sql_literal(_user_states_create_table_sql(table, json_column_type))
    return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"


def _metadata_create_table_sql(table: str) -> str:
    table_ref = _table_ref(table)
    return (
        f"CREATE TABLE {table_ref} ("
        "[key] VARCHAR(128) NOT NULL, "
        "value VARCHAR(512) NOT NULL, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'key')} PRIMARY KEY ([key])"
        ")"
    )


def _metadata_table_ddl(table: str) -> str:
    escaped_ref = escape_sql_literal(_table_ref(table))
    create_sql = escape_sql_literal(_metadata_create_table_sql(table))
    return f"IF OBJECT_ID('{escaped_ref}') IS NULL EXECUTE('{create_sql}');"


def _memory_create_table_sql(table: str, json_column_type: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f", {owner_id_column_ddl}" if owner_id_column_ddl else ""
    table_ref = _table_ref(table)
    return (
        f"CREATE TABLE {table_ref} ("
        "id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "scope VARCHAR(16) DEFAULT 'user' NOT NULL, "
        "event_id VARCHAR(128) NOT NULL, "
        "author VARCHAR(256) NULL, "
        "timestamp BIGDATETIME NOT NULL, "
        f"content_json {json_column_type} NOT NULL, "
        "content_text TEXT NOT NULL, "
        f"metadata_json {json_column_type} NULL, "
        f"inserted_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL{owner_line}, "
        f"CONSTRAINT {_constraint_ref('pk', table, 'id')} PRIMARY KEY (id), "
        f"CONSTRAINT {_constraint_ref('uq', table, 'event_id')} UNIQUE (app_name, user_id, event_id)"
        ")"
    )


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    return f"CREATE INDEX {quote_bracket_identifier(index_name)} ON {_table_ref(table)} ({columns})"


def _casefold_names(rows: "list[Any]", key: str) -> "set[str]":
    """Collapse data-dictionary rows into a case-folded, schema-stripped name set."""
    return {str(row.get(key, "")).rsplit(".", 1)[-1].casefold() for row in rows}


def _bare_name(name: str) -> str:
    """Return the case-folded, schema-stripped object name for membership checks."""
    return name.rsplit(".", 1)[-1].casefold()


def _insert_event_sql(table: str) -> str:
    return f"""
    INSERT INTO {_table_ref(table)} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """


def _upsert_state_sql(table: str, key_columns: "tuple[str, ...]") -> str:
    table_ref = _table_ref(table)
    where_clause = " AND ".join(f"{quote_bracket_identifier(col)} = %s" for col in key_columns)
    insert_columns = ", ".join(quote_bracket_identifier(col) for col in (*key_columns, "state", "update_time"))
    insert_placeholders = ", ".join(["%s" for _ in key_columns] + ["%s", "GETUTCDATE()"])
    return f"""
    IF EXISTS (SELECT 1 FROM {table_ref} HOLDLOCK WHERE {where_clause})
        UPDATE {table_ref} SET state = %s, update_time = GETUTCDATE() WHERE {where_clause}
    ELSE
        INSERT INTO {table_ref} ({insert_columns}) VALUES ({insert_placeholders})
    """


def _upsert_metadata_sql(table: str) -> str:
    table_ref = _table_ref(table)
    return f"""
    IF EXISTS (SELECT 1 FROM {table_ref} HOLDLOCK WHERE [key] = %s)
        UPDATE {table_ref} SET value = %s WHERE [key] = %s
    ELSE
        INSERT INTO {table_ref} ([key], value) VALUES (%s, %s)
    """


def _events_query(
    events_table: str,
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
        after_clause = " AND e.timestamp > %s"
        params.append(after_timestamp)
    top_clause = ""
    if limit is not None:
        top_clause = "TOP %s "
        params.insert(0, limit)
    sql = f"""
    SELECT {top_clause}e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data
    FROM {_table_ref(events_table)} e
    JOIN {_table_ref(session_table)} s
        ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
    WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s{after_clause}
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


def _is_sybase_table_missing(exc: BaseException) -> bool:
    target = getattr(exc, "__cause__", None) or exc
    text = f"{exc!s} {target!s}".lower()
    return (
        "not found" in text
        or "invalid object name" in text
        or extract_error_number(exc) == SYBASE_TABLE_NOT_FOUND_ERROR
        or extract_error_number(target) == SYBASE_TABLE_NOT_FOUND_ERROR
        or isinstance(exc, NotFoundError)
        or isinstance(target, NotFoundError)
    )


def _table_ref(table: str) -> str:
    schema_name, table_name = split_schema_and_table(table)
    return f"{quote_bracket_identifier(schema_name)}.{quote_bracket_identifier(table_name)}"


def _constraint_ref(prefix: str, table: str, suffix: str) -> str:
    _, table_name = split_schema_and_table(table)
    return quote_bracket_identifier(f"{prefix}_{table_name}_{suffix}")


def _validate_event_record_scope(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
    if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
        app_name,
        user_id,
        session_id,
    ):
        msg = (
            "event_record scope "
            f"({event_record['app_name']!r}, {event_record['user_id']!r}, {event_record['session_id']!r}) "
            f"does not match ({app_name!r}, {user_id!r}, {session_id!r})"
        )
        raise ValueError(msg)


def _raise_session_not_found(session_id: str) -> NoReturn:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _raise_session_not_found_for_scope(session_id: str, app_name: str, user_id: str) -> NoReturn:
    msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
    raise ValueError(msg)


def _build_sybase_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = %s AND ((scope = 'user' AND user_id = %s) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = %s AND scope = 'user' AND user_id = %s", (app_name, user_id)
    return "app_name = %s AND scope = 'app'", (app_name,)


def _escape_like_pattern(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("[", "\\[")


def _session_list_query(
    session_table: str,
    app_name: str,
    user_id: "str | None",
    column: str,
    direction: str,
    limit: "int | None",
    offset: int,
) -> "tuple[str, tuple[Any, ...]]":
    """Return the bounded Sybase ASE session-list query and its bound values."""
    params: list[Any] = [app_name]
    where_clause = "app_name = %s"
    if user_id is not None:
        params.append(user_id)
        where_clause = f"{where_clause} AND user_id = %s"

    top_clause = ""
    page_clause = ""
    if limit is not None and offset == 0:
        top_clause = "TOP %s "
        params.insert(0, limit)
    elif limit is not None:
        params.extend((limit, offset))
        page_clause = "\n    ROWS LIMIT %s OFFSET %s"

    sql = f"""
    SELECT {top_clause}id, app_name, user_id, state, create_time, update_time
    FROM {_table_ref(session_table)}
    WHERE {where_clause}
    ORDER BY {column} {direction}, id {direction}{page_clause}
    """
    return sql, tuple(params)
