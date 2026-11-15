"""Sybase ASE ADK stores for Google Agent Development Kit session storage."""

import contextlib
import secrets
import time
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
from sqlspec.exceptions import DeadlockError, NotFoundError, SQLSpecError, UniqueViolationError
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.converters import merge_scoped_state
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.migrations.schema import SchemaTarget, metadata_has_index
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import timedelta

    from sqlspec.adapters.sybase.config import SybaseConfig
    from sqlspec.adapters.sybase.driver import SybaseDriver
    from sqlspec.extensions.adk import SessionOrderBy
    from sqlspec.extensions.adk.memory._types import StoredMemory

__all__ = ("SybaseADKMemoryStore", "SybaseADKStore")

SYBASE_TABLE_NOT_FOUND_ERROR: Final[int] = 208
SYBASE_SCHEMA: Final[str] = "dbo"
JSON_COLUMN_TYPE: Final[str] = "TEXT"
_SYBASE_RETRY_MAX_ATTEMPTS: Final[int] = 5
_SYBASE_RETRY_BASE_SECONDS: Final[float] = 0.002
_SYBASE_RETRY_MAX_SECONDS: Final[float] = 0.05


def _exact_key_predicate(column: str, parameter: str = "%s") -> str:
    return (
        f"{column} = {parameter} AND CONVERT(VARBINARY(512), {column}) = "
        f"CONVERT(VARBINARY(512), CONVERT(VARCHAR(128), {parameter}))"
    )


def _exact_column_predicate(left: str, right: str) -> str:
    return f"{left} = {right} AND CONVERT(VARBINARY(512), {left}) = CONVERT(VARBINARY(512), {right})"


def _exact_where(*columns: str) -> str:
    return " AND ".join(_exact_key_predicate(column) for column in columns)


def _exact_params(*values: Any) -> "tuple[Any, ...]":
    return tuple(item for value in values for item in (value, value))


_APP_KEY_WHERE: Final = _exact_where("app_name")
_USER_KEY_WHERE: Final = _exact_where("app_name", "user_id")
_SESSION_KEY_WHERE: Final = _exact_where("app_name", "user_id", "id")
_EVENT_SESSION_KEY_WHERE: Final = _exact_where("app_name", "user_id", "session_id")


class SybaseADKStore(BaseSyncADKStore["SybaseConfig"]):
    """Synchronous Sybase ASE ADK session/event store."""

    connector_name: ClassVar[str] = "sybase"
    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(self, config: "SybaseConfig", *, read_config: "SybaseConfig | None" = None) -> None:
        super().__init__(config, read_config=read_config)

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
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: "dict[str, Any]",
        owner_id: "Any | None" = None,
        *,
        app_state_delta: "dict[str, Any] | None" = None,
        user_state_delta: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Create a new ADK session and atomically apply/read scoped state."""
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
        WHERE {_SESSION_KEY_WHERE}
        """
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            params = (session_id, app_name, user_id, owner_id, to_json(state))
        else:
            params = (session_id, app_name, user_id, to_json(state))

        app_lock_sql = f"UPDATE {_table_ref(self._app_state_table)} SET state = state WHERE {_APP_KEY_WHERE}"
        app_read_sql = f"SELECT state FROM {_table_ref(self._app_state_table)} WHERE {_APP_KEY_WHERE}"
        app_update_sql = f"""
        UPDATE {_table_ref(self._app_state_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE {_APP_KEY_WHERE}
        """
        app_insert_sql = f"""
        INSERT INTO {_table_ref(self._app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, GETUTCDATE())
        """

        user_lock_sql = f"UPDATE {_table_ref(self._user_state_table)} SET state = state WHERE {_USER_KEY_WHERE}"
        user_read_sql = f"SELECT state FROM {_table_ref(self._user_state_table)} WHERE {_USER_KEY_WHERE}"
        user_update_sql = f"""
        UPDATE {_table_ref(self._user_state_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE {_USER_KEY_WHERE}
        """
        user_insert_sql = f"""
        INSERT INTO {_table_ref(self._user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, GETUTCDATE())
        """

        def _operation(cursor: Any, mark_scoped: "Callable[[], None]") -> StoredSession:
            cursor.execute(insert_sql, params)
            cursor.execute(select_sql, _exact_params(app_name, user_id, session_id))
            row = _require_created_session_row(cursor.fetchone())
            mark_scoped()

            merged_app_state: dict[str, Any] | None
            if app_state_delta:
                merged_app_state = _apply_state_delta(
                    cursor, app_lock_sql, app_read_sql, app_update_sql, app_insert_sql, (app_name,), app_state_delta
                )
            else:
                cursor.execute(app_read_sql, _exact_params(app_name))
                app_row = cursor.fetchone()
                merged_app_state = _json_dict(_row_value(app_row, "state", 0)) if app_row is not None else None

            merged_user_state: dict[str, Any] | None
            if user_state_delta:
                merged_user_state = _apply_state_delta(
                    cursor,
                    user_lock_sql,
                    user_read_sql,
                    user_update_sql,
                    user_insert_sql,
                    (app_name, user_id),
                    user_state_delta,
                )
            else:
                cursor.execute(user_read_sql, _exact_params(app_name, user_id))
                user_row = cursor.fetchone()
                merged_user_state = _json_dict(_row_value(user_row, "state", 0)) if user_row is not None else None

            session = _session_record_from_row(row)
            session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
            return session

        return _run_transaction_with_retry(
            self._config, _operation, has_deltas=bool(app_state_delta or user_state_delta)
        )

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
                    WHERE {_SESSION_KEY_WHERE}
                    """,
                    _exact_params(app_name, user_id, session_id),
                    commit=True,
                    read=renew_for is None,
                )
            row = self._execute_fetchone(
                f"""
                SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
                FROM {_table_ref(self._session_table)}
                WHERE {_SESSION_KEY_WHERE}
                """,
                _exact_params(app_name, user_id, session_id),
                read=renew_for is None,
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
            WHERE {_SESSION_KEY_WHERE}
            """,
            (to_json(state), *_exact_params(app_name, user_id, session_id)),
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
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its associated events."""
        events_sql = f"DELETE FROM {_table_ref(self._events_table)} WHERE {_EVENT_SESSION_KEY_WHERE}"
        session_sql = f"DELETE FROM {_table_ref(self._session_table)} WHERE {_SESSION_KEY_WHERE}"
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(events_sql, _exact_params(app_name, user_id, session_id))
                cursor.execute(session_sql, _exact_params(app_name, user_id, session_id))
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
            f"SELECT TOP 1 1 FROM {_table_ref(self._session_table)} WHERE {_exact_where('id', 'app_name', 'user_id')}"
        )
        with self._config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(ownership_sql, _exact_params(session_id, app_name, user_id))
                row = cursor.fetchone()
                if row is None:
                    with contextlib.suppress(Exception):
                        conn.rollback()
                    _raise_session_not_found_for_user(session_id, app_name, user_id)
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
        app_state_delta: "dict[str, Any] | None" = None,
        user_state_delta: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Atomically append an event and merge scoped state deltas."""
        _validate_event_belongs_to_session(event_record, app_name, user_id, session_id)
        update_sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE {_SESSION_KEY_WHERE}
        """
        select_sql = f"""
        SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE {_SESSION_KEY_WHERE}
        """
        app_lock_sql = f"UPDATE {_table_ref(self._app_state_table)} SET state = state WHERE {_APP_KEY_WHERE}"
        app_read_sql = f"SELECT state FROM {_table_ref(self._app_state_table)} WHERE {_APP_KEY_WHERE}"
        app_update_sql = f"""
        UPDATE {_table_ref(self._app_state_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE {_APP_KEY_WHERE}
        """
        app_insert_sql = f"""
        INSERT INTO {_table_ref(self._app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, GETUTCDATE())
        """
        user_lock_sql = f"UPDATE {_table_ref(self._user_state_table)} SET state = state WHERE {_USER_KEY_WHERE}"
        user_read_sql = f"SELECT state FROM {_table_ref(self._user_state_table)} WHERE {_USER_KEY_WHERE}"
        user_update_sql = f"""
        UPDATE {_table_ref(self._user_state_table)}
        SET state = %s, update_time = GETUTCDATE()
        WHERE {_USER_KEY_WHERE}
        """
        user_insert_sql = f"""
        INSERT INTO {_table_ref(self._user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, GETUTCDATE())
        """

        def _operation(cursor: Any, mark_scoped: "Callable[[], None]") -> StoredSession:
            cursor.execute(update_sql, (to_json(state), *_exact_params(app_name, user_id, session_id)))
            cursor.execute(select_sql, _exact_params(app_name, user_id, session_id))
            row = cursor.fetchone()
            if row is None:
                _raise_session_not_found(session_id)
            cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            mark_scoped()
            merged_app_state = (
                _apply_state_delta(
                    cursor, app_lock_sql, app_read_sql, app_update_sql, app_insert_sql, (app_name,), app_state_delta
                )
                if app_state_delta
                else None
            )
            merged_user_state = (
                _apply_state_delta(
                    cursor,
                    user_lock_sql,
                    user_read_sql,
                    user_update_sql,
                    user_insert_sql,
                    (app_name, user_id),
                    user_state_delta,
                )
                if user_state_delta
                else None
            )
            session = _session_record_from_row(row)
            if merged_app_state is not None or merged_user_state is not None:
                session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
            return session

        return _run_transaction_with_retry(
            self._config, _operation, has_deltas=bool(app_state_delta or user_state_delta)
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        """Return events for a scoped session ordered chronologically ascending."""
        if limit == 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = self._execute_fetchall(sql, params, read=True)
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return []
            raise
        if limit is not None:
            rows = list(reversed(rows))
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than ``before``."""
        sql = f"DELETE FROM {_table_ref(self._events_table)} WHERE timestamp < %s"
        params: list[Any] = [before]
        if app_name is not None:
            sql += f" AND {_exact_key_predicate('app_name')}"
            params.extend(_exact_params(app_name))
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
            sql += f" AND {_exact_key_predicate('app_name')}"
            params.extend(_exact_params(app_name))
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
            sql += f" AND {_exact_key_predicate('app_name')}"
            params.extend(_exact_params(app_name))
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
                f"SELECT TOP 1 state FROM {_table_ref(self._app_state_table)} WHERE {_APP_KEY_WHERE}",
                _exact_params(app_name),
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
                WHERE {_USER_KEY_WHERE}
                """,
                _exact_params(app_name, user_id),
            )
        except (SybaseError, SQLSpecError) as exc:
            if _is_sybase_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        encoded = to_json(state)
        self._execute(
            self._upsert_app_state_sql(),
            (*_exact_params(app_name), encoded, *_exact_params(app_name), app_name, encoded),
            commit=True,
        )

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        encoded = to_json(state)
        self._execute(
            self._upsert_user_state_sql(),
            (*_exact_params(app_name, user_id), encoded, *_exact_params(app_name, user_id), app_name, user_id, encoded),
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

    def _execute_fetchone(
        self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False, read: bool = False
    ) -> "Any | None":
        with self._provide_replica_connection(primary=not read) as conn, SybaseCursor(conn, as_dict=False) as cursor:
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

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = (), *, read: bool = False) -> "list[Any]":
        with self._provide_replica_connection(primary=not read) as conn, SybaseCursor(conn, as_dict=False) as cursor:
            try:
                cursor.execute(sql, params)
                return list(cursor.fetchall())
            except Exception as exc:
                raise create_mapped_exception(exc) from exc

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False, read: bool = False) -> int:
        with self._provide_replica_connection(primary=not read) as conn, SybaseCursor(conn, as_dict=False) as cursor:
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

    def __init__(self, config: "SybaseConfig", *, read_config: "SybaseConfig | None" = None) -> None:
        super().__init__(config, read_config=read_config)

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
            WHERE {_exact_where("app_name", "user_id", "event_id")}
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
                    params.extend(_exact_params(entry["app_name"], entry["user_id"], entry["event_id"]))
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
        rows = self._execute_fetchall(sql, (limit_value, *scope_params, f"%{escaped_query}%"), read=True)
        return [_memory_record_from_row(row) for row in rows]

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a specific session, owned by an app and user."""
        where_sql = _exact_where("session_id", "app_name", "user_id")
        return self._execute(
            f"DELETE FROM {_table_ref(self._memory_table)} WHERE {where_sql}",
            _exact_params(session_id, app_name, user_id),
            commit=True,
        )

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than the retention window."""
        clauses = ["inserted_at < DATEADD(day, -%s, GETUTCDATE())"]
        params: list[Any] = [days]
        if app_name is not None:
            clauses.append(_exact_key_predicate("app_name"))
            params.extend(_exact_params(app_name))
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

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = (), *, read: bool = False) -> "list[Any]":
        with self._provide_replica_connection(primary=not read) as conn, SybaseCursor(conn, as_dict=False) as cursor:
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
    where_clause = " AND ".join(_exact_key_predicate(quote_bracket_identifier(col)) for col in key_columns)
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
    params: list[Any] = list(_exact_params(app_name, user_id, session_id))
    after_clause = ""
    if after_timestamp is not None:
        after_clause = " AND e.timestamp > %s"
        params.append(after_timestamp)
    top_clause = ""
    if limit is not None:
        top_clause = "TOP %s "
        params.insert(0, limit)
    order_direction = "DESC" if limit is not None else "ASC"
    sql = f"""
    SELECT {top_clause}e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data
    FROM {_table_ref(events_table)} e
    JOIN {_table_ref(session_table)} s
        ON {_exact_column_predicate("e.session_id", "s.id")}
            AND {_exact_column_predicate("e.app_name", "s.app_name")}
            AND {_exact_column_predicate("e.user_id", "s.user_id")}
    WHERE {_exact_key_predicate("s.app_name")} AND {_exact_key_predicate("s.user_id")}
        AND {_exact_key_predicate("s.id")}{after_clause}
    ORDER BY e.timestamp {order_direction}
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


def _validate_event_belongs_to_session(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
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


def _require_created_session_row(row: Any) -> Any:
    if row is None:
        msg = "Failed to fetch created session"
        raise RuntimeError(msg)
    return row


def _raise_session_not_found_for_user(session_id: str, app_name: str, user_id: str) -> NoReturn:
    msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
    raise ValueError(msg)


def _build_sybase_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        user_scope = f"(scope = 'user' AND {_exact_key_predicate('user_id')}) OR scope = 'app'"
        return f"{_exact_key_predicate('app_name')} AND ({user_scope})", _exact_params(app_name, user_id)
    if scope_filter == "user":
        return (
            f"{_exact_key_predicate('app_name')} AND scope = 'user' AND {_exact_key_predicate('user_id')}",
            _exact_params(app_name, user_id),
        )
    return f"{_exact_key_predicate('app_name')} AND scope = 'app'", _exact_params(app_name)


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
    params: list[Any] = list(_exact_params(app_name))
    where_clause = _exact_key_predicate("app_name")
    if user_id is not None:
        params.extend(_exact_params(user_id))
        where_clause = f"{where_clause} AND {_exact_key_predicate('user_id')}"

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


def _apply_state_delta(
    cursor: Any,
    lock_sql: str,
    read_sql: str,
    update_sql: str,
    insert_sql: str,
    key_params: "tuple[Any, ...]",
    delta: "dict[str, Any]",
) -> "dict[str, Any]":
    """Acquire an exclusive row lock via a no-op UPDATE, then read, merge, and persist scoped state."""
    predicate_params = _exact_params(*key_params)
    cursor.execute(lock_sql, predicate_params)
    cursor.execute(read_sql, predicate_params)
    row = cursor.fetchone()
    if row is not None:
        merged_state = {**_json_dict(_row_value(row, "state", 0)), **delta}
        cursor.execute(update_sql, (to_json(merged_state), *predicate_params))
        return merged_state
    merged_state = dict(delta)
    cursor.execute(insert_sql, (*key_params, to_json(merged_state)))
    return merged_state


def _rollback_transaction(conn: Any, cursor: Any, explicit_tx: bool) -> None:
    with contextlib.suppress(Exception):
        if explicit_tx:
            cursor.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
        else:
            conn.rollback()


def _calculate_backoff_seconds(attempt: int) -> float:
    base = min(_SYBASE_RETRY_MAX_SECONDS, _SYBASE_RETRY_BASE_SECONDS * (attempt + 1))
    jitter = secrets.randbelow(80) / 10_000.0
    return base + jitter


def _run_transaction_with_retry(
    config: Any, operation: "Callable[[Any, Callable[[], None]], StoredSession]", *, has_deltas: bool
) -> StoredSession:
    """Run a Sybase ADK transaction with bounded retry on deadlock or scoped-insert race."""
    for attempt in range(_SYBASE_RETRY_MAX_ATTEMPTS):
        scoped_started = False

        def _mark_state_writes_started() -> None:
            nonlocal scoped_started
            scoped_started = True

        try:
            with config.provide_connection() as conn, SybaseCursor(conn, as_dict=False) as cursor:
                explicit_tx = getattr(conn, "autocommit_state", None) is True
                if explicit_tx:
                    cursor.execute("BEGIN TRANSACTION")
                try:
                    result = operation(cursor, _mark_state_writes_started)
                    if explicit_tx:
                        cursor.execute("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")
                    else:
                        conn.commit()
                except (ValueError, RuntimeError):
                    _rollback_transaction(conn, cursor, explicit_tx)
                    raise
                except Exception as exc:
                    _rollback_transaction(conn, cursor, explicit_tx)
                    mapped = exc if isinstance(exc, SQLSpecError) else create_mapped_exception(exc)
                    if mapped is exc:
                        raise
                    raise mapped from exc
                return result
        except (DeadlockError, UniqueViolationError) as exc:
            retryable = isinstance(exc, DeadlockError) or (scoped_started and has_deltas)
            if not retryable or attempt + 1 >= _SYBASE_RETRY_MAX_ATTEMPTS:
                raise
            time.sleep(_calculate_backoff_seconds(attempt))
    msg = "Failed to complete Sybase ADK transaction after retries."
    raise DeadlockError(msg)
