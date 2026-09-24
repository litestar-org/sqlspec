"""arrow-odbc ADK stores for Google Agent Development Kit session storage."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, cast

from typing_extensions import NotRequired

from sqlspec.adapters.arrow_odbc.core import (
    DB2_INDEX_EXISTS_SQL,
    DB2_TABLE_EXISTS_SQL,
    db2_timestamp_text,
    extract_native_error_number,
    split_db2_name,
)
from sqlspec.config import ADKConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.memory import BaseSyncADKMemoryStore, StoredMemory
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import timedelta

    from sqlspec.adapters.arrow_odbc.config import ArrowOdbcConfig
    from sqlspec.extensions.adk import SessionOrderBy
else:
    ArrowOdbcConfig = Any


__all__ = ("ArrowOdbcADKConfig", "ArrowOdbcADKMemoryStore", "ArrowOdbcADKStore")

MSSQL_SCHEMA: Final[str] = "dbo"
JSON_COLUMN_TYPE: Final[str] = "NVARCHAR(MAX)"
DB2_JSON_COLUMN_TYPE: Final[str] = "CLOB(1M)"
DB2_UNDEFINED_OBJECT_ERROR: Final[int] = -204


class ArrowOdbcADKConfig(ADKConfig):
    """arrow-odbc ADK extension settings."""

    native_json: NotRequired[bool]
    """Accepted for parity with SQL Server adapters; arrow-odbc uses NVARCHAR(MAX)."""


class ArrowOdbcADKStore(BaseSyncADKStore["ArrowOdbcConfig"]):
    """Synchronous ADK session/event store using arrow-odbc.

    SQL Server statements are used by default; a config whose dialect resolves
    to ``db2`` uses Db2 statements with bound UTC times and catalog-probed DDL.
    """

    connector_name: ClassVar[str] = "arrow_odbc"
    __slots__ = ("_sql",)

    def __init__(self, config: "ArrowOdbcConfig") -> None:
        super().__init__(config)
        self._sql = _adk_sql_for(config)

    def create_tables(self) -> None:
        """Create the ADK tables and indexes the catalog reports as missing."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            table_ddls = (
                (self._session_table, self._sessions_table_ddl()),
                (self._events_table, self._events_table_ddl()),
                (self._app_state_table, self._app_states_table_ddl()),
                (self._user_state_table, self._user_states_table_ddl()),
                (self._metadata_table, self._metadata_table_ddl()),
            )
            self._sql.create_missing_objects(driver, table_ddls, self._index_specs())
            driver.commit()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new ADK session."""
        sql_dialect = self._sql
        owner_column = (
            f", {sql_dialect.quote_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        )
        owner_param = ", ?" if self._owner_id_column_name else ""
        now = sql_dialect.now_sql
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            params = (session_id, app_name, user_id, owner_id, to_json(state))
        else:
            params = (session_id, app_name, user_id, to_json(state))
        params = (*params, *sql_dialect.now_params(), *sql_dialect.now_params())
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                INSERT INTO {sql_dialect.table_ref(self._session_table)} (
                    id, app_name, user_id{owner_column}, state, create_time, update_time
                )
                VALUES (?, ?, ?{owner_param}, ?, {now}, {now})
                """,
                params,
            )
            row = driver.select_one_or_none(
                sql_dialect.session_select_sql(self._session_table), (app_name, user_id, session_id)
            )
            driver.commit()
        if row is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return _session_record_from_row(row)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Return a scoped session or ``None`` if absent."""
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(
                        f"""
                        UPDATE {self._sql.table_ref(self._session_table)}
                        SET update_time = {self._sql.now_sql}
                        WHERE app_name = ? AND user_id = ? AND id = ?
                        """,
                        (*self._sql.now_params(), app_name, user_id, session_id),
                    )
                row = driver.select_one_or_none(
                    self._sql.session_select_sql(self._session_table), (app_name, user_id, session_id)
                )
                if renew_for is not None:
                    driver.commit()
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return None
            raise
        return _session_record_from_row(row) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Replace a session's durable state."""
        self._execute(
            f"""
            UPDATE {self._sql.table_ref(self._session_table)}
            SET state = ?, update_time = {self._sql.now_sql}
            WHERE app_name = ? AND user_id = ? AND id = ?
            """,
            (to_json(state), *self._sql.now_params(), app_name, user_id, session_id),
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
            self._sql.table_ref(self._session_table), app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            rows = self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session. Event rows cascade through the FK."""
        self._execute(
            f"DELETE FROM {self._sql.table_ref(self._session_table)} WHERE app_name = ? AND user_id = ? AND id = ?",
            (app_name, user_id, session_id),
            commit=True,
        )

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        self._execute(
            _insert_event_sql(self._sql.table_ref(self._events_table)),
            _event_insert_params(event_record, self._sql.format_datetime),
            commit=True,
        )

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
        sql_dialect = self._sql
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                UPDATE {sql_dialect.table_ref(self._session_table)}
                SET state = ?, update_time = {sql_dialect.now_sql}
                WHERE app_name = ? AND user_id = ? AND id = ?
                """,
                (to_json(state), *sql_dialect.now_params(), app_name, user_id, session_id),
            )
            row = driver.select_one_or_none(
                sql_dialect.session_select_sql(self._session_table), (app_name, user_id, session_id)
            )
            if row is None:
                _raise_session_not_found(session_id)
            driver.execute(
                _insert_event_sql(sql_dialect.table_ref(self._events_table)),
                _event_insert_params(event_record, sql_dialect.format_datetime),
            )
            if app_state is not None:
                driver.execute(self._upsert_app_state_sql(), (app_name, to_json(app_state), *sql_dialect.now_params()))
            if user_state is not None:
                driver.execute(
                    self._upsert_user_state_sql(), (app_name, user_id, to_json(user_state), *sql_dialect.now_params())
                )
            driver.commit()
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
        if limit is not None and limit <= 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than ``before``."""
        count_sql = f"SELECT COUNT(*) AS row_count FROM {self._sql.table_ref(self._events_table)} WHERE timestamp < ?"
        delete_sql = f"DELETE FROM {self._sql.table_ref(self._events_table)} WHERE timestamp < ?"
        params: list[Any] = [self._sql.format_datetime(before)]
        if app_name is not None:
            count_sql += " AND app_name = ?"
            delete_sql += " AND app_name = ?"
            params.append(app_name)
        try:
            count = self._select_count(count_sql, tuple(params))
            self._execute(delete_sql, tuple(params), commit=True)
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return 0
            raise
        else:
            return count

    def delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete sessions whose update_time is older than ``updated_before``."""
        count_sql = (
            f"SELECT COUNT(*) AS row_count FROM {self._sql.table_ref(self._session_table)} WHERE update_time < ?"
        )
        delete_sql = f"DELETE FROM {self._sql.table_ref(self._session_table)} WHERE update_time < ?"
        params: list[Any] = [self._sql.format_datetime(updated_before)]
        if app_name is not None:
            count_sql += " AND app_name = ?"
            delete_sql += " AND app_name = ?"
            params.append(app_name)
        try:
            count = self._select_count(count_sql, tuple(params))
            self._execute(delete_sql, tuple(params), commit=True)
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return 0
            raise
        else:
            return count

    def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user state rows whose update_time is older than ``updated_before``."""
        count_sql = (
            f"SELECT COUNT(*) AS row_count FROM {self._sql.table_ref(self._user_state_table)} WHERE update_time < ?"
        )
        delete_sql = f"DELETE FROM {self._sql.table_ref(self._user_state_table)} WHERE update_time < ?"
        params: list[Any] = [self._sql.format_datetime(updated_before)]
        if app_name is not None:
            count_sql += " AND app_name = ?"
            delete_sql += " AND app_name = ?"
            params.append(app_name)
        try:
            count = self._select_count(count_sql, tuple(params))
            self._execute(delete_sql, tuple(params), commit=True)
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return 0
            raise
        else:
            return count

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        try:
            row = self._execute_fetchone(self._sql.app_state_select_sql(self._app_state_table), (app_name,))
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        try:
            row = self._execute_fetchone(self._sql.user_state_select_sql(self._user_state_table), (app_name, user_id))
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        self._execute(self._upsert_app_state_sql(), (app_name, to_json(state), *self._sql.now_params()), commit=True)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        self._execute(
            self._upsert_user_state_sql(), (app_name, user_id, to_json(state), *self._sql.now_params()), commit=True
        )

    def get_metadata(self, key: str) -> "str | None":
        """Return an ADK metadata value."""
        try:
            row = self._execute_fetchone(self._sql.metadata_select_sql(self._metadata_table), (key,))
        except SQLSpecError as exc:
            if self._sql.is_table_missing(exc):
                return None
            raise
        value = _row_value(row, "value", 0) if row is not None else None
        return str(value) if value is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Set an ADK metadata value."""
        self._execute(self._sql.upsert_metadata_sql(self._metadata_table), (key, value), commit=True)

    def _index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for session and event indexes."""
        return self._sql.session_index_specs(self._session_table, self._events_table)

    def _sessions_table_ddl(self) -> str:
        """Return DDL for the ADK session table."""
        return self._sql.sessions_table_ddl(self._session_table, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return DDL for the ADK event table."""
        return self._sql.events_table_ddl(self._events_table, self._session_table)

    def _app_states_table_ddl(self) -> str:
        """Return DDL for the app-scoped state table."""
        return self._sql.app_states_table_ddl(self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        """Return DDL for the user-scoped state table."""
        return self._sql.user_states_table_ddl(self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        """Return DDL for the ADK metadata table."""
        return self._sql.metadata_table_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return self._sql.drop_table_sql(self._app_state_table)

    def _drop_user_states_table_sql(self) -> str:
        return self._sql.drop_table_sql(self._user_state_table)

    def _drop_metadata_table_sql(self) -> str:
        return self._sql.drop_table_sql(self._metadata_table)

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            self._sql.drop_table_sql(self._events_table),
            self._sql.drop_table_sql(self._session_table),
        ]

    def _upsert_app_state_sql(self) -> str:
        return self._sql.upsert_state_sql(self._app_state_table, ("app_name",))

    def _upsert_user_state_sql(self) -> str:
        return self._sql.upsert_state_sql(self._user_state_table, ("app_name", "user_id"))

    def _events_query(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "tuple[str, tuple[Any, ...]]":
        return self._sql.events_query(self._events_table, app_name, user_id, session_id, after_timestamp, limit)

    def _execute_fetchone(self, sql: str, params: "tuple[Any, ...]" = ()) -> "dict[str, Any] | None":
        with self._config.provide_session() as driver:
            return driver.select_one_or_none(sql, params)

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = ()) -> "list[dict[str, Any]]":
        with self._config.provide_session() as driver:
            return driver.select(sql, params)

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            if commit:
                driver.commit()
            return int(result.rows_affected)

    def _select_count(self, sql: str, params: "tuple[Any, ...]" = ()) -> int:
        with self._config.provide_session() as driver:
            value = driver.select_value(sql, params)
        return int(value or 0)


class ArrowOdbcADKMemoryStore(BaseSyncADKMemoryStore["ArrowOdbcConfig"]):
    """ADK memory store using arrow-odbc.

    SQL Server statements are used by default; a config whose dialect resolves
    to ``db2`` uses Db2 statements with bound UTC times and catalog-probed DDL.
    """

    __slots__ = ("_sql",)

    def __init__(self, config: "ArrowOdbcConfig") -> None:
        super().__init__(config)
        self._sql = _adk_sql_for(config)

    def create_tables(self) -> None:
        """Create the memory table and indexes the catalog reports as missing."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            self._sql.create_missing_objects(
                driver, ((self._memory_table, self._memory_table_ddl()),), self._memory_index_specs()
            )
            driver.commit()

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Insert memory entries, skipping duplicates by event_id."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0

        inserted_count = 0
        sql_dialect = self._sql
        with self._config.provide_session() as driver:
            for entry in entries:
                exists = driver.select_one_or_none(
                    sql_dialect.memory_duplicate_sql(self._memory_table), (entry["event_id"],)
                )
                if exists is not None:
                    continue
                owner_column = (
                    f", {sql_dialect.quote_identifier(self._owner_id_column_name)}"
                    if self._owner_id_column_name
                    else ""
                )
                owner_param = ", ?" if self._owner_id_column_name else ""
                params: tuple[Any, ...]
                if self._owner_id_column_name:
                    params = (*_memory_insert_params(entry, sql_dialect.format_datetime), owner_id)
                else:
                    params = _memory_insert_params(entry, sql_dialect.format_datetime)
                driver.execute(
                    f"""
                    INSERT INTO {sql_dialect.table_ref(self._memory_table)} (
                        id, session_id, app_name, user_id, scope, event_id, author,
                        timestamp, content_json, content_text, metadata_json, inserted_at{owner_column}
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_param})
                    """,
                    params,
                )
                inserted_count += 1
            driver.commit()
        return inserted_count

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        """Search memory entries with SQL Server LIKE matching."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        effective_limit = max(0, int(limit if limit is not None else self._max_results))
        if effective_limit == 0:
            return []
        where_scope, scope_params = _build_arrow_odbc_scope_where(app_name, user_id, scope_filter)
        rows = self._execute_fetchall(
            self._sql.memory_search_sql(self._memory_table, where_scope, effective_limit), (*scope_params, f"%{query}%")
        )
        return [_memory_record_from_row(row) for row in rows]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        table_ref = self._sql.table_ref(self._memory_table)
        count = self._select_count(f"SELECT COUNT(*) AS row_count FROM {table_ref} WHERE session_id = ?", (session_id,))
        self._execute(f"DELETE FROM {table_ref} WHERE session_id = ?", (session_id,), commit=True)
        return count

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than ``days`` days."""
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86_400)
        cutoff_dt = datetime.fromtimestamp(cutoff, tz=timezone.utc)
        clauses = ["inserted_at < ?"]
        params: list[Any] = [self._sql.format_datetime(cutoff_dt)]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        where_sql = " AND ".join(clauses)
        table_ref = self._sql.table_ref(self._memory_table)
        count = self._select_count(f"SELECT COUNT(*) AS row_count FROM {table_ref} WHERE {where_sql}", tuple(params))
        self._execute(f"DELETE FROM {table_ref} WHERE {where_sql}", tuple(params), commit=True)
        return count

    def _memory_table_ddl(self) -> str:
        return self._sql.memory_table_ddl(self._memory_table, self._owner_id_column_ddl)

    def _memory_index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for memory-table indexes."""
        return self._sql.memory_index_specs(self._memory_table)

    def _drop_memory_table_sql(self) -> "list[str]":
        return [self._sql.drop_table_sql(self._memory_table)]

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = ()) -> "list[dict[str, Any]]":
        with self._config.provide_session() as driver:
            return driver.select(sql, params)

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            if commit:
                driver.commit()
            return int(result.rows_affected)

    def _select_count(self, sql: str, params: "tuple[Any, ...]" = ()) -> int:
        with self._config.provide_session() as driver:
            value = driver.select_value(sql, params)
        return int(value or 0)


def _tsql_memory_table_ddl(table: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f",\n        {owner_id_column_ddl}" if owner_id_column_ddl else ""
    return f"""
CREATE TABLE {_table_ref(table)} (
    id NVARCHAR(128) NOT NULL,
    session_id NVARCHAR(128) NOT NULL,
    app_name NVARCHAR(128) NOT NULL,
    user_id NVARCHAR(128) NOT NULL,
    scope NVARCHAR(16) NOT NULL DEFAULT 'user',
    event_id NVARCHAR(128) NOT NULL,
    author NVARCHAR(256) NULL,
    timestamp DATETIME2(6) NOT NULL,
    content_json NVARCHAR(MAX) NOT NULL,
    content_text NVARCHAR(MAX) NOT NULL,
    metadata_json NVARCHAR(MAX) NULL,
    inserted_at DATETIME2(6) NOT NULL{owner_line},
    CONSTRAINT {_constraint_ref("pk", table, "id")} PRIMARY KEY (id),
    CONSTRAINT {_constraint_ref("uq", table, "event_id")} UNIQUE (event_id)
)
"""


def _tsql_memory_index_specs(table: str) -> "list[tuple[str, str, str]]":
    return [
        (f"idx_{table}_app_scope_user_time", table, "app_name, scope, user_id, timestamp DESC"),
        (f"idx_{table}_scope", table, "app_name, scope"),
        (f"idx_{table}_session", table, "session_id"),
    ]


def _session_select_sql(table: str) -> str:
    return f"""
    SELECT TOP 1 id, app_name, user_id, state, create_time, update_time
    FROM {_table_ref(table)}
    WHERE app_name = ? AND user_id = ? AND id = ?
    """


def _sessions_table_ddl(table: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f",\n        {owner_id_column_ddl}" if owner_id_column_ddl else ""
    return f"""
CREATE TABLE {_table_ref(table)} (
    row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {_constraint_ref("df", table, "row_id")} DEFAULT NEWSEQUENTIALID(),
    id NVARCHAR(128) NOT NULL,
    app_name NVARCHAR(128) NOT NULL,
    user_id NVARCHAR(128) NOT NULL{owner_line},
    state {JSON_COLUMN_TYPE} NOT NULL,
    create_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "create_time")} DEFAULT SYSUTCDATETIME(),
    update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
    CONSTRAINT {_constraint_ref("pk", table, "row_id")} PRIMARY KEY (row_id),
    CONSTRAINT {_constraint_ref("uq", table, "id")} UNIQUE (id)
)
"""


def _sessions_index_specs(table: str) -> "list[tuple[str, str, str]]":
    return [
        (f"idx_{table}_app_user", table, "app_name, user_id"),
        (f"idx_{table}_update_time", table, "update_time DESC"),
    ]


def _events_table_ddl(table: str, session_table: str) -> str:
    return f"""
CREATE TABLE {_table_ref(table)} (
    row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {_constraint_ref("df", table, "row_id")} DEFAULT NEWSEQUENTIALID(),
    id NVARCHAR(128) NOT NULL,
    app_name NVARCHAR(128) NOT NULL,
    user_id NVARCHAR(128) NOT NULL,
    session_id NVARCHAR(128) NOT NULL,
    invocation_id NVARCHAR(256) NOT NULL,
    timestamp DATETIME2(6) NOT NULL,
    event_data {JSON_COLUMN_TYPE} NOT NULL,
    CONSTRAINT {_constraint_ref("pk", table, "row_id")} PRIMARY KEY (row_id),
    CONSTRAINT {_constraint_ref("uq", table, "id")} UNIQUE (id),
    CONSTRAINT {_constraint_ref("fk", table, "session")} FOREIGN KEY (session_id)
        REFERENCES {_table_ref(session_table)}(id) ON DELETE CASCADE
)
"""


def _events_index_specs(table: str) -> "list[tuple[str, str, str]]":
    return [
        (f"idx_{table}_scope", table, "app_name, user_id, session_id, timestamp ASC"),
        (f"idx_{table}_session", table, "session_id, timestamp ASC"),
        (f"idx_{table}_invocation", table, "invocation_id"),
        (f"idx_{table}_timestamp", table, "timestamp ASC"),
        (f"idx_{table}_app_timestamp", table, "app_name, timestamp ASC"),
    ]


def _app_states_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {_table_ref(table)} (
    app_name NVARCHAR(128) NOT NULL,
    state {JSON_COLUMN_TYPE} NOT NULL,
    update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
    CONSTRAINT {_constraint_ref("pk", table, "app_name")} PRIMARY KEY (app_name)
)
"""


def _user_states_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {_table_ref(table)} (
    app_name NVARCHAR(128) NOT NULL,
    user_id NVARCHAR(128) NOT NULL,
    state {JSON_COLUMN_TYPE} NOT NULL,
    update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
    CONSTRAINT {_constraint_ref("pk", table, "app_user")} PRIMARY KEY (app_name, user_id)
)
"""


def _metadata_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {_table_ref(table)} (
    [key] NVARCHAR(128) NOT NULL,
    value NVARCHAR(512) NOT NULL,
    CONSTRAINT {_constraint_ref("pk", table, "key")} PRIMARY KEY ([key])
)
"""


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    return f"CREATE INDEX {_quote_identifier(index_name)} ON {_table_ref(table)} ({columns})"


def _casefold_names(rows: "list[Any]", key: str) -> "set[str]":
    """Collapse data-dictionary rows into a case-folded, schema-stripped name set."""
    return {str(row.get(key, "")).rsplit(".", 1)[-1].casefold() for row in rows}


def _bare_name(name: str) -> str:
    """Return the case-folded, schema-stripped object name for membership checks."""
    return name.rsplit(".", 1)[-1].casefold()


def _insert_event_sql(table_ref: str) -> str:
    return f"""
    INSERT INTO {table_ref} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """


def _upsert_state_sql(table: str, key_columns: "tuple[str, ...]", key_params: "tuple[str, ...]") -> str:
    source_columns = ", ".join(
        f"{param} AS {_quote_identifier(column)}" for column, param in zip(key_columns, key_params, strict=False)
    )
    source_columns = f"{source_columns}, ? AS state"
    insert_columns = ", ".join(_quote_identifier(column) for column in (*key_columns, "state", "update_time"))
    insert_values = ", ".join(f"source.{_quote_identifier(column)}" for column in (*key_columns, "state"))
    match_clause = " AND ".join(
        f"target.{_quote_identifier(column)} = source.{_quote_identifier(column)}" for column in key_columns
    )
    return f"""
    MERGE INTO {_table_ref(table)} WITH (HOLDLOCK) AS target
    USING (SELECT {source_columns}) AS source
    ON ({match_clause})
    WHEN MATCHED THEN
        UPDATE SET state = source.state, update_time = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values}, SYSUTCDATETIME());
    """


def _upsert_metadata_sql(table: str) -> str:
    return f"""
    MERGE INTO {_table_ref(table)} WITH (HOLDLOCK) AS target
    USING (SELECT ? AS [key], ? AS value) AS source
    ON (target.[key] = source.[key])
    WHEN MATCHED THEN
        UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
        INSERT ([key], value)
        VALUES (source.[key], source.value);
    """


def _tsql_events_query(
    table: str, app_name: str, user_id: str, session_id: str, after_timestamp: "datetime | None", limit: "int | None"
) -> "tuple[str, tuple[Any, ...]]":
    top_clause = f"TOP {int(limit)} " if limit is not None else ""
    params: list[Any] = [app_name, user_id, session_id]
    after_clause = ""
    if after_timestamp is not None:
        after_clause = " AND timestamp > ?"
        params.append(_format_datetime(after_timestamp))
    sql = f"""
    SELECT {top_clause}id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    FROM {_table_ref(table)}
    WHERE app_name = ? AND user_id = ? AND session_id = ?{after_clause}
    ORDER BY timestamp ASC
    """
    return sql, tuple(params)


def _event_insert_params(
    event_record: StoredEvent, format_datetime: "Callable[[datetime | None], str | None]"
) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        format_datetime(event_record["timestamp"]),
        to_json(event_record["event_data"]),
    )


def _session_record_from_row(row: Any) -> StoredSession:
    return StoredSession(
        id=str(_row_value(row, "id", 0)),
        app_name=str(_row_value(row, "app_name", 1)),
        user_id=str(_row_value(row, "user_id", 2)),
        state=_json_dict(_row_value(row, "state", 3)),
        create_time=_datetime_value(_row_value(row, "create_time", 4)),
        update_time=_datetime_value(_row_value(row, "update_time", 5)),
    )


def _event_record_from_row(row: Any) -> StoredEvent:
    return StoredEvent(
        id=str(_row_value(row, "id", 0)),
        app_name=str(_row_value(row, "app_name", 1)),
        user_id=str(_row_value(row, "user_id", 2)),
        session_id=str(_row_value(row, "session_id", 3)),
        invocation_id=str(_row_value(row, "invocation_id", 4)),
        timestamp=_datetime_value(_row_value(row, "timestamp", 5)),
        event_data=_json_dict(_row_value(row, "event_data", 6)),
    )


def _memory_insert_params(
    entry: StoredMemory, format_datetime: "Callable[[datetime | None], str | None]"
) -> "tuple[Any, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry.get("scope", "user"),
        entry["event_id"],
        entry["author"],
        format_datetime(entry["timestamp"]),
        to_json(entry["content_json"]),
        entry["content_text"],
        to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        format_datetime(entry["inserted_at"]),
    )


def _memory_record_from_row(row: Any) -> StoredMemory:
    return StoredMemory(
        id=str(_row_value(row, "id", 0)),
        session_id=str(_row_value(row, "session_id", 1)),
        app_name=str(_row_value(row, "app_name", 2)),
        user_id=str(_row_value(row, "user_id", 3)),
        scope=str(_row_value(row, "scope", 4) or "user"),
        event_id=str(_row_value(row, "event_id", 5)),
        author=cast("str | None", _row_value(row, "author", 6)),
        timestamp=_datetime_value(_row_value(row, "timestamp", 7)),
        content_json=_json_dict(_row_value(row, "content_json", 8)),
        content_text=str(_row_value(row, "content_text", 9) or ""),
        metadata_json=_optional_json_dict(_row_value(row, "metadata_json", 10)),
        inserted_at=_datetime_value(_row_value(row, "inserted_at", 11)),
        embedding=None,
    )


def _row_value(row: Any, key: str, index: int) -> Any:
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


def _json_dict(value: Any) -> "dict[str, Any]":
    if value is None:
        return {}
    if isinstance(value, dict):
        return cast("dict[str, Any]", value)
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return cast("dict[str, Any]", from_json(value))
    return cast("dict[str, Any]", from_json(str(value)))


def _optional_json_dict(value: Any) -> "dict[str, Any] | None":
    if value is None:
        return None
    return _json_dict(value)


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _format_datetime(value: "datetime | None") -> "str | None":
    if value is None:
        return None
    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return normalized.replace(tzinfo=None).isoformat(timespec="microseconds")


def _is_table_missing(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "invalid object name" in text or "42s02" in text or "(208)" in text


def _quote_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _table_ref(table: str) -> str:
    return f"{_quote_identifier(MSSQL_SCHEMA)}.{_quote_identifier(table)}"


def _constraint_ref(prefix: str, table: str, suffix: str) -> str:
    return _quote_identifier(f"{prefix}_{table}_{suffix}")


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _build_arrow_odbc_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = 'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = 'app'", (app_name,)


def _session_list_query(
    session_table_ref: str,
    app_name: str,
    user_id: "str | None",
    column: str,
    direction: str,
    limit: "int | None",
    offset: int,
) -> "tuple[str, tuple[Any, ...]]":
    """Return the bounded session-list query and its bound values."""
    params: list[Any] = [app_name]
    where_clause = "app_name = ?"
    if user_id is not None:
        params.append(user_id)
        where_clause = f"{where_clause} AND user_id = ?"

    page_clause = ""
    if limit is not None:
        params.extend((offset, limit))
        page_clause = "\n            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"

    sql = f"""
    SELECT id, app_name, user_id, state, create_time, update_time
    FROM {session_table_ref}
    WHERE {where_clause}
    ORDER BY {column} {direction}, id {direction}{page_clause}
    """
    return sql, tuple(params)


class _TsqlAdkSql:
    """SQL Server statements for the ADK stores."""

    __slots__ = ()

    now_sql: ClassVar[str] = "SYSUTCDATETIME()"

    def now_params(self) -> "tuple[Any, ...]":
        """Return bind values for ``now_sql``; SQL Server reads its own UTC clock."""
        return ()

    def table_ref(self, table: str) -> str:
        return _table_ref(table)

    def quote_identifier(self, identifier: str) -> str:
        return _quote_identifier(identifier)

    def format_datetime(self, value: "datetime | None") -> "str | None":
        return _format_datetime(value)

    def is_table_missing(self, exc: BaseException) -> bool:
        return _is_table_missing(exc)

    def create_missing_objects(
        self, driver: Any, table_ddls: "Sequence[tuple[str, str]]", index_specs: "Sequence[tuple[str, str, str]]"
    ) -> None:
        """Create the tables and indexes the data dictionary does not list in ``dbo``."""
        dd = driver.data_dictionary
        existing_tables = _casefold_names(dd.get_tables(driver, schema=MSSQL_SCHEMA), "table_name")
        existing_indexes = _casefold_names(dd.get_indexes(driver, schema=MSSQL_SCHEMA), "index_name")
        for table, ddl in table_ddls:
            if _bare_name(table) not in existing_tables:
                driver.execute(ddl)
        for index_name, index_table, columns in index_specs:
            if _bare_name(index_name) not in existing_indexes:
                driver.execute(_create_index_sql(index_table, index_name, columns))

    def session_select_sql(self, table: str) -> str:
        return _session_select_sql(table)

    def app_state_select_sql(self, table: str) -> str:
        return f"SELECT TOP 1 state FROM {_table_ref(table)} WHERE app_name = ?"

    def user_state_select_sql(self, table: str) -> str:
        return f"""
                SELECT TOP 1 state
                FROM {_table_ref(table)}
                WHERE app_name = ? AND user_id = ?
                """

    def metadata_select_sql(self, table: str) -> str:
        return f"SELECT TOP 1 value FROM {_table_ref(table)} WHERE [key] = ?"

    def upsert_state_sql(self, table: str, key_columns: "tuple[str, ...]") -> str:
        return _upsert_state_sql(table, key_columns, tuple("?" for _ in key_columns))

    def upsert_metadata_sql(self, table: str) -> str:
        return _upsert_metadata_sql(table)

    def events_query(
        self,
        table: str,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None",
        limit: "int | None",
    ) -> "tuple[str, tuple[Any, ...]]":
        return _tsql_events_query(table, app_name, user_id, session_id, after_timestamp, limit)

    def session_index_specs(self, session_table: str, events_table: str) -> "list[tuple[str, str, str]]":
        return [*_sessions_index_specs(session_table), *_events_index_specs(events_table)]

    def sessions_table_ddl(self, table: str, owner_id_column_ddl: "str | None") -> str:
        return _sessions_table_ddl(table, owner_id_column_ddl)

    def events_table_ddl(self, table: str, session_table: str) -> str:
        return _events_table_ddl(table, session_table)

    def app_states_table_ddl(self, table: str) -> str:
        return _app_states_table_ddl(table)

    def user_states_table_ddl(self, table: str) -> str:
        return _user_states_table_ddl(table)

    def metadata_table_ddl(self, table: str) -> str:
        return _metadata_table_ddl(table)

    def drop_table_sql(self, table: str) -> str:
        return f"DROP TABLE IF EXISTS {_table_ref(table)}"

    def memory_table_ddl(self, table: str, owner_id_column_ddl: "str | None") -> str:
        return _tsql_memory_table_ddl(table, owner_id_column_ddl)

    def memory_index_specs(self, table: str) -> "list[tuple[str, str, str]]":
        return _tsql_memory_index_specs(table)

    def memory_duplicate_sql(self, table: str) -> str:
        return f"SELECT TOP 1 id FROM {_table_ref(table)} WHERE event_id = ?"

    def memory_search_sql(self, table: str, where_scope: str, limit: int) -> str:
        return f"""
            SELECT id, session_id, app_name, user_id, scope, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {_table_ref(table)}
            WHERE {where_scope}
              AND content_text LIKE ?
            ORDER BY timestamp DESC
            OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY
            """


class _Db2AdkSql:
    """Db2 statements for the ADK stores.

    Identifiers are written unquoted, so Db2 folds them to uppercase; the
    metadata key column is the delimited ``"KEY"``. Times are bound as naive-UTC
    text, and objects are created only when the SYSCAT catalog does not list them.
    """

    __slots__ = ()

    now_sql: ClassVar[str] = "?"

    def now_params(self) -> "tuple[Any, ...]":
        """Return the current naive-UTC time bound to ``now_sql``."""
        return (db2_timestamp_text(datetime.now(timezone.utc)),)

    def table_ref(self, table: str) -> str:
        return table

    def quote_identifier(self, identifier: str) -> str:
        return identifier

    def format_datetime(self, value: "datetime | None") -> "str | None":
        return db2_timestamp_text(value)

    def is_table_missing(self, exc: BaseException) -> bool:
        """Return whether the error is Db2's undefined-name error (native code -204)."""
        return extract_native_error_number(exc) == DB2_UNDEFINED_OBJECT_ERROR

    def create_missing_objects(
        self, driver: Any, table_ddls: "Sequence[tuple[str, str]]", index_specs: "Sequence[tuple[str, str, str]]"
    ) -> None:
        """Create each table and index the Db2 catalog does not list, probing by folded name."""
        for table, ddl in table_ddls:
            if driver.select_one_or_none(DB2_TABLE_EXISTS_SQL, split_db2_name(table)) is None:
                driver.execute(ddl)
        for index_name, index_table, columns in index_specs:
            if driver.select_one_or_none(DB2_INDEX_EXISTS_SQL, split_db2_name(index_name)) is None:
                driver.execute(f"CREATE INDEX {index_name} ON {index_table} ({columns})")

    def session_select_sql(self, table: str) -> str:
        return (
            f"SELECT id, app_name, user_id, state, create_time, update_time FROM {table} "
            "WHERE app_name = ? AND user_id = ? AND id = ? FETCH FIRST 1 ROWS ONLY"
        )

    def app_state_select_sql(self, table: str) -> str:
        return f"SELECT state FROM {table} WHERE app_name = ? FETCH FIRST 1 ROWS ONLY"

    def user_state_select_sql(self, table: str) -> str:
        return f"SELECT state FROM {table} WHERE app_name = ? AND user_id = ? FETCH FIRST 1 ROWS ONLY"

    def metadata_select_sql(self, table: str) -> str:
        return f'SELECT value FROM {table} WHERE "KEY" = ? FETCH FIRST 1 ROWS ONLY'

    def upsert_state_sql(self, table: str, key_columns: "tuple[str, ...]") -> str:
        """Return a MERGE whose source row casts each marker; bind keys, state, then the UTC time."""
        source_columns = ", ".join(f"CAST(? AS VARCHAR(128)) AS {column}" for column in key_columns)
        match_clause = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
        insert_columns = ", ".join((*key_columns, "state", "update_time"))
        insert_values = ", ".join(f"source.{column}" for column in (*key_columns, "state", "now_utc"))
        return (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_columns}, CAST(? AS {DB2_JSON_COLUMN_TYPE}) AS state, "
            "CAST(? AS TIMESTAMP) AS now_utc FROM SYSIBM.SYSDUMMY1) AS source "
            f"ON ({match_clause}) "
            "WHEN MATCHED THEN UPDATE SET state = source.state, update_time = source.now_utc "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )

    def upsert_metadata_sql(self, table: str) -> str:
        return (
            f'MERGE INTO {table} AS target USING (SELECT CAST(? AS VARCHAR(128)) AS "KEY", '
            "CAST(? AS VARCHAR(512)) AS value FROM SYSIBM.SYSDUMMY1) AS source "
            'ON (target."KEY" = source."KEY") WHEN MATCHED THEN UPDATE SET value = source.value '
            'WHEN NOT MATCHED THEN INSERT ("KEY", value) VALUES (source."KEY", source.value)'
        )

    def events_query(
        self,
        table: str,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None",
        limit: "int | None",
    ) -> "tuple[str, tuple[Any, ...]]":
        params: list[Any] = [app_name, user_id, session_id]
        after_clause = ""
        if after_timestamp is not None:
            after_clause = " AND timestamp > ?"
            params.append(db2_timestamp_text(after_timestamp))
        limit_clause = f" FETCH FIRST {int(limit)} ROWS ONLY" if limit is not None else ""
        sql = (
            f"SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data FROM {table} "
            f"WHERE app_name = ? AND user_id = ? AND session_id = ?{after_clause} ORDER BY timestamp ASC{limit_clause}"
        )
        return sql, tuple(params)

    def session_index_specs(self, session_table: str, events_table: str) -> "list[tuple[str, str, str]]":
        return [
            (f"idx_{session_table}_app_user", session_table, "app_name, user_id"),
            (f"idx_{session_table}_update_time", session_table, "update_time"),
            (f"idx_{events_table}_scope", events_table, "app_name, user_id, session_id, timestamp"),
            (f"idx_{events_table}_session", events_table, "session_id, timestamp"),
            (f"idx_{events_table}_invocation", events_table, "invocation_id"),
            (f"idx_{events_table}_timestamp", events_table, "timestamp"),
            (f"idx_{events_table}_app_timestamp", events_table, "app_name, timestamp"),
        ]

    def sessions_table_ddl(self, table: str, owner_id_column_ddl: "str | None") -> str:
        owner_line = f", {owner_id_column_ddl}" if owner_id_column_ddl else ""
        return (
            f"CREATE TABLE {table} (id VARCHAR(128) NOT NULL, app_name VARCHAR(128) NOT NULL, "
            f"user_id VARCHAR(128) NOT NULL{owner_line}, state {DB2_JSON_COLUMN_TYPE} NOT NULL, "
            "create_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
            "update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
            f"CONSTRAINT pk_{table}_id PRIMARY KEY (id))"
        )

    def events_table_ddl(self, table: str, session_table: str) -> str:
        return (
            f"CREATE TABLE {table} (id VARCHAR(128) NOT NULL, app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, session_id VARCHAR(128) NOT NULL, "
            "invocation_id VARCHAR(256) NOT NULL, timestamp TIMESTAMP NOT NULL, "
            f"event_data {DB2_JSON_COLUMN_TYPE} NOT NULL, CONSTRAINT pk_{table}_id PRIMARY KEY (id), "
            f"CONSTRAINT fk_{table}_session FOREIGN KEY (session_id) REFERENCES {session_table}(id) ON DELETE CASCADE)"
        )

    def app_states_table_ddl(self, table: str) -> str:
        return (
            f"CREATE TABLE {table} (app_name VARCHAR(128) NOT NULL, state {DB2_JSON_COLUMN_TYPE} NOT NULL, "
            "update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
            f"CONSTRAINT pk_{table}_app_name PRIMARY KEY (app_name))"
        )

    def user_states_table_ddl(self, table: str) -> str:
        return (
            f"CREATE TABLE {table} (app_name VARCHAR(128) NOT NULL, user_id VARCHAR(128) NOT NULL, "
            f"state {DB2_JSON_COLUMN_TYPE} NOT NULL, update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
            f"CONSTRAINT pk_{table}_app_user PRIMARY KEY (app_name, user_id))"
        )

    def metadata_table_ddl(self, table: str) -> str:
        return (
            f'CREATE TABLE {table} ("KEY" VARCHAR(128) NOT NULL, value VARCHAR(512) NOT NULL, '
            f'CONSTRAINT pk_{table}_key PRIMARY KEY ("KEY"))'
        )

    def drop_table_sql(self, table: str) -> str:
        return f"DROP TABLE {table}"

    def memory_table_ddl(self, table: str, owner_id_column_ddl: "str | None") -> str:
        owner_line = f", {owner_id_column_ddl}" if owner_id_column_ddl else ""
        return (
            f"CREATE TABLE {table} (id VARCHAR(128) NOT NULL, session_id VARCHAR(128) NOT NULL, "
            "app_name VARCHAR(128) NOT NULL, user_id VARCHAR(128) NOT NULL, "
            "scope VARCHAR(16) NOT NULL DEFAULT 'user', event_id VARCHAR(128) NOT NULL, author VARCHAR(256), "
            f"timestamp TIMESTAMP NOT NULL, content_json {DB2_JSON_COLUMN_TYPE} NOT NULL, "
            f"content_text {DB2_JSON_COLUMN_TYPE} NOT NULL, metadata_json {DB2_JSON_COLUMN_TYPE}, "
            f"inserted_at TIMESTAMP NOT NULL{owner_line}, CONSTRAINT pk_{table}_id PRIMARY KEY (id), "
            f"CONSTRAINT uq_{table}_event UNIQUE (event_id))"
        )

    def memory_index_specs(self, table: str) -> "list[tuple[str, str, str]]":
        return [
            (f"idx_{table}_scope", table, "app_name, user_id, scope, timestamp"),
            (f"idx_{table}_session", table, "session_id, timestamp"),
            (f"idx_{table}_inserted", table, "inserted_at"),
        ]

    def memory_duplicate_sql(self, table: str) -> str:
        return f"SELECT id FROM {table} WHERE event_id = ? FETCH FIRST 1 ROWS ONLY"

    def memory_search_sql(self, table: str, where_scope: str, limit: int) -> str:
        return (
            "SELECT id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, "
            f"content_text, metadata_json, inserted_at FROM {table} WHERE {where_scope} AND content_text LIKE ? "
            f"ORDER BY timestamp DESC FETCH FIRST {int(limit)} ROWS ONLY"
        )


def _adk_sql_for(config: "ArrowOdbcConfig") -> "_TsqlAdkSql | _Db2AdkSql":
    """Return the statement provider for the config's resolved dialect."""
    if str(config.statement_config.dialect).lower() == "db2":
        return _Db2AdkSql()
    return _TsqlAdkSql()
