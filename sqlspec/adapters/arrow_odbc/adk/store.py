"""arrow-odbc ADK stores for Google Agent Development Kit session storage."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from typing_extensions import NotRequired

from sqlspec.config import ADKConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory import BaseSyncADKMemoryStore, MemoryRecord
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import timedelta

    from sqlspec.adapters.arrow_odbc.config import ArrowOdbcConfig
else:
    ArrowOdbcConfig = Any


__all__ = ("ArrowOdbcADKConfig", "ArrowOdbcADKMemoryStore", "ArrowOdbcADKStore")

MSSQL_SCHEMA: Final[str] = "dbo"
JSON_COLUMN_TYPE: Final[str] = "NVARCHAR(MAX)"


class ArrowOdbcADKConfig(ADKConfig):
    """arrow-odbc ADK extension settings."""

    native_json: NotRequired[bool]
    """Accepted for parity with SQL Server adapters; arrow-odbc uses NVARCHAR(MAX)."""


class ArrowOdbcADKStore(BaseSyncADKStore["ArrowOdbcConfig"]):
    """Synchronous SQL Server ADK session/event store using arrow-odbc."""

    connector_name: ClassVar[str] = "arrow_odbc"
    __slots__ = ()

    def create_tables(self) -> None:
        """Create the ADK tables and indexes the data dictionary reports as missing."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            dd = driver.data_dictionary
            existing_tables = _casefold_names(dd.get_tables(driver, schema=MSSQL_SCHEMA), "table_name")
            existing_indexes = _casefold_names(dd.get_indexes(driver, schema=MSSQL_SCHEMA), "index_name")
            table_ddls = (
                (self._session_table, self._sessions_table_ddl()),
                (self._events_table, self._events_table_ddl()),
                (self._app_state_table, self._app_states_table_ddl()),
                (self._user_state_table, self._user_states_table_ddl()),
                (self._metadata_table, self._metadata_table_ddl()),
            )
            for table, ddl in table_ddls:
                if _bare_name(table) not in existing_tables:
                    driver.execute(ddl)
            for index_name, index_table, columns in self._index_specs():
                if _bare_name(index_name) not in existing_indexes:
                    driver.execute(_create_index_sql(index_table, index_name, columns))
            driver.commit()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new ADK session."""
        owner_column = f", {_quote_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_param = ", ?" if self._owner_id_column_name else ""
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            params = (session_id, app_name, user_id, owner_id, to_json(state))
        else:
            params = (session_id, app_name, user_id, to_json(state))
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                INSERT INTO {_table_ref(self._session_table)} (
                    id, app_name, user_id{owner_column}, state, create_time, update_time
                )
                VALUES (?, ?, ?{owner_param}, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                """,
                params,
            )
            row = driver.select_one_or_none(_session_select_sql(self._session_table), (app_name, user_id, session_id))
            driver.commit()
        if row is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return _session_record_from_row(row)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Return a scoped session or ``None`` if absent."""
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(
                        f"""
                        UPDATE {_table_ref(self._session_table)}
                        SET update_time = SYSUTCDATETIME()
                        WHERE app_name = ? AND user_id = ? AND id = ?
                        """,
                        (app_name, user_id, session_id),
                    )
                row = driver.select_one_or_none(
                    _session_select_sql(self._session_table), (app_name, user_id, session_id)
                )
                if renew_for is not None:
                    driver.commit()
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise
        return _session_record_from_row(row) if row is not None else None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Replace a session's durable state."""
        self._execute(
            f"""
            UPDATE {_table_ref(self._session_table)}
            SET state = ?, update_time = SYSUTCDATETIME()
            WHERE app_name = ? AND user_id = ? AND id = ?
            """,
            (to_json(state), app_name, user_id, session_id),
            commit=True,
        )

    def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        """List ADK sessions for an application, optionally scoped to a user."""
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {_table_ref(self._session_table)}
            WHERE app_name = ?
            ORDER BY update_time DESC
            """
            params: tuple[Any, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {_table_ref(self._session_table)}
            WHERE app_name = ? AND user_id = ?
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)
        try:
            rows = self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session. Event rows cascade through the FK."""
        self._execute(
            f"DELETE FROM {_table_ref(self._session_table)} WHERE app_name = ? AND user_id = ? AND id = ?",
            (app_name, user_id, session_id),
            commit=True,
        )

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        self._execute(_insert_event_sql(self._events_table), _event_insert_params(event_record), commit=True)

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
        """Atomically append an event and update durable session/scoped state."""
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                UPDATE {_table_ref(self._session_table)}
                SET state = ?, update_time = SYSUTCDATETIME()
                WHERE app_name = ? AND user_id = ? AND id = ?
                """,
                (to_json(state), app_name, user_id, session_id),
            )
            row = driver.select_one_or_none(_session_select_sql(self._session_table), (app_name, user_id, session_id))
            if row is None:
                _raise_session_not_found(session_id)
            driver.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            if app_state is not None:
                driver.execute(self._upsert_app_state_sql(), (app_name, to_json(app_state)))
            if user_state is not None:
                driver.execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(user_state)))
            driver.commit()
        return _session_record_from_row(row)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Return events for a scoped session ordered by event timestamp."""
        if limit is not None and limit <= 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than ``before``."""
        try:
            count = self._select_count(
                f"SELECT COUNT(*) AS row_count FROM {_table_ref(self._events_table)} WHERE timestamp < ?",
                (_format_datetime(before),),
            )
            self._execute(
                f"DELETE FROM {_table_ref(self._events_table)} WHERE timestamp < ?",
                (_format_datetime(before),),
                commit=True,
            )
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return 0
            raise
        else:
            return count

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions whose update_time is older than ``updated_before``."""
        try:
            count = self._select_count(
                f"SELECT COUNT(*) AS row_count FROM {_table_ref(self._session_table)} WHERE update_time < ?",
                (_format_datetime(updated_before),),
            )
            self._execute(
                f"DELETE FROM {_table_ref(self._session_table)} WHERE update_time < ?",
                (_format_datetime(updated_before),),
                commit=True,
            )
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return 0
            raise
        else:
            return count

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP 1 state FROM {_table_ref(self._app_state_table)} WHERE app_name = ?", (app_name,)
            )
        except SQLSpecError as exc:
            if _is_table_missing(exc):
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
                WHERE app_name = ? AND user_id = ?
                """,
                (app_name, user_id),
            )
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise
        return _json_dict(_row_value(row, "state", 0)) if row is not None else None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        self._execute(self._upsert_app_state_sql(), (app_name, to_json(state)), commit=True)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        self._execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(state)), commit=True)

    def get_metadata(self, key: str) -> "str | None":
        """Return an ADK metadata value."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP 1 value FROM {_table_ref(self._metadata_table)} WHERE [key] = ?", (key,)
            )
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise
        value = _row_value(row, "value", 0) if row is not None else None
        return str(value) if value is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Set an ADK metadata value."""
        self._execute(_upsert_metadata_sql(self._metadata_table), (key, value), commit=True)

    def _index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for session and event indexes."""
        return [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]

    def _sessions_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK session table."""
        return _sessions_table_ddl(self._session_table, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK event table."""
        return _events_table_ddl(self._events_table, self._session_table)

    def _app_states_table_ddl(self) -> str:
        """Return T-SQL DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        """Return T-SQL DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_table_ref(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_table_ref(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_table_ref(self._metadata_table)}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {_table_ref(self._events_table)}",
            f"DROP TABLE IF EXISTS {_table_ref(self._session_table)}",
        ]

    def _upsert_app_state_sql(self) -> str:
        return _upsert_state_sql(self._app_state_table, ("app_name",), ("?",))

    def _upsert_user_state_sql(self) -> str:
        return _upsert_state_sql(self._user_state_table, ("app_name", "user_id"), ("?", "?"))

    def _events_query(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "tuple[str, tuple[Any, ...]]":
        return _events_query(self._events_table, app_name, user_id, session_id, after_timestamp, limit)

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
    """SQL Server ADK memory store using arrow-odbc."""

    __slots__ = ()

    def create_tables(self) -> None:
        """Create the memory table and indexes the data dictionary reports as missing."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            dd = driver.data_dictionary
            existing_tables = _casefold_names(dd.get_tables(driver, schema=MSSQL_SCHEMA), "table_name")
            existing_indexes = _casefold_names(dd.get_indexes(driver, schema=MSSQL_SCHEMA), "index_name")
            if _bare_name(self._memory_table) not in existing_tables:
                driver.execute(self._memory_table_ddl())
            for index_name, index_table, columns in self._memory_index_specs():
                if _bare_name(index_name) not in existing_indexes:
                    driver.execute(_create_index_sql(index_table, index_name, columns))
            driver.commit()

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Insert memory entries, skipping duplicates by event_id."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if not entries:
            return 0

        inserted_count = 0
        with self._config.provide_session() as driver:
            for entry in entries:
                exists = driver.select_one_or_none(
                    f"SELECT TOP 1 id FROM {_table_ref(self._memory_table)} WHERE event_id = ?", (entry["event_id"],)
                )
                if exists is not None:
                    continue
                owner_column = (
                    f", {_quote_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
                )
                owner_param = ", ?" if self._owner_id_column_name else ""
                params: tuple[Any, ...]
                if self._owner_id_column_name:
                    params = (*_memory_insert_params(entry), owner_id)
                else:
                    params = _memory_insert_params(entry)
                driver.execute(
                    f"""
                    INSERT INTO {_table_ref(self._memory_table)} (
                        id, session_id, app_name, user_id, event_id, author,
                        timestamp, content_json, content_text, metadata_json, inserted_at{owner_column}
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_param})
                    """,
                    params,
                )
                inserted_count += 1
            driver.commit()
        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries with SQL Server LIKE matching."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        effective_limit = max(0, int(limit if limit is not None else self._max_results))
        if effective_limit == 0:
            return []
        rows = self._execute_fetchall(
            f"""
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {_table_ref(self._memory_table)}
            WHERE app_name = ?
              AND user_id = ?
              AND content_text LIKE ?
            ORDER BY timestamp DESC
            OFFSET 0 ROWS FETCH NEXT {effective_limit} ROWS ONLY
            """,
            (app_name, user_id, f"%{query}%"),
        )
        return [_memory_record_from_row(row) for row in rows]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        count = self._select_count(
            f"SELECT COUNT(*) AS row_count FROM {_table_ref(self._memory_table)} WHERE session_id = ?", (session_id,)
        )
        self._execute(f"DELETE FROM {_table_ref(self._memory_table)} WHERE session_id = ?", (session_id,), commit=True)
        return count

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than ``days`` days."""
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 86_400)
        cutoff_dt = datetime.fromtimestamp(cutoff, tz=timezone.utc)
        count = self._select_count(
            f"SELECT COUNT(*) AS row_count FROM {_table_ref(self._memory_table)} WHERE inserted_at < ?",
            (_format_datetime(cutoff_dt),),
        )
        self._execute(
            f"DELETE FROM {_table_ref(self._memory_table)} WHERE inserted_at < ?",
            (_format_datetime(cutoff_dt),),
            commit=True,
        )
        return count

    def _memory_table_ddl(self) -> str:
        owner_line = f",\n        {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
CREATE TABLE {_table_ref(self._memory_table)} (
    id NVARCHAR(128) NOT NULL,
    session_id NVARCHAR(128) NOT NULL,
    app_name NVARCHAR(128) NOT NULL,
    user_id NVARCHAR(128) NOT NULL,
    event_id NVARCHAR(128) NOT NULL,
    author NVARCHAR(256) NULL,
    timestamp DATETIME2(6) NOT NULL,
    content_json NVARCHAR(MAX) NOT NULL,
    content_text NVARCHAR(MAX) NOT NULL,
    metadata_json NVARCHAR(MAX) NULL,
    inserted_at DATETIME2(6) NOT NULL{owner_line},
    CONSTRAINT {_constraint_ref("pk", self._memory_table, "id")} PRIMARY KEY (id),
    CONSTRAINT {_constraint_ref("uq", self._memory_table, "event_id")} UNIQUE (event_id)
)
"""

    def _memory_index_specs(self) -> "list[tuple[str, str, str]]":
        """Return ``(index_name, table, columns)`` specs for memory-table indexes."""
        return [
            (f"idx_{self._memory_table}_app_user_time", self._memory_table, "app_name, user_id, timestamp DESC"),
            (f"idx_{self._memory_table}_session", self._memory_table, "session_id"),
        ]

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {_table_ref(self._memory_table)}"]

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


def _insert_event_sql(table: str) -> str:
    return f"""
    INSERT INTO {_table_ref(table)} (
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


def _events_query(
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


def _event_insert_params(event_record: EventRecord) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        _format_datetime(event_record["timestamp"]),
        to_json(event_record["event_data"]),
    )


def _session_record_from_row(row: Any) -> SessionRecord:
    return SessionRecord(
        id=str(_row_value(row, "id", 0)),
        app_name=str(_row_value(row, "app_name", 1)),
        user_id=str(_row_value(row, "user_id", 2)),
        state=_json_dict(_row_value(row, "state", 3)),
        create_time=_datetime_value(_row_value(row, "create_time", 4)),
        update_time=_datetime_value(_row_value(row, "update_time", 5)),
    )


def _event_record_from_row(row: Any) -> EventRecord:
    return EventRecord(
        id=str(_row_value(row, "id", 0)),
        app_name=str(_row_value(row, "app_name", 1)),
        user_id=str(_row_value(row, "user_id", 2)),
        session_id=str(_row_value(row, "session_id", 3)),
        invocation_id=str(_row_value(row, "invocation_id", 4)),
        timestamp=_datetime_value(_row_value(row, "timestamp", 5)),
        event_data=_json_dict(_row_value(row, "event_data", 6)),
    )


def _memory_insert_params(entry: MemoryRecord) -> "tuple[Any, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry["event_id"],
        entry["author"],
        _format_datetime(entry["timestamp"]),
        to_json(entry["content_json"]),
        entry["content_text"],
        to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        _format_datetime(entry["inserted_at"]),
    )


def _memory_record_from_row(row: Any) -> MemoryRecord:
    return MemoryRecord(
        id=str(_row_value(row, "id", 0)),
        session_id=str(_row_value(row, "session_id", 1)),
        app_name=str(_row_value(row, "app_name", 2)),
        user_id=str(_row_value(row, "user_id", 3)),
        event_id=str(_row_value(row, "event_id", 4)),
        author=cast("str | None", _row_value(row, "author", 5)),
        timestamp=_datetime_value(_row_value(row, "timestamp", 6)),
        content_json=_json_dict(_row_value(row, "content_json", 7)),
        content_text=str(_row_value(row, "content_text", 8) or ""),
        metadata_json=_optional_json_dict(_row_value(row, "metadata_json", 9)),
        inserted_at=_datetime_value(_row_value(row, "inserted_at", 10)),
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
