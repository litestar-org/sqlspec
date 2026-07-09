"""mssql-python ADK stores for Google Agent Development Kit session storage."""

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python._typing import MSSQL_PYTHON_MODULE, MssqlPythonCursor
from sqlspec.adapters.mssql_python.data_dictionary import MssqlVersionInfo
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import timedelta

    from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
    from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

__all__ = ("MssqlPythonADKConfig", "MssqlPythonADKStore")

MSSQL_TABLE_NOT_FOUND_ERROR: Final[int] = 208
MSSQL_DUPLICATE_OBJECT_ERROR: Final[int] = 2714
MSSQL_DUPLICATE_INDEX_ERROR: Final[int] = 1913
MSSQL_SCHEMA: Final[str] = "dbo"
MSSQL_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(([-]?\d+)\)")
MSSQL_ERROR: Final[type[BaseException]] = cast("type[BaseException]", getattr(MSSQL_PYTHON_MODULE, "Error", Exception))
JSON_FALLBACK_COLUMN_TYPE: Final[str] = "NVARCHAR(MAX)"
JSON_NATIVE_COLUMN_TYPE: Final[str] = "JSON"


class MssqlPythonADKConfig(ADKConfig):
    """mssql-python ADK extension settings."""

    native_json: NotRequired[bool]
    """Force native SQL Server JSON columns when True, or NVARCHAR(MAX) when False."""


class MssqlPythonADKStore(BaseSyncADKStore["MssqlPythonConfig"]):
    """Synchronous mssql-python ADK session/event store."""

    connector_name: ClassVar[str] = "mssql_python"
    __slots__ = ("_json_column_type", "_native_json")

    def __init__(self, config: "MssqlPythonConfig") -> None:
        super().__init__(config)
        adk_config = _adk_config(config)
        native_json = adk_config.get("native_json")
        self._native_json: bool | None = native_json if isinstance(native_json, bool) else None
        self._json_column_type: str | None = None

    def create_tables(self) -> None:
        """Create all ADK session tables if they do not exist."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            driver.execute_script(self._metadata_seed_sql())
            driver.commit()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new ADK session."""
        owner_column = f", {_quote_identifier(self._owner_id_column_name)}" if self._owner_id_column_name else ""
        owner_param = ", ?" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {_table_ref(self._session_table)} (
            id, app_name, user_id{owner_column}, state, create_time, update_time
        )
        OUTPUT inserted.id, inserted.app_name, inserted.user_id, inserted.state, inserted.create_time, inserted.update_time
        VALUES (?, ?, ?{owner_param}, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
        """
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            params = (session_id, app_name, user_id, owner_id, to_json(state))
        else:
            params = (session_id, app_name, user_id, to_json(state))
        row = self._execute_fetchone(sql, params, commit=True)
        if row is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return _session_record_from_row(row)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Return a scoped session or ``None`` if absent."""
        try:
            if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                self._execute(
                    f"""
                    UPDATE {_table_ref(self._session_table)}
                    SET update_time = SYSUTCDATETIME()
                    WHERE app_name = ? AND user_id = ? AND id = ?
                    """,
                    (app_name, user_id, session_id),
                    commit=True,
                )
            row = self._execute_fetchone(
                f"""
                SELECT TOP (1) id, app_name, user_id, state, create_time, update_time
                FROM {_table_ref(self._session_table)}
                WHERE app_name = ? AND user_id = ? AND id = ?
                """,
                (app_name, user_id, session_id),
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
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
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
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
        update_sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = ?, update_time = SYSUTCDATETIME()
        OUTPUT inserted.id, inserted.app_name, inserted.user_id, inserted.state, inserted.create_time, inserted.update_time
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        with self._config.provide_connection() as conn, MssqlPythonCursor(conn) as cursor:
            try:
                cursor.execute(update_sql, (to_json(state), app_name, user_id, session_id))
                row = cursor.fetchone()
                if row is None:
                    _raise_session_not_found(session_id)
                cursor.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
                if app_state is not None:
                    cursor.execute(self._upsert_app_state_sql(), (app_name, to_json(app_state)))
                if user_state is not None:
                    cursor.execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(user_state)))
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
    ) -> "list[EventRecord]":
        """Return events for a scoped session ordered by event timestamp."""
        if limit == 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = self._execute_fetchall(sql, params)
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than ``before``."""
        try:
            return self._execute(
                f"DELETE FROM {_table_ref(self._events_table)} WHERE timestamp < ?", (before,), commit=True
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return 0
            raise

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions whose update_time is older than ``updated_before``."""
        try:
            return self._execute(
                f"DELETE FROM {_table_ref(self._session_table)} WHERE update_time < ?", (updated_before,), commit=True
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        try:
            row = self._execute_fetchone(
                f"SELECT TOP (1) state FROM {_table_ref(self._app_state_table)} WHERE app_name = ?", (app_name,)
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return None
            raise
        return _json_dict(row[0]) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        try:
            row = self._execute_fetchone(
                f"""
                SELECT TOP (1) state
                FROM {_table_ref(self._user_state_table)}
                WHERE app_name = ? AND user_id = ?
                """,
                (app_name, user_id),
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return None
            raise
        return _json_dict(row[0]) if row is not None else None

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
                f"SELECT TOP (1) value FROM {_table_ref(self._metadata_table)} WHERE [key] = ?", (key,)
            )
        except MSSQL_ERROR as exc:
            if _is_mssql_table_missing(exc):
                return None
            raise
        return str(row[0]) if row is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        """Set an ADK metadata value."""
        self._execute(_upsert_metadata_sql(self._metadata_table), (key, value), commit=True)

    def _sessions_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK session table."""
        return _sessions_table_ddl(self._session_table, self._json_column_type_sync(), self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK event table."""
        return _events_table_ddl(self._events_table, self._session_table, self._json_column_type_sync())

    def _app_states_table_ddl(self) -> str:
        """Return T-SQL DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table, self._json_column_type_sync())

    def _user_states_table_ddl(self) -> str:
        """Return T-SQL DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table, self._json_column_type_sync())

    def _metadata_table_ddl(self) -> str:
        """Return T-SQL DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    def _metadata_seed_sql(self) -> str:
        """Return T-SQL to seed schema-version metadata."""
        return _metadata_seed_sql(self._metadata_table)

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

    def _json_column_type_sync(self) -> str:
        if self._json_column_type is not None:
            return self._json_column_type
        configured = _configured_json_column_type(self._native_json)
        if configured is not None:
            self._json_column_type = configured
            return configured
        with self._config.provide_session() as driver:
            self._json_column_type = _json_column_type_from_sync_driver(driver)
        return self._json_column_type

    def _execute_fetchone(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> "Any | None":
        with self._config.provide_connection() as conn, MssqlPythonCursor(conn) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if commit:
                conn.commit()
            return row

    def _execute_fetchall(self, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        with self._config.provide_connection() as conn, MssqlPythonCursor(conn) as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def _execute(self, sql: str, params: "tuple[Any, ...]" = (), *, commit: bool = False) -> int:
        with self._config.provide_connection() as conn, MssqlPythonCursor(conn) as cursor:
            cursor.execute(sql, params)
            rowcount = _cursor_rowcount(cursor)
            if commit:
                conn.commit()
            return rowcount


def _adk_config(config: Any) -> MssqlPythonADKConfig:
    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("MssqlPythonADKConfig", adk_config)


def _configured_json_column_type(native_json: "bool | None") -> "str | None":
    if native_json is True:
        return JSON_NATIVE_COLUMN_TYPE
    if native_json is False:
        return JSON_FALLBACK_COLUMN_TYPE
    return None


def _json_column_type_from_sync_driver(driver: "MssqlPythonDriver") -> str:
    version_info = driver.data_dictionary.get_version(driver)
    if isinstance(version_info, MssqlVersionInfo) and version_info.supports_native_json():
        return JSON_NATIVE_COLUMN_TYPE
    return JSON_FALLBACK_COLUMN_TYPE


def _sessions_table_ddl(table: str, json_column_type: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f",\n        {owner_id_column_ddl}" if owner_id_column_ddl else ""
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'{_escape_sql_literal(table)}' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE {_table_ref(table)} (
        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {_constraint_ref("df", table, "row_id")} DEFAULT NEWSEQUENTIALID(),
        id NVARCHAR(128) NOT NULL,
        app_name NVARCHAR(128) NOT NULL,
        user_id NVARCHAR(128) NOT NULL{owner_line},
        state {json_column_type} NOT NULL,
        create_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "create_time")} DEFAULT SYSUTCDATETIME(),
        update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
        CONSTRAINT {_constraint_ref("pk", table, "row_id")} PRIMARY KEY (row_id),
        CONSTRAINT {_constraint_ref("uq", table, "id")} UNIQUE (id)
    );
END;
{_create_index_sql(table, f"idx_{table}_app_user", "app_name, user_id")}
{_create_index_sql(table, f"idx_{table}_update_time", "update_time DESC")}
"""


def _events_table_ddl(table: str, session_table: str, json_column_type: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'{_escape_sql_literal(table)}' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE {_table_ref(table)} (
        row_id UNIQUEIDENTIFIER NOT NULL CONSTRAINT {_constraint_ref("df", table, "row_id")} DEFAULT NEWSEQUENTIALID(),
        id NVARCHAR(128) NOT NULL,
        app_name NVARCHAR(128) NOT NULL,
        user_id NVARCHAR(128) NOT NULL,
        session_id NVARCHAR(128) NOT NULL,
        invocation_id NVARCHAR(256) NOT NULL,
        timestamp DATETIME2(6) NOT NULL,
        event_data {json_column_type} NOT NULL,
        CONSTRAINT {_constraint_ref("pk", table, "row_id")} PRIMARY KEY (row_id),
        CONSTRAINT {_constraint_ref("uq", table, "id")} UNIQUE (id),
        CONSTRAINT {_constraint_ref("fk", table, "session")} FOREIGN KEY (session_id)
            REFERENCES {_table_ref(session_table)}(id) ON DELETE CASCADE
    );
END;
{_create_index_sql(table, f"idx_{table}_scope", "app_name, user_id, session_id, timestamp ASC")}
{_create_index_sql(table, f"idx_{table}_session", "session_id, timestamp ASC")}
{_create_index_sql(table, f"idx_{table}_invocation", "invocation_id")}
{_create_index_sql(table, f"idx_{table}_timestamp", "timestamp ASC")}
"""


def _app_states_table_ddl(table: str, json_column_type: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'{_escape_sql_literal(table)}' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE {_table_ref(table)} (
        app_name NVARCHAR(128) NOT NULL,
        state {json_column_type} NOT NULL,
        update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
        CONSTRAINT {_constraint_ref("pk", table, "app_name")} PRIMARY KEY (app_name)
    );
END;
"""


def _user_states_table_ddl(table: str, json_column_type: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'{_escape_sql_literal(table)}' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE {_table_ref(table)} (
        app_name NVARCHAR(128) NOT NULL,
        user_id NVARCHAR(128) NOT NULL,
        state {json_column_type} NOT NULL,
        update_time DATETIME2(6) NOT NULL CONSTRAINT {_constraint_ref("df", table, "update_time")} DEFAULT SYSUTCDATETIME(),
        CONSTRAINT {_constraint_ref("pk", table, "app_user")} PRIMARY KEY (app_name, user_id)
    );
END;
"""


def _metadata_table_ddl(table: str) -> str:
    return f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'{_escape_sql_literal(table)}' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE {_table_ref(table)} (
        [key] NVARCHAR(128) NOT NULL,
        value NVARCHAR(512) NOT NULL,
        CONSTRAINT {_constraint_ref("pk", table, "key")} PRIMARY KEY ([key])
    );
END;
"""


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    return f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'{_escape_sql_literal(index_name)}'
      AND object_id = OBJECT_ID(N'{_escape_sql_literal(MSSQL_SCHEMA)}.{_escape_sql_literal(table)}')
)
BEGIN
    CREATE INDEX {_quote_identifier(index_name)} ON {_table_ref(table)} ({columns});
END;
"""


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


def _metadata_seed_sql(table: str) -> str:
    return f"""
    MERGE INTO {_table_ref(table)} WITH (HOLDLOCK) AS target
    USING (SELECT N'schema_version' AS [key], N'1' AS value) AS source
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
    top_clause = "TOP (?) " if limit is not None else ""
    params: list[Any] = [limit] if limit is not None else []
    params.extend([app_name, user_id, session_id])
    after_clause = ""
    if after_timestamp is not None:
        after_clause = " AND timestamp > ?"
        params.append(after_timestamp)
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
        event_record["timestamp"],
        to_json(event_record["event_data"]),
    )


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


def _cursor_rowcount(cursor: Any) -> int:
    rowcount = getattr(cursor, "rowcount", 0)
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


def _is_mssql_table_missing(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "invalid object name" in text or _mssql_error_number(exc) == MSSQL_TABLE_NOT_FOUND_ERROR


def _mssql_error_number(exc: BaseException) -> "int | None":
    matches = MSSQL_ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None


def _quote_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _table_ref(table: str) -> str:
    return f"{_quote_identifier(MSSQL_SCHEMA)}.{_quote_identifier(table)}"


def _constraint_ref(prefix: str, table: str, suffix: str) -> str:
    return _quote_identifier(f"{prefix}_{table}_{suffix}")


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)
