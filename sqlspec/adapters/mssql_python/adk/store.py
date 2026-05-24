"""mssql-python ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.mssql_python.data_dictionary import MssqlVersionInfo
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.mssql_python.config import MssqlPythonAsyncConfig, MssqlPythonConfig

__all__ = ("MssqlPythonADKStore", "MssqlPythonAsyncADKStore", "MssqlPythonSyncADKStore")


class _MssqlPythonADKStoreMixin:
    """Shared T-SQL ADK DDL and query helpers."""

    __slots__ = ()

    _state_column_type: str | None
    _version_info: MssqlVersionInfo | None
    _app_state_table: str
    _events_table: str
    _metadata_table: str
    _owner_id_column_ddl: str | None
    _owner_id_column_name: str | None
    _session_table: str
    _user_state_table: str

    def _sessions_ddl(self, state_type: str) -> str:
        owner_id_line = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return _idempotent_table_script(
            self._session_table,
            f"""
            CREATE TABLE {self._session_table} (
                id NVARCHAR(128) PRIMARY KEY,
                app_name NVARCHAR(128) NOT NULL,
                user_id NVARCHAR(128) NOT NULL,
                state {state_type} NOT NULL DEFAULT N'{{}}',
                create_time DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
                update_time DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
                {owner_id_line}
            );
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'idx_{self._session_table}_app_user'
                  AND object_id = OBJECT_ID(N'dbo.{self._session_table}')
            )
                CREATE INDEX idx_{self._session_table}_app_user ON {self._session_table}(app_name, user_id);
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'idx_{self._session_table}_update_time'
                  AND object_id = OBJECT_ID(N'dbo.{self._session_table}')
            )
                CREATE INDEX idx_{self._session_table}_update_time ON {self._session_table}(update_time DESC);
            """,
        )

    def _events_ddl(self, state_type: str) -> str:
        return _idempotent_table_script(
            self._events_table,
            f"""
            CREATE TABLE {self._events_table} (
                session_id NVARCHAR(128) NOT NULL,
                invocation_id NVARCHAR(256) NOT NULL,
                author NVARCHAR(256) NOT NULL,
                timestamp DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
                event_data {state_type} NOT NULL,
                FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
            );
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'idx_{self._events_table}_session'
                  AND object_id = OBJECT_ID(N'dbo.{self._events_table}')
            )
                CREATE INDEX idx_{self._events_table}_session ON {self._events_table}(session_id, timestamp ASC);
            """,
        )

    def _app_states_ddl(self, state_type: str) -> str:
        return _idempotent_table_script(
            self._app_state_table,
            f"""
            CREATE TABLE {self._app_state_table} (
                app_name NVARCHAR(128) PRIMARY KEY,
                state {state_type} NOT NULL DEFAULT N'{{}}',
                update_time DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
            );
            """,
        )

    def _user_states_ddl(self, state_type: str) -> str:
        return _idempotent_table_script(
            self._user_state_table,
            f"""
            CREATE TABLE {self._user_state_table} (
                app_name NVARCHAR(128) NOT NULL,
                user_id NVARCHAR(128) NOT NULL,
                state {state_type} NOT NULL DEFAULT N'{{}}',
                update_time DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
                PRIMARY KEY (app_name, user_id)
            );
            """,
        )

    def _metadata_ddl(self) -> str:
        return _idempotent_table_script(
            self._metadata_table,
            f"""
            CREATE TABLE {self._metadata_table} (
                [key] NVARCHAR(128) PRIMARY KEY,
                value NVARCHAR(512) NOT NULL
            );
            """,
        )

    def _seed_metadata_sql_text(self) -> str:
        return f"""
        IF NOT EXISTS (SELECT 1 FROM {self._metadata_table} WHERE [key] = N'schema_version')
            INSERT INTO {self._metadata_table} ([key], value) VALUES (N'schema_version', N'1');
        """

    def _app_state_merge_sql(self) -> str:
        return f"""
        MERGE {self._app_state_table} WITH (HOLDLOCK) AS target
        USING (SELECT :app_name AS app_name, :state_json AS state_json) AS source
        ON target.app_name = source.app_name
        WHEN MATCHED THEN
            UPDATE SET state = source.state_json, update_time = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, source.state_json, SYSUTCDATETIME());
        """

    def _user_state_merge_sql(self) -> str:
        return f"""
        MERGE {self._user_state_table} WITH (HOLDLOCK) AS target
        USING (
            SELECT :app_name AS app_name, :user_id AS user_id, :state_json AS state_json
        ) AS source
        ON target.app_name = source.app_name AND target.user_id = source.user_id
        WHEN MATCHED THEN
            UPDATE SET state = source.state_json, update_time = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, source.state_json, SYSUTCDATETIME());
        """

    def _metadata_merge_sql(self) -> str:
        return f"""
        MERGE {self._metadata_table} WITH (HOLDLOCK) AS target
        USING (SELECT :key AS [key], :value AS value) AS source
        ON target.[key] = source.[key]
        WHEN MATCHED THEN UPDATE SET value = source.value
        WHEN NOT MATCHED THEN INSERT ([key], value) VALUES (source.[key], source.value);
        """

    def _get_drop_app_states_table_sql(self) -> str:
        return _drop_table_script(self._app_state_table)

    def _get_drop_user_states_table_sql(self) -> str:
        return _drop_table_script(self._user_state_table)

    def _get_drop_metadata_table_sql(self) -> str:
        return _drop_table_script(self._metadata_table)

    def _get_drop_tables_sql(self) -> list[str]:
        return [
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            _drop_table_script(self._events_table),
            _drop_table_script(self._session_table),
        ]


class MssqlPythonAsyncADKStore(_MssqlPythonADKStoreMixin, BaseAsyncADKStore["MssqlPythonAsyncConfig"]):
    """Async ADK store for Microsoft SQL Server via mssql-python."""

    __slots__ = ("_state_column_type", "_version_info")

    def __init__(self, config: "MssqlPythonAsyncConfig") -> None:
        super().__init__(config)
        self._state_column_type = None
        self._version_info = None

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())
            await driver.execute_script(await self._get_create_app_states_table_sql())
            await driver.execute_script(await self._get_create_user_states_table_sql())
            await driver.execute_script(await self._get_create_metadata_table_sql())
            await driver.execute_script(await self._get_seed_metadata_sql())
            await driver.commit()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        state_json = _json_to_db(state)
        async with self._config.provide_session() as driver:
            if self._owner_id_column_name:
                await driver.execute(
                    f"""
                    INSERT INTO {self._session_table}
                        (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                    VALUES (:session_id, :app_name, :user_id, :owner_id, :state_json, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    session_id=session_id,
                    app_name=app_name,
                    user_id=user_id,
                    owner_id=owner_id,
                    state_json=state_json,
                )
            else:
                await driver.execute(
                    f"""
                    INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                    VALUES (:session_id, :app_name, :user_id, :state_json, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    session_id=session_id,
                    app_name=app_name,
                    user_id=user_id,
                    state_json=state_json,
                )
            await driver.commit()
        session = await self.get_session(session_id)
        if session is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return session

    async def get_session(self, session_id: str, *, renew_for: "int | timedelta | None" = None) -> SessionRecord | None:
        try:
            async with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    await driver.execute(
                        f"UPDATE {self._session_table} SET update_time = SYSUTCDATETIME() WHERE id = :session_id",
                        session_id=session_id,
                    )
                    await driver.commit()
                row = await driver.select_one_or_none(
                    f"""
                    SELECT id, app_name, user_id, state, create_time, update_time
                    FROM {self._session_table}
                    WHERE id = :session_id
                    """,
                    session_id=session_id,
                )
            return _session_from_row(row) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"""
                UPDATE {self._session_table}
                SET state = :state_json, update_time = SYSUTCDATETIME()
                WHERE id = :session_id
                """,
                state_json=_json_to_db(state),
                session_id=session_id,
            )
            await driver.commit()

    async def delete_session(self, session_id: str) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(f"DELETE FROM {self._session_table} WHERE id = :session_id", session_id=session_id)
            await driver.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        try:
            async with self._config.provide_session() as driver:
                if user_id is None:
                    rows = await driver.select(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {self._session_table}
                        WHERE app_name = :app_name
                        ORDER BY update_time DESC
                        """,
                        app_name=app_name,
                    )
                else:
                    rows = await driver.select(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {self._session_table}
                        WHERE app_name = :app_name AND user_id = :user_id
                        ORDER BY update_time DESC
                        """,
                        app_name=app_name,
                        user_id=user_id,
                    )
            return [_session_from_row(row) for row in rows]
        except Exception as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"""
                INSERT INTO {self._events_table} (session_id, invocation_id, author, timestamp, event_data)
                VALUES (:session_id, :invocation_id, :author, :timestamp, :event_data)
                """,
                session_id=event_record["session_id"],
                invocation_id=event_record["invocation_id"],
                author=event_record["author"],
                timestamp=event_record["timestamp"],
                event_data=_json_to_db(event_record["event_data"]),
            )
            await driver.commit()

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: dict[str, Any],
        *,
        app_name: str | None = None,
        user_id: str | None = None,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> SessionRecord:
        async with self._config.provide_session() as driver:
            await driver.execute(
                f"""
                INSERT INTO {self._events_table} (session_id, invocation_id, author, timestamp, event_data)
                VALUES (:session_id, :invocation_id, :author, :timestamp, :event_data)
                """,
                session_id=event_record["session_id"],
                invocation_id=event_record["invocation_id"],
                author=event_record["author"],
                timestamp=event_record["timestamp"],
                event_data=_json_to_db(event_record["event_data"]),
            )
            await driver.execute(
                f"""
                UPDATE {self._session_table}
                SET state = :state_json, update_time = SYSUTCDATETIME()
                WHERE id = :session_id
                """,
                state_json=_json_to_db(state),
                session_id=session_id,
            )
            if app_state:
                if app_name is None:
                    msg = "app_name is required when app_state is provided."
                    raise ValueError(msg)
                await driver.execute(self._app_state_merge_sql(), app_name=app_name, state_json=_json_to_db(app_state))
            if user_state:
                if app_name is None or user_id is None:
                    msg = "app_name and user_id are required when user_state is provided."
                    raise ValueError(msg)
                await driver.execute(
                    self._user_state_merge_sql(), app_name=app_name, user_id=user_id, state_json=_json_to_db(user_state)
                )
            await driver.commit()
        session = await self.get_session(session_id)
        if session is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return session

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: int | None = None
    ) -> list[EventRecord]:
        where_clauses = ["session_id = :session_id"]
        params: dict[str, Any] = {"session_id": session_id}
        if after_timestamp is not None:
            where_clauses.append("timestamp > :after_timestamp")
            params["after_timestamp"] = after_timestamp
        top_clause = f"TOP ({int(limit)}) " if limit else ""
        try:
            async with self._config.provide_session() as driver:
                rows = await driver.select(
                    f"""
                    SELECT {top_clause}session_id, invocation_id, author, timestamp, event_data
                    FROM {self._events_table}
                    WHERE {" AND ".join(where_clauses)}
                    ORDER BY timestamp ASC
                    """,
                    **params,
                )
            return [_event_from_row(row) for row in rows]
        except Exception as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def delete_expired_events(self, before: "datetime") -> int:
        async with self._config.provide_session() as driver:
            result = await driver.execute(f"DELETE FROM {self._events_table} WHERE timestamp < :before", before=before)
            await driver.commit()
        return result.rows_affected

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        async with self._config.provide_session() as driver:
            result = await driver.execute(
                f"DELETE FROM {self._session_table} WHERE update_time < :updated_before", updated_before=updated_before
            )
            await driver.commit()
        return result.rows_affected

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT state FROM {self._app_state_table} WHERE app_name = :app_name", app_name=app_name
                )
            return _json_from_db(row["state"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"""
                    SELECT state FROM {self._user_state_table}
                    WHERE app_name = :app_name AND user_id = :user_id
                    """,
                    app_name=app_name,
                    user_id=user_id,
                )
            return _json_from_db(row["state"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(self._app_state_merge_sql(), app_name=app_name, state_json=_json_to_db(state))
            await driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(
                self._user_state_merge_sql(), app_name=app_name, user_id=user_id, state_json=_json_to_db(state)
            )
            await driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT value FROM {self._metadata_table} WHERE [key] = :key", key=key
                )
            return str(row["value"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute(self._metadata_merge_sql(), key=key, value=value)
            await driver.commit()

    async def _detect_state_column_type(self) -> str:
        if self._state_column_type is not None:
            return self._state_column_type
        async with self._config.provide_session() as session:
            self._version_info = await session.data_dictionary.get_version(session)
        self._state_column_type = _state_column_type(self._version_info)
        return self._state_column_type

    async def _get_create_sessions_table_sql(self) -> str:
        return self._sessions_ddl(await self._detect_state_column_type())

    async def _get_create_events_table_sql(self) -> str:
        return self._events_ddl(await self._detect_state_column_type())

    async def _get_create_app_states_table_sql(self) -> str:
        return self._app_states_ddl(await self._detect_state_column_type())

    async def _get_create_user_states_table_sql(self) -> str:
        return self._user_states_ddl(await self._detect_state_column_type())

    async def _get_create_metadata_table_sql(self) -> str:
        return self._metadata_ddl()

    async def _get_seed_metadata_sql(self) -> str:
        return self._seed_metadata_sql_text()


class MssqlPythonADKStore(_MssqlPythonADKStoreMixin, BaseAsyncADKStore["MssqlPythonConfig"]):
    """Async-compatible ADK store for mssql-python synchronous configs."""

    __slots__ = ("_state_column_type", "_version_info")

    def __init__(self, config: "MssqlPythonConfig") -> None:
        super().__init__(config)
        self._state_column_type = None
        self._version_info = None

    async def create_tables(self) -> None:
        await async_(self._create_tables_sync)()

    def _create_tables_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_sessions_table_sql)())
            driver.execute_script(run_(self._get_create_events_table_sql)())
            driver.execute_script(run_(self._get_create_app_states_table_sql)())
            driver.execute_script(run_(self._get_create_user_states_table_sql)())
            driver.execute_script(run_(self._get_create_metadata_table_sql)())
            driver.execute_script(run_(self._get_seed_metadata_sql)())
            driver.commit()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        return await async_(self._create_session_sync)(session_id, app_name, user_id, state, owner_id)

    def _create_session_sync(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> SessionRecord:
        state_json = _json_to_db(state)
        with self._config.provide_session() as driver:
            if self._owner_id_column_name:
                driver.execute(
                    f"""
                    INSERT INTO {self._session_table}
                        (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
                    VALUES (:session_id, :app_name, :user_id, :owner_id, :state_json, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    session_id=session_id,
                    app_name=app_name,
                    user_id=user_id,
                    owner_id=owner_id,
                    state_json=state_json,
                )
            else:
                driver.execute(
                    f"""
                    INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
                    VALUES (:session_id, :app_name, :user_id, :state_json, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    session_id=session_id,
                    app_name=app_name,
                    user_id=user_id,
                    state_json=state_json,
                )
            driver.commit()
        session = self._get_session_sync(session_id)
        if session is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return session

    async def get_session(self, session_id: str, *, renew_for: "int | timedelta | None" = None) -> SessionRecord | None:
        return await async_(self._get_session_sync)(session_id, renew_for)

    def _get_session_sync(self, session_id: str, renew_for: "int | timedelta | None" = None) -> SessionRecord | None:
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(
                        f"UPDATE {self._session_table} SET update_time = SYSUTCDATETIME() WHERE id = :session_id",
                        session_id=session_id,
                    )
                    driver.commit()
                row = driver.select_one_or_none(
                    f"""
                    SELECT id, app_name, user_id, state, create_time, update_time
                    FROM {self._session_table}
                    WHERE id = :session_id
                    """,
                    session_id=session_id,
                )
            return _session_from_row(row) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def update_session_state(self, session_id: str, state: dict[str, Any]) -> None:
        await async_(self._update_session_state_sync)(session_id, state)

    def _update_session_state_sync(self, session_id: str, state: dict[str, Any]) -> None:
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                UPDATE {self._session_table}
                SET state = :state_json, update_time = SYSUTCDATETIME()
                WHERE id = :session_id
                """,
                state_json=_json_to_db(state),
                session_id=session_id,
            )
            driver.commit()

    async def delete_session(self, session_id: str) -> None:
        await async_(self._delete_session_sync)(session_id)

    def _delete_session_sync(self, session_id: str) -> None:
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {self._session_table} WHERE id = :session_id", session_id=session_id)
            driver.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        return await async_(self._list_sessions_sync)(app_name, user_id)

    def _list_sessions_sync(self, app_name: str, user_id: str | None = None) -> list[SessionRecord]:
        try:
            with self._config.provide_session() as driver:
                if user_id is None:
                    rows = driver.select(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {self._session_table}
                        WHERE app_name = :app_name
                        ORDER BY update_time DESC
                        """,
                        app_name=app_name,
                    )
                else:
                    rows = driver.select(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {self._session_table}
                        WHERE app_name = :app_name AND user_id = :user_id
                        ORDER BY update_time DESC
                        """,
                        app_name=app_name,
                        user_id=user_id,
                    )
            return [_session_from_row(row) for row in rows]
        except Exception as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def append_event(self, event_record: EventRecord) -> None:
        await async_(self._append_event_sync)(event_record)

    def _append_event_sync(self, event_record: EventRecord) -> None:
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                INSERT INTO {self._events_table} (session_id, invocation_id, author, timestamp, event_data)
                VALUES (:session_id, :invocation_id, :author, :timestamp, :event_data)
                """,
                session_id=event_record["session_id"],
                invocation_id=event_record["invocation_id"],
                author=event_record["author"],
                timestamp=event_record["timestamp"],
                event_data=_json_to_db(event_record["event_data"]),
            )
            driver.commit()

    async def append_event_and_update_state(
        self,
        event_record: EventRecord,
        session_id: str,
        state: dict[str, Any],
        *,
        app_name: str | None = None,
        user_id: str | None = None,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> SessionRecord:
        return await async_(self._append_event_and_update_state_sync)(
            event_record,
            session_id,
            state,
            app_name=app_name,
            user_id=user_id,
            app_state=app_state,
            user_state=user_state,
        )

    def _append_event_and_update_state_sync(
        self,
        event_record: EventRecord,
        session_id: str,
        state: dict[str, Any],
        *,
        app_name: str | None = None,
        user_id: str | None = None,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> SessionRecord:
        with self._config.provide_session() as driver:
            driver.execute(
                f"""
                INSERT INTO {self._events_table} (session_id, invocation_id, author, timestamp, event_data)
                VALUES (:session_id, :invocation_id, :author, :timestamp, :event_data)
                """,
                session_id=event_record["session_id"],
                invocation_id=event_record["invocation_id"],
                author=event_record["author"],
                timestamp=event_record["timestamp"],
                event_data=_json_to_db(event_record["event_data"]),
            )
            driver.execute(
                f"""
                UPDATE {self._session_table}
                SET state = :state_json, update_time = SYSUTCDATETIME()
                WHERE id = :session_id
                """,
                state_json=_json_to_db(state),
                session_id=session_id,
            )
            if app_state:
                if app_name is None:
                    msg = "app_name is required when app_state is provided."
                    raise ValueError(msg)
                driver.execute(self._app_state_merge_sql(), app_name=app_name, state_json=_json_to_db(app_state))
            if user_state:
                if app_name is None or user_id is None:
                    msg = "app_name and user_id are required when user_state is provided."
                    raise ValueError(msg)
                driver.execute(
                    self._user_state_merge_sql(), app_name=app_name, user_id=user_id, state_json=_json_to_db(user_state)
                )
            driver.commit()
        session = self._get_session_sync(session_id)
        if session is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return session

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: int | None = None
    ) -> list[EventRecord]:
        return await async_(self._get_events_sync)(session_id, after_timestamp, limit)

    def _get_events_sync(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: int | None = None
    ) -> list[EventRecord]:
        where_clauses = ["session_id = :session_id"]
        params: dict[str, Any] = {"session_id": session_id}
        if after_timestamp is not None:
            where_clauses.append("timestamp > :after_timestamp")
            params["after_timestamp"] = after_timestamp
        top_clause = f"TOP ({int(limit)}) " if limit else ""
        try:
            with self._config.provide_session() as driver:
                rows = driver.select(
                    f"""
                    SELECT {top_clause}session_id, invocation_id, author, timestamp, event_data
                    FROM {self._events_table}
                    WHERE {" AND ".join(where_clauses)}
                    ORDER BY timestamp ASC
                    """,
                    **params,
                )
            return [_event_from_row(row) for row in rows]
        except Exception as exc:
            if _is_missing_table_error(exc):
                return []
            raise

    async def delete_expired_events(self, before: "datetime") -> int:
        return await async_(self._delete_expired_events_sync)(before)

    def _delete_expired_events_sync(self, before: "datetime") -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(f"DELETE FROM {self._events_table} WHERE timestamp < :before", before=before)
            driver.commit()
        return result.rows_affected

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        return await async_(self._delete_idle_sessions_sync)(updated_before)

    def _delete_idle_sessions_sync(self, updated_before: "datetime") -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(
                f"DELETE FROM {self._session_table} WHERE update_time < :updated_before", updated_before=updated_before
            )
            driver.commit()
        return result.rows_affected

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        return await async_(self._get_app_state_sync)(app_name)

    def _get_app_state_sync(self, app_name: str) -> dict[str, Any] | None:
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT state FROM {self._app_state_table} WHERE app_name = :app_name", app_name=app_name
                )
            return _json_from_db(row["state"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        return await async_(self._get_user_state_sync)(app_name, user_id)

    def _get_user_state_sync(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"""
                    SELECT state FROM {self._user_state_table}
                    WHERE app_name = :app_name AND user_id = :user_id
                    """,
                    app_name=app_name,
                    user_id=user_id,
                )
            return _json_from_db(row["state"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        await async_(self._upsert_app_state_sync)(app_name, state)

    def _upsert_app_state_sync(self, app_name: str, state: dict[str, Any]) -> None:
        with self._config.provide_session() as driver:
            driver.execute(self._app_state_merge_sql(), app_name=app_name, state_json=_json_to_db(state))
            driver.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        await async_(self._upsert_user_state_sync)(app_name, user_id, state)

    def _upsert_user_state_sync(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        with self._config.provide_session() as driver:
            driver.execute(
                self._user_state_merge_sql(), app_name=app_name, user_id=user_id, state_json=_json_to_db(state)
            )
            driver.commit()

    async def get_metadata(self, key: str) -> str | None:
        return await async_(self._get_metadata_sync)(key)

    def _get_metadata_sync(self, key: str) -> str | None:
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT value FROM {self._metadata_table} WHERE [key] = :key", key=key)
            return str(row["value"]) if row is not None else None
        except Exception as exc:
            if _is_missing_table_error(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        await async_(self._set_metadata_sync)(key, value)

    def _set_metadata_sync(self, key: str, value: str) -> None:
        with self._config.provide_session() as driver:
            driver.execute(self._metadata_merge_sql(), key=key, value=value)
            driver.commit()

    def _detect_state_column_type_sync(self) -> str:
        if self._state_column_type is not None:
            return self._state_column_type
        with self._config.provide_session() as session:
            self._version_info = session.data_dictionary.get_version(session)
        self._state_column_type = _state_column_type(self._version_info)
        return self._state_column_type

    async def _get_create_sessions_table_sql(self) -> str:
        return self._sessions_ddl(self._detect_state_column_type_sync())

    async def _get_create_events_table_sql(self) -> str:
        return self._events_ddl(self._detect_state_column_type_sync())

    async def _get_create_app_states_table_sql(self) -> str:
        return self._app_states_ddl(self._detect_state_column_type_sync())

    async def _get_create_user_states_table_sql(self) -> str:
        return self._user_states_ddl(self._detect_state_column_type_sync())

    async def _get_create_metadata_table_sql(self) -> str:
        return self._metadata_ddl()

    async def _get_seed_metadata_sql(self) -> str:
        return self._seed_metadata_sql_text()


MssqlPythonSyncADKStore = MssqlPythonADKStore


def _is_missing_table_error(exc: BaseException) -> bool:
    text = str(exc)
    return "Invalid object name" in text or "(208)" in text


def _state_column_type(version_info: MssqlVersionInfo | None) -> str:
    if version_info is not None and version_info.supports_native_json():
        return "JSON"
    return "NVARCHAR(MAX)"


def _json_to_db(value: dict[str, Any]) -> str:
    return to_json(value)


def _json_from_db(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return cast("dict[str, Any]", from_json(value))
    if isinstance(value, bytes):
        return cast("dict[str, Any]", from_json(value))
    return cast("dict[str, Any]", value)


def _session_from_row(row: dict[str, Any]) -> SessionRecord:
    return SessionRecord(
        id=str(row["id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        state=_json_from_db(row["state"]),
        create_time=cast("datetime", row["create_time"]),
        update_time=cast("datetime", row["update_time"]),
    )


def _event_from_row(row: dict[str, Any]) -> EventRecord:
    return EventRecord(
        session_id=str(row["session_id"]),
        invocation_id=str(row["invocation_id"]),
        author=str(row["author"]),
        timestamp=cast("datetime", row["timestamp"]),
        event_data=_json_from_db(row["event_data"]),
    )


def _object_id_guard(table_name: str, *, exists: bool) -> str:
    operator = "IS NOT NULL" if exists else "IS NULL"
    return f"OBJECT_ID(N'dbo.{table_name}', N'U') {operator}"


def _idempotent_table_script(table_name: str, create_sql: str) -> str:
    return f"IF {_object_id_guard(table_name, exists=False)} BEGIN {create_sql} END;"


def _drop_table_script(table_name: str) -> str:
    return f"IF {_object_id_guard(table_name, exists=True)} DROP TABLE {table_name};"
