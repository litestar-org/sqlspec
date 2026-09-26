"""IBM Db2 ADK stores for Google Agent Development Kit session and memory storage."""

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, cast

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.core import (
    INDEX_EXISTS_SQL,
    TABLE_EXISTS_SQL,
    extract_sqlstate,
    split_db2_table_name,
    to_db_timestamp,
    utc_now,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    SessionOrderBy,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.extensions.adk.memory import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore, StoredMemory
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = ("Db2AsyncADKMemoryStore", "Db2AsyncADKStore", "Db2SyncADKMemoryStore", "Db2SyncADKStore")

JSON_COLUMN_TYPE: Final[str] = "CLOB(1M)"
MISSING_OBJECT_SQLSTATE: Final[str] = "42704"


class _Db2ADKStoreMixin:
    """Table layout, index, drop, upsert and event-query builders shared by the Db2 ADK session stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _session_table: str
        _events_table: str
        _app_state_table: str
        _user_state_table: str
        _metadata_table: str
        _owner_id_column_ddl: str | None

    def _table_specs(self) -> list[tuple[str, str]]:
        """Return each ADK table name with the DDL that creates it, in creation order."""
        return [
            (self._session_table, _sessions_table_ddl(self._session_table, self._owner_id_column_ddl)),
            (self._events_table, _events_table_ddl(self._events_table, self._session_table)),
            (self._app_state_table, _app_states_table_ddl(self._app_state_table)),
            (self._user_state_table, _user_states_table_ddl(self._user_state_table)),
            (self._metadata_table, _metadata_table_ddl(self._metadata_table)),
        ]

    def _index_specs(self) -> list[tuple[str, str, str]]:
        return [*_sessions_index_specs(self._session_table), *_events_index_specs(self._events_table)]

    def _drop_app_states_table_sql(self) -> str:
        return _drop_table_sql(self._app_state_table)

    def _drop_user_states_table_sql(self) -> str:
        return _drop_table_sql(self._user_state_table)

    def _drop_metadata_table_sql(self) -> str:
        return _drop_table_sql(self._metadata_table)

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            _drop_table_sql(self._events_table),
            _drop_table_sql(self._session_table),
        ]

    def _upsert_app_state_sql(self) -> str:
        return _upsert_state_sql(self._app_state_table, ("app_name",))

    def _upsert_user_state_sql(self) -> str:
        return _upsert_state_sql(self._user_state_table, ("app_name", "user_id"))

    def _upsert_metadata_sql(self) -> str:
        return _upsert_metadata_sql(self._metadata_table)

    def _events_query(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        return _events_query(self._events_table, app_name, user_id, session_id, after_timestamp, limit)


class Db2SyncADKStore(_Db2ADKStoreMixin, BaseSyncADKStore[Db2SyncConfig]):
    """Synchronous IBM Db2 ADK session/event store."""

    connector_name: ClassVar[str] = "db2"
    __slots__ = ()

    def create_tables(self) -> None:
        """Create the ADK tables and indexes the Db2 catalog does not list."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            for table, ddl in self._table_specs():
                _create_missing(driver, TABLE_EXISTS_SQL, table, ddl)
            for index_name, index_table, columns in self._index_specs():
                _create_missing(
                    driver, INDEX_EXISTS_SQL, index_name, _create_index_sql(index_table, index_name, columns)
                )
            driver.commit()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Create a new ADK session."""
        params = _session_insert_params(
            session_id, app_name, user_id, state, self._owner_id_column_name is not None, owner_id
        )
        with self._config.provide_session() as driver:
            driver.execute(_insert_session_sql(self._session_table, self._owner_id_column_name), params)
            row = driver.select_one_or_none(_session_select_sql(self._session_table), (app_name, user_id, session_id))
            driver.commit()
        return _created_session_record(row)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Return a scoped session or None if absent."""
        try:
            with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    driver.execute(_touch_session_sql(self._session_table), (utc_now(), app_name, user_id, session_id))
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

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace a session durable state."""
        self._execute(
            _update_session_state_sql(self._session_table),
            (to_json(state), utc_now(), app_name, user_id, session_id),
            commit=True,
        )

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions for an application, optionally scoped to a user."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            rows = self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session. Event rows cascade through the FK."""
        self._execute(_delete_session_sql(self._session_table), (app_name, user_id, session_id), commit=True)

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        self._execute(_insert_event_sql(self._events_table), _event_insert_params(event_record), commit=True)

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event and update durable session/scoped state."""
        with self._config.provide_session() as driver:
            driver.execute(
                _update_session_state_sql(self._session_table),
                (to_json(state), utc_now(), app_name, user_id, session_id),
            )
            row = driver.select_one_or_none(_session_select_sql(self._session_table), (app_name, user_id, session_id))
            if row is None:
                _raise_session_not_found(session_id)
            driver.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            if app_state is not None:
                driver.execute(self._upsert_app_state_sql(), (app_name, to_json(app_state), utc_now()))
            if user_state is not None:
                driver.execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(user_state), utc_now()))
            driver.commit()
        return _session_record_from_row(row)

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
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

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than before."""
        return self._delete_counted(*_retention_statements(self._events_table, "timestamp", before, app_name))

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose update_time is older than updated_before."""
        return self._delete_counted(
            *_retention_statements(self._session_table, "update_time", updated_before, app_name)
        )

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose update_time is older than updated_before."""
        return self._delete_counted(
            *_retention_statements(self._user_state_table, "update_time", updated_before, app_name)
        )

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped state."""
        row = self._fetch_scoped_row(_app_state_select_sql(self._app_state_table), (app_name,))
        return _json_dict(row["state"]) if row is not None else None

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped state."""
        row = self._fetch_scoped_row(_user_state_select_sql(self._user_state_table), (app_name, user_id))
        return _json_dict(row["state"]) if row is not None else None

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped state."""
        self._execute(self._upsert_app_state_sql(), (app_name, to_json(state), utc_now()), commit=True)

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped state."""
        self._execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(state), utc_now()), commit=True)

    def get_metadata(self, key: str) -> str | None:
        """Return a metadata value."""
        return _metadata_value(self._fetch_scoped_row(_metadata_select_sql(self._metadata_table), (key,)))

    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata key-value pair."""
        self._execute(self._upsert_metadata_sql(), (key, value), commit=True)

    def _sessions_table_ddl(self) -> str:
        """Return Db2 DDL for the sessions table."""
        return _sessions_table_ddl(self._session_table, self._owner_id_column_ddl)

    def _events_table_ddl(self) -> str:
        """Return Db2 DDL for the events table."""
        return _events_table_ddl(self._events_table, self._session_table)

    def _app_states_table_ddl(self) -> str:
        """Return Db2 DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        """Return Db2 DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        """Return Db2 DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    def _fetch_scoped_row(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        """Read one row, treating a missing table as no row."""
        try:
            return self._execute_fetchone(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise

    def _delete_counted(self, count_sql: str, delete_sql: str, params: tuple[Any, ...]) -> int:
        """Count, then delete, the matching rows; a missing table deletes nothing."""
        try:
            count = self._select_count(count_sql, params)
            self._execute(delete_sql, params, commit=True)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return 0
            raise
        return count

    def _execute_fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._config.provide_session() as driver:
            return driver.select_one_or_none(sql, params)

    def _execute_fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._config.provide_session() as driver:
            return driver.select(sql, params)

    def _execute(self, sql: str, params: tuple[Any, ...] = (), *, commit: bool = False) -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            if commit:
                driver.commit()
            return int(result.rows_affected)

    def _select_count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        with self._config.provide_session() as driver:
            value = driver.select_value(sql, params)
        return int(value or 0)


class Db2AsyncADKStore(_Db2ADKStoreMixin, BaseAsyncADKStore[Db2AsyncConfig]):
    """Asynchronous IBM Db2 ADK session/event store."""

    connector_name: ClassVar[str] = "db2"
    __slots__ = ()

    async def create_tables(self) -> None:
        """Create the ADK tables and indexes the Db2 catalog does not list."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        async with self._config.provide_session() as driver:
            for table, ddl in self._table_specs():
                await _create_missing_async(driver, TABLE_EXISTS_SQL, table, ddl)
            for index_name, index_table, columns in self._index_specs():
                await _create_missing_async(
                    driver, INDEX_EXISTS_SQL, index_name, _create_index_sql(index_table, index_name, columns)
                )
            await driver.commit()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: dict[str, Any], owner_id: Any | None = None
    ) -> StoredSession:
        """Create a new ADK session."""
        params = _session_insert_params(
            session_id, app_name, user_id, state, self._owner_id_column_name is not None, owner_id
        )
        async with self._config.provide_session() as driver:
            await driver.execute(_insert_session_sql(self._session_table, self._owner_id_column_name), params)
            row = await driver.select_one_or_none(
                _session_select_sql(self._session_table), (app_name, user_id, session_id)
            )
            await driver.commit()
        return _created_session_record(row)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Return a scoped session or None if absent."""
        try:
            async with self._config.provide_session() as driver:
                if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
                    await driver.execute(
                        _touch_session_sql(self._session_table), (utc_now(), app_name, user_id, session_id)
                    )
                row = await driver.select_one_or_none(
                    _session_select_sql(self._session_table), (app_name, user_id, session_id)
                )
                if renew_for is not None:
                    await driver.commit()
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise
        return _session_record_from_row(row) if row is not None else None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace a session durable state."""
        await self._execute(
            _update_session_state_sql(self._session_table),
            (to_json(state), utc_now(), app_name, user_id, session_id),
            commit=True,
        )

    async def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List ADK sessions for an application, optionally scoped to a user."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )
        try:
            rows = await self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return []
            raise
        return [_session_record_from_row(row) for row in rows]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session. Event rows cascade through the FK."""
        await self._execute(_delete_session_sql(self._session_table), (app_name, user_id, session_id), commit=True)

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        await self._execute(_insert_event_sql(self._events_table), _event_insert_params(event_record), commit=True)

    async def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state: dict[str, Any] | None = None,
        user_state: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event and update durable session/scoped state."""
        async with self._config.provide_session() as driver:
            await driver.execute(
                _update_session_state_sql(self._session_table),
                (to_json(state), utc_now(), app_name, user_id, session_id),
            )
            row = await driver.select_one_or_none(
                _session_select_sql(self._session_table), (app_name, user_id, session_id)
            )
            if row is None:
                _raise_session_not_found(session_id)
            await driver.execute(_insert_event_sql(self._events_table), _event_insert_params(event_record))
            if app_state is not None:
                await driver.execute(self._upsert_app_state_sql(), (app_name, to_json(app_state), utc_now()))
            if user_state is not None:
                await driver.execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(user_state), utc_now()))
            await driver.commit()
        return _session_record_from_row(row)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Return events for a scoped session ordered by event timestamp."""
        if limit is not None and limit <= 0:
            return []
        sql, params = self._events_query(app_name, user_id, session_id, after_timestamp, limit)
        try:
            rows = await self._execute_fetchall(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return []
            raise
        return [_event_record_from_row(row) for row in rows]

    async def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than before."""
        return await self._delete_counted(*_retention_statements(self._events_table, "timestamp", before, app_name))

    async def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete sessions whose update_time is older than updated_before."""
        return await self._delete_counted(
            *_retention_statements(self._session_table, "update_time", updated_before, app_name)
        )

    async def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user state rows whose update_time is older than updated_before."""
        return await self._delete_counted(
            *_retention_statements(self._user_state_table, "update_time", updated_before, app_name)
        )

    async def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Return app-scoped state."""
        row = await self._fetch_scoped_row(_app_state_select_sql(self._app_state_table), (app_name,))
        return _json_dict(row["state"]) if row is not None else None

    async def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Return user-scoped state."""
        row = await self._fetch_scoped_row(_user_state_select_sql(self._user_state_table), (app_name, user_id))
        return _json_dict(row["state"]) if row is not None else None

    async def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or update app-scoped state."""
        await self._execute(self._upsert_app_state_sql(), (app_name, to_json(state), utc_now()), commit=True)

    async def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or update user-scoped state."""
        await self._execute(self._upsert_user_state_sql(), (app_name, user_id, to_json(state), utc_now()), commit=True)

    async def get_metadata(self, key: str) -> str | None:
        """Return a metadata value."""
        return _metadata_value(await self._fetch_scoped_row(_metadata_select_sql(self._metadata_table), (key,)))

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata key-value pair."""
        await self._execute(self._upsert_metadata_sql(), (key, value), commit=True)

    async def _sessions_table_ddl(self) -> str:
        """Return Db2 DDL for the sessions table."""
        return _sessions_table_ddl(self._session_table, self._owner_id_column_ddl)

    async def _events_table_ddl(self) -> str:
        """Return Db2 DDL for the events table."""
        return _events_table_ddl(self._events_table, self._session_table)

    async def _app_states_table_ddl(self) -> str:
        """Return Db2 DDL for the app-scoped state table."""
        return _app_states_table_ddl(self._app_state_table)

    async def _user_states_table_ddl(self) -> str:
        """Return Db2 DDL for the user-scoped state table."""
        return _user_states_table_ddl(self._user_state_table)

    async def _metadata_table_ddl(self) -> str:
        """Return Db2 DDL for the ADK metadata table."""
        return _metadata_table_ddl(self._metadata_table)

    async def _fetch_scoped_row(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        """Read one row, treating a missing table as no row."""
        try:
            async with self._config.provide_session() as driver:
                return await driver.select_one_or_none(sql, params)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return None
            raise

    async def _delete_counted(self, count_sql: str, delete_sql: str, params: tuple[Any, ...]) -> int:
        """Count, then delete, the matching rows; a missing table deletes nothing."""
        try:
            count = await self._select_count(count_sql, params)
            await self._execute(delete_sql, params, commit=True)
        except SQLSpecError as exc:
            if _is_table_missing(exc):
                return 0
            raise
        return count

    async def _execute_fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self._config.provide_session() as driver:
            return await driver.select(sql, params)

    async def _execute(self, sql: str, params: tuple[Any, ...] = (), *, commit: bool = False) -> int:
        async with self._config.provide_session() as driver:
            result = await driver.execute(sql, params)
            if commit:
                await driver.commit()
            return int(result.rows_affected)

    async def _select_count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        async with self._config.provide_session() as driver:
            value = await driver.select_value(sql, params)
        return int(value or 0)


class _Db2ADKMemoryStoreMixin:
    """Memory-table index and drop builders shared by the Db2 ADK memory stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _memory_table: str

    def _memory_index_specs(self) -> list[tuple[str, str, str]]:
        return _memory_index_specs(self._memory_table)

    def _drop_memory_table_sql(self) -> list[str]:
        return [_drop_table_sql(self._memory_table)]


class Db2SyncADKMemoryStore(_Db2ADKMemoryStoreMixin, BaseSyncADKMemoryStore[Db2SyncConfig]):
    """IBM Db2 ADK memory store for synchronous configs."""

    __slots__ = ()

    def create_tables(self) -> None:
        """Create the memory table and indexes the Db2 catalog does not list."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return
        with self._config.provide_session() as driver:
            _create_missing(driver, TABLE_EXISTS_SQL, self._memory_table, self._memory_table_ddl())
            for index_name, index_table, columns in self._memory_index_specs():
                _create_missing(
                    driver, INDEX_EXISTS_SQL, index_name, _create_index_sql(index_table, index_name, columns)
                )
            driver.commit()

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory entries, skipping duplicates by event_id."""
        _require_memory_enabled(self._enabled)
        if not entries:
            return 0

        inserted_count = 0
        insert_sql = _memory_insert_sql(self._memory_table, self._owner_id_column_name)
        with self._config.provide_session() as driver:
            for entry in entries:
                if driver.select_one_or_none(_memory_exists_sql(self._memory_table), (entry["event_id"],)) is not None:
                    continue
                driver.execute(insert_sql, _memory_insert_row(entry, self._owner_id_column_name is not None, owner_id))
                inserted_count += 1
            driver.commit()
        return inserted_count

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search memory entries with case-insensitive matching."""
        _require_memory_enabled(self._enabled)
        effective_limit = max(0, int(limit if limit is not None else self._max_results))
        if effective_limit == 0:
            return []
        sql, params = _memory_search_statement(
            self._memory_table, query, app_name, user_id, scope_filter, effective_limit
        )
        return [_memory_record_from_row(row) for row in self._execute_fetchall(sql, params)]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        count_sql, delete_sql = _memory_session_delete_statements(self._memory_table)
        count = self._select_count(count_sql, (session_id,))
        self._execute(delete_sql, (session_id,), commit=True)
        return count

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than specified days."""
        count_sql, delete_sql, params = _memory_retention_statements(self._memory_table, days, app_name, scope)
        count = self._select_count(count_sql, params)
        self._execute(delete_sql, params, commit=True)
        return count

    def _memory_table_ddl(self) -> str:
        return _memory_table_ddl(self._memory_table, self._owner_id_column_ddl)

    def _execute_fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._config.provide_session() as driver:
            return driver.select(sql, params)

    def _execute(self, sql: str, params: tuple[Any, ...] = (), *, commit: bool = False) -> int:
        with self._config.provide_session() as driver:
            result = driver.execute(sql, params)
            if commit:
                driver.commit()
            return int(result.rows_affected)

    def _select_count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        with self._config.provide_session() as driver:
            value = driver.select_value(sql, params)
        return int(value or 0)


class Db2AsyncADKMemoryStore(_Db2ADKMemoryStoreMixin, BaseAsyncADKMemoryStore[Db2AsyncConfig]):
    """IBM Db2 ADK memory store for asynchronous configs."""

    __slots__ = ()

    async def create_tables(self) -> None:
        """Create the memory table and indexes the Db2 catalog does not list."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        if not self._enabled:
            return
        async with self._config.provide_session() as driver:
            await _create_missing_async(driver, TABLE_EXISTS_SQL, self._memory_table, await self._memory_table_ddl())
            for index_name, index_table, columns in self._memory_index_specs():
                await _create_missing_async(
                    driver, INDEX_EXISTS_SQL, index_name, _create_index_sql(index_table, index_name, columns)
                )
            await driver.commit()

    async def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert memory entries, skipping duplicates by event_id."""
        _require_memory_enabled(self._enabled)
        if not entries:
            return 0

        inserted_count = 0
        insert_sql = _memory_insert_sql(self._memory_table, self._owner_id_column_name)
        async with self._config.provide_session() as driver:
            for entry in entries:
                existing = await driver.select_one_or_none(_memory_exists_sql(self._memory_table), (entry["event_id"],))
                if existing is not None:
                    continue
                await driver.execute(
                    insert_sql, _memory_insert_row(entry, self._owner_id_column_name is not None, owner_id)
                )
                inserted_count += 1
            await driver.commit()
        return inserted_count

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Search memory entries with case-insensitive matching."""
        _require_memory_enabled(self._enabled)
        effective_limit = max(0, int(limit if limit is not None else self._max_results))
        if effective_limit == 0:
            return []
        sql, params = _memory_search_statement(
            self._memory_table, query, app_name, user_id, scope_filter, effective_limit
        )
        return [_memory_record_from_row(row) for row in await self._execute_fetchall(sql, params)]

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        count_sql, delete_sql = _memory_session_delete_statements(self._memory_table)
        count = await self._select_count(count_sql, (session_id,))
        await self._execute(delete_sql, (session_id,), commit=True)
        return count

    async def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memory entries older than specified days."""
        count_sql, delete_sql, params = _memory_retention_statements(self._memory_table, days, app_name, scope)
        count = await self._select_count(count_sql, params)
        await self._execute(delete_sql, params, commit=True)
        return count

    async def _memory_table_ddl(self) -> str:
        return _memory_table_ddl(self._memory_table, self._owner_id_column_ddl)

    async def _execute_fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self._config.provide_session() as driver:
            return await driver.select(sql, params)

    async def _execute(self, sql: str, params: tuple[Any, ...] = (), *, commit: bool = False) -> int:
        async with self._config.provide_session() as driver:
            result = await driver.execute(sql, params)
            if commit:
                await driver.commit()
            return int(result.rows_affected)

    async def _select_count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        async with self._config.provide_session() as driver:
            value = await driver.select_value(sql, params)
        return int(value or 0)


def _sessions_table_ddl(table: str, owner_id_column_ddl: str | None) -> str:
    owner_line = f",\n    {owner_id_column_ddl}" if owner_id_column_ddl else ""
    return f"""
CREATE TABLE {table} (
    id VARCHAR(128) NOT NULL,
    app_name VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL{owner_line},
    state {JSON_COLUMN_TYPE} NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT pk_{table}_id PRIMARY KEY (id)
)
"""


def _sessions_index_specs(table: str) -> list[tuple[str, str, str]]:
    return [(f"idx_{table}_app_user", table, "app_name, user_id"), (f"idx_{table}_update_time", table, "update_time")]


def _events_table_ddl(table: str, session_table: str) -> str:
    return f"""
CREATE TABLE {table} (
    id VARCHAR(128) NOT NULL,
    app_name VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    invocation_id VARCHAR(256) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    event_data {JSON_COLUMN_TYPE} NOT NULL,
    CONSTRAINT pk_{table}_id PRIMARY KEY (id),
    CONSTRAINT fk_{table}_session FOREIGN KEY (session_id)
        REFERENCES {session_table}(id) ON DELETE CASCADE
)
"""


def _events_index_specs(table: str) -> list[tuple[str, str, str]]:
    return [
        (f"idx_{table}_scope", table, "app_name, user_id, session_id, timestamp"),
        (f"idx_{table}_session", table, "session_id, timestamp"),
        (f"idx_{table}_invocation", table, "invocation_id"),
        (f"idx_{table}_timestamp", table, "timestamp"),
        (f"idx_{table}_app_timestamp", table, "app_name, timestamp"),
    ]


def _app_states_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    app_name VARCHAR(128) NOT NULL,
    state {JSON_COLUMN_TYPE} NOT NULL,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT pk_{table}_app_name PRIMARY KEY (app_name)
)
"""


def _user_states_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    app_name VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    state {JSON_COLUMN_TYPE} NOT NULL,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT pk_{table}_app_user PRIMARY KEY (app_name, user_id)
)
"""


def _metadata_table_ddl(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    "KEY" VARCHAR(128) NOT NULL,
    value VARCHAR(512) NOT NULL,
    CONSTRAINT pk_{table}_key PRIMARY KEY ("KEY")
)
"""


def _create_index_sql(table: str, index_name: str, columns: str) -> str:
    return f"CREATE INDEX {index_name} ON {table} ({columns})"


def _insert_event_sql(table: str) -> str:
    return f"""
    INSERT INTO {table} (
        id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """


def _upsert_state_sql(table: str, key_columns: tuple[str, ...]) -> str:
    source_values = ", ".join("CAST(? AS VARCHAR(128))" for _ in key_columns)
    source_columns = ", ".join((*key_columns, "state", "update_time"))
    match_clause = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
    insert_values = ", ".join(f"source.{column}" for column in (*key_columns, "state", "update_time"))
    return f"""
    MERGE INTO {table} AS target
    USING (VALUES ({source_values}, CAST(? AS CLOB(1M)), CAST(? AS TIMESTAMP))) AS source ({source_columns})
    ON ({match_clause})
    WHEN MATCHED THEN
        UPDATE SET state = source.state, update_time = source.update_time
    WHEN NOT MATCHED THEN
        INSERT ({source_columns})
        VALUES ({insert_values})
    """


def _upsert_metadata_sql(table: str) -> str:
    return f"""
    MERGE INTO {table} AS target
    USING (VALUES (CAST(? AS VARCHAR(128)), CAST(? AS VARCHAR(512)))) AS source ("KEY", value)
    ON (target."KEY" = source."KEY")
    WHEN MATCHED THEN
        UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
        INSERT ("KEY", value)
        VALUES (source."KEY", source.value)
    """


def _events_query(
    table: str, app_name: str, user_id: str, session_id: str, after_timestamp: datetime | None, limit: int | None
) -> tuple[str, tuple[Any, ...]]:
    params: list[Any] = [app_name, user_id, session_id]
    after_clause = ""
    if after_timestamp is not None:
        after_clause = " AND timestamp > ?"
        params.append(to_db_timestamp(after_timestamp))
    limit_clause = ""
    if limit is not None:
        limit_clause = f" FETCH FIRST {int(limit)} ROWS ONLY"
    sql = f"""
    SELECT id, app_name, user_id, session_id, invocation_id, timestamp, event_data
    FROM {table}
    WHERE app_name = ? AND user_id = ? AND session_id = ?{after_clause}
    ORDER BY timestamp ASC{limit_clause}
    """
    return sql, tuple(params)


def _event_insert_params(event_record: StoredEvent) -> tuple[Any, ...]:
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        to_db_timestamp(event_record["timestamp"]),
        to_json(event_record["event_data"]),
    )


def _session_record_from_row(row: Any) -> StoredSession:
    return StoredSession(
        id=str(row["id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        state=_json_dict(row["state"]),
        create_time=_datetime_value(row["create_time"]),
        update_time=_datetime_value(row["update_time"]),
    )


def _session_select_sql(table: str) -> str:
    return f"""
    SELECT id, app_name, user_id, state, create_time, update_time
    FROM {table}
    WHERE app_name = ? AND user_id = ? AND id = ?
    FETCH FIRST 1 ROWS ONLY
    """


def _event_record_from_row(row: Any) -> StoredEvent:
    return StoredEvent(
        id=str(row["id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        session_id=str(row["session_id"]),
        invocation_id=str(row["invocation_id"]),
        timestamp=_datetime_value(row["timestamp"]),
        event_data=_json_dict(row["event_data"]),
    )


def _memory_insert_params(entry: StoredMemory) -> tuple[Any, ...]:
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry.get("scope", "user"),
        entry["event_id"],
        entry["author"],
        to_db_timestamp(entry["timestamp"]),
        to_json(entry["content_json"]),
        entry["content_text"],
        to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        to_db_timestamp(entry["inserted_at"]),
    )


def _memory_record_from_row(row: Any) -> StoredMemory:
    return StoredMemory(
        id=str(row["id"]),
        session_id=str(row["session_id"]),
        app_name=str(row["app_name"]),
        user_id=str(row["user_id"]),
        scope=str(row["scope"] or "user"),
        event_id=str(row["event_id"]),
        author=cast("str | None", row["author"]),
        timestamp=_datetime_value(row["timestamp"]),
        content_json=_json_dict(row["content_json"]),
        content_text=str(row["content_text"] or ""),
        metadata_json=_optional_json_dict(row["metadata_json"]),
        inserted_at=_datetime_value(row["inserted_at"]),
        embedding=None,
    )


def _json_dict(value: Any) -> dict[str, Any]:
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


def _optional_json_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _json_dict(value)


def _datetime_value(value: Any) -> datetime:
    """Decode a Db2 timestamp value as an aware UTC datetime.

    Naive datetimes and ISO text without an offset are taken to be UTC.

    Args:
        value: ``datetime``, or ISO-8601 text as ``str`` or ``bytes``.

    Returns:
        The aware UTC datetime.

    Raises:
        TypeError: When the value is of any other type.
    """
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
    msg = f"Unsupported Db2 timestamp value: {type(value).__name__}"
    raise TypeError(msg)


def _is_table_missing(exc: BaseException) -> bool:
    """Return whether an error reports an undefined object (SQLSTATE 42704)."""
    return extract_sqlstate(exc) == MISSING_OBJECT_SQLSTATE


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _build_db2_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = 'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = 'app'", (app_name,)


def _session_list_query(
    session_table: str, app_name: str, user_id: str | None, column: str, direction: str, limit: int | None, offset: int
) -> tuple[str, tuple[Any, ...]]:
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
    FROM {session_table}
    WHERE {where_clause}
    ORDER BY {column} {direction}, id {direction}{page_clause}
    """
    return sql, tuple(params)


def _drop_table_sql(table: str) -> str:
    return f"DROP TABLE {table}"


def _catalog_names(name: str) -> "tuple[str | None, str]":
    """Return the catalog schema and name of an object whose DDL writes the name unquoted."""
    return split_db2_table_name(name.upper())


def _create_missing(driver: "Db2SyncDriver", probe_sql: str, name: str, ddl: str) -> None:
    """Run ``ddl`` unless the catalog probe finds the object.

    Object names are written unquoted in the DDL, so they are probed by their upper-folded
    catalog name.

    Args:
        driver: Session driver.
        probe_sql: ``TABLE_EXISTS_SQL`` or ``INDEX_EXISTS_SQL``.
        name: Unquoted table or index name.
        ddl: Statement creating the object.
    """
    if driver.select_one_or_none(probe_sql, _catalog_names(name)) is None:
        driver.execute(ddl)


async def _create_missing_async(driver: "Db2AsyncDriver", probe_sql: str, name: str, ddl: str) -> None:
    """Run ``ddl`` unless the catalog probe finds the object, on an async driver.

    Args:
        driver: Async session driver.
        probe_sql: ``TABLE_EXISTS_SQL`` or ``INDEX_EXISTS_SQL``.
        name: Unquoted table or index name.
        ddl: Statement creating the object.
    """
    if await driver.select_one_or_none(probe_sql, _catalog_names(name)) is None:
        await driver.execute(ddl)


def _insert_session_sql(table: str, owner_column_name: "str | None") -> str:
    owner_column = f", {owner_column_name}" if owner_column_name else ""
    owner_param = ", ?" if owner_column_name else ""
    return f"""
    INSERT INTO {table} (
        id, app_name, user_id{owner_column}, state, create_time, update_time
    )
    VALUES (?, ?, ?{owner_param}, ?, ?, ?)
    """


def _session_insert_params(
    session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", has_owner: bool, owner_id: Any
) -> "tuple[Any, ...]":
    now = utc_now()
    if has_owner:
        return (session_id, app_name, user_id, owner_id, to_json(state), now, now)
    return (session_id, app_name, user_id, to_json(state), now, now)


def _created_session_record(row: "dict[str, Any] | None") -> StoredSession:
    if row is None:
        msg = "Failed to fetch created session"
        raise RuntimeError(msg)
    return _session_record_from_row(row)


def _touch_session_sql(table: str) -> str:
    return f"""
    UPDATE {table}
    SET update_time = ?
    WHERE app_name = ? AND user_id = ? AND id = ?
    """


def _update_session_state_sql(table: str) -> str:
    return f"""
    UPDATE {table}
    SET state = ?, update_time = ?
    WHERE app_name = ? AND user_id = ? AND id = ?
    """


def _delete_session_sql(table: str) -> str:
    return f"DELETE FROM {table} WHERE app_name = ? AND user_id = ? AND id = ?"


def _retention_statements(
    table: str, column: str, cutoff: datetime, app_name: "str | None"
) -> "tuple[str, str, tuple[Any, ...]]":
    """Return the count and delete statements for rows older than ``cutoff`` and their parameters."""
    where_sql = f"{column} < ?"
    params: list[Any] = [to_db_timestamp(cutoff)]
    if app_name is not None:
        where_sql += " AND app_name = ?"
        params.append(app_name)
    return (
        f"SELECT COUNT(*) AS row_count FROM {table} WHERE {where_sql}",
        f"DELETE FROM {table} WHERE {where_sql}",
        tuple(params),
    )


def _app_state_select_sql(table: str) -> str:
    return f"SELECT state FROM {table} WHERE app_name = ? FETCH FIRST 1 ROWS ONLY"


def _user_state_select_sql(table: str) -> str:
    return f"""
    SELECT state
    FROM {table}
    WHERE app_name = ? AND user_id = ?
    FETCH FIRST 1 ROWS ONLY
    """


def _metadata_select_sql(table: str) -> str:
    return f'SELECT value FROM {table} WHERE "KEY" = ? FETCH FIRST 1 ROWS ONLY'


def _metadata_value(row: "dict[str, Any] | None") -> "str | None":
    if row is None:
        return None
    value = row["value"]
    return str(value) if value is not None else None


def _memory_table_ddl(table: str, owner_id_column_ddl: "str | None") -> str:
    owner_line = f",\n    {owner_id_column_ddl}" if owner_id_column_ddl else ""
    return f"""
CREATE TABLE {table} (
    id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    app_name VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    scope VARCHAR(16) NOT NULL DEFAULT 'user',
    event_id VARCHAR(128) NOT NULL,
    author VARCHAR(256),
    timestamp TIMESTAMP NOT NULL,
    content_json {JSON_COLUMN_TYPE} NOT NULL,
    content_text CLOB(1M) NOT NULL,
    metadata_json {JSON_COLUMN_TYPE},
    inserted_at TIMESTAMP NOT NULL{owner_line},
    CONSTRAINT pk_{table}_id PRIMARY KEY (id),
    CONSTRAINT uq_{table}_event UNIQUE (event_id)
)
"""


def _memory_index_specs(table: str) -> "list[tuple[str, str, str]]":
    return [
        (f"idx_{table}_scope", table, "app_name, user_id, scope, timestamp"),
        (f"idx_{table}_session", table, "session_id, timestamp"),
        (f"idx_{table}_inserted", table, "inserted_at"),
    ]


def _memory_exists_sql(table: str) -> str:
    return f"SELECT id FROM {table} WHERE event_id = ? FETCH FIRST 1 ROWS ONLY"


def _memory_insert_sql(table: str, owner_column_name: "str | None") -> str:
    owner_column = f", {owner_column_name}" if owner_column_name else ""
    owner_param = ", ?" if owner_column_name else ""
    return f"""
    INSERT INTO {table} (
        id, session_id, app_name, user_id, scope, event_id, author,
        timestamp, content_json, content_text, metadata_json, inserted_at{owner_column}
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{owner_param})
    """


def _memory_insert_row(entry: StoredMemory, has_owner: bool, owner_id: object) -> "tuple[Any, ...]":
    params = _memory_insert_params(entry)
    return (*params, owner_id) if has_owner else params


def _memory_search_statement(
    table: str, query: str, app_name: str, user_id: str, scope_filter: 'Literal["all", "user", "app"]', limit: int
) -> "tuple[str, tuple[Any, ...]]":
    where_scope, scope_params = _build_db2_scope_where(app_name, user_id, scope_filter)
    sql = f"""
    SELECT id, session_id, app_name, user_id, scope, event_id, author,
           timestamp, content_json, content_text, metadata_json, inserted_at
    FROM {table}
    WHERE {where_scope}
      AND POSSTR(LOWER(content_text), LOWER(?)) > 0
    ORDER BY timestamp DESC
    FETCH FIRST {int(limit)} ROWS ONLY
    """
    return sql, (*scope_params, query)


def _memory_session_delete_statements(table: str) -> "tuple[str, str]":
    return (
        f"SELECT COUNT(*) AS row_count FROM {table} WHERE session_id = ?",
        f"DELETE FROM {table} WHERE session_id = ?",
    )


def _memory_retention_statements(
    table: str, days: int, app_name: "str | None", scope: "str | None"
) -> "tuple[str, str, tuple[Any, ...]]":
    clauses = ["inserted_at < ?"]
    params: list[Any] = [utc_now() - timedelta(days=days)]
    if app_name is not None:
        clauses.append("app_name = ?")
        params.append(app_name)
    if scope is not None:
        clauses.append("scope = ?")
        params.append(scope)
    where_sql = " AND ".join(clauses)
    return (
        f"SELECT COUNT(*) AS row_count FROM {table} WHERE {where_sql}",
        f"DELETE FROM {table} WHERE {where_sql}",
        tuple(params),
    )


def _require_memory_enabled(enabled: bool) -> None:
    if not enabled:
        msg = "Memory store is disabled"
        raise RuntimeError(msg)
