"""aiomysql ADK store for Google Agent Development Kit session/event storage."""

import asyncio
import contextlib
import re
import secrets
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.adapters.aiomysql._typing import AiomysqlCursor, AiomysqlProgrammingError, AiomysqlRawCursor
from sqlspec.adapters.aiomysql.core import format_identifier, resolve_rowcount
from sqlspec.config import ADKConfig
from sqlspec.exceptions import DeadlockError
from sqlspec.extensions.adk import BaseAsyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.converters import merge_scoped_state
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.migrations.schema import metadata_has_index
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime, timedelta

    from sqlspec.adapters.aiomysql.config import AiomysqlConfig
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory


__all__ = ("AiomysqlADKConfig", "AiomysqlADKMemoryStore", "AiomysqlADKStore")

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146
_MYSQL_RETRYABLE_ERRNOS: Final = frozenset({1205, 1213})
_MAX_STATE_MERGE_ATTEMPTS: Final = 16
_MYSQL_SESSION_IDENTITY_COLUMNS: Final = ("id", "app_name", "user_id")
_MYSQL_EVENT_IDENTITY_COLUMNS: Final = ("id", "app_name", "user_id", "session_id")
_MYSQL_APP_STATE_IDENTITY_COLUMNS: Final = ("app_name",)
_MYSQL_USER_STATE_IDENTITY_COLUMNS: Final = ("app_name", "user_id")
_MYSQL_MEMORY_IDENTITY_COLUMNS: Final = ("id", "session_id", "app_name", "user_id", "event_id")


class AiomysqlADKConfig(ADKConfig):
    """aiomysql-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the aiomysql ADK store.
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


class AiomysqlADKStore(BaseAsyncADKStore["AiomysqlConfig"]):
    """MySQL/MariaDB ADK store using aiomysql driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(self, config: "AiomysqlConfig", *, read_config: "AiomysqlConfig | None" = None) -> None:
        """Initialize aiomysql ADK store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def create_tables(self) -> None:
        """Create all ADK session tables if they don't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Reconcile legacy column collations and missing session/event secondary indexes."""
        adk_config = _adk_config(self._config)
        fk_targets = (
            (self._session_table, _MYSQL_SESSION_IDENTITY_COLUMNS),
            (self._events_table, _MYSQL_EVENT_IDENTITY_COLUMNS),
        )
        fk_alter_statements: list[str] = []
        for table_name, columns in fk_targets:
            sql, params = _mysql_columns_query(table_name)
            column_rows = await driver.select(sql, *params)
            fk_alter_statements.extend(_mysql_column_alter_statements(table_name, columns, list(column_rows)))
        if fk_alter_statements:
            await driver.execute("SET FOREIGN_KEY_CHECKS = 0")
            try:
                for statement in fk_alter_statements:
                    await driver.execute(statement)
            finally:
                await driver.execute("SET FOREIGN_KEY_CHECKS = 1")

        other_targets: tuple[tuple[str, tuple[str, ...]], ...] = (
            (self._app_state_table, _MYSQL_APP_STATE_IDENTITY_COLUMNS),
            (self._user_state_table, _MYSQL_USER_STATE_IDENTITY_COLUMNS),
        )
        for state_table_name, state_columns in other_targets:
            sql, params = _mysql_columns_query(state_table_name)
            column_rows = await driver.select(sql, *params)
            for statement in _mysql_column_alter_statements(state_table_name, state_columns, list(column_rows)):
                await driver.execute(statement)

        session_db, session_bare = _mysql_split_table(self._session_table)
        session_rows = await driver.data_dictionary.get_indexes(driver, table=session_bare, schema=session_db)
        for statement in _mysql_missing_index_statements(
            self._session_table, _mysql_session_index_specs(self._session_table), list(session_rows)
        ):
            await driver.execute(statement)

        events_db, events_bare = _mysql_split_table(self._events_table)
        event_rows = await driver.data_dictionary.get_indexes(driver, table=events_bare, schema=events_db)
        for statement in _mysql_missing_index_statements(
            self._events_table, _mysql_event_index_specs(self._events_table, adk_config), list(event_rows)
        ):
            await driver.execute(statement)

    async def create_session(
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
        """Create a new session and atomically apply/read scoped state."""
        state_json = self._json_serializer(state)
        empty_state_json = self._json_serializer({})

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {_mysql_table_ref(self._session_table)} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {_mysql_table_ref(self._session_table)} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, state_json)

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_mysql_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                async with (
                    self._config.provide_connection() as conn,
                    AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
                ):
                    try:
                        await cursor.execute("BEGIN")
                        await cursor.execute(sql, params)
                        await cursor.execute(select_sql, (app_name, user_id, session_id))
                        row = await cursor.fetchone()
                        row = _require_created_session_row(row)

                        if app_state_delta:
                            await cursor.execute(
                                _mysql_insert_ignore_app_state_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            await cursor.execute(
                                _mysql_select_app_state_for_update_sql(self._app_state_table), (app_name,)
                            )
                            app_row = await cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), self._json_deserializer) or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            await cursor.execute(
                                _mysql_update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        else:
                            await cursor.execute(_mysql_select_app_state_sql(self._app_state_table), (app_name,))
                            app_row = await cursor.fetchone()
                            merged_app_state = (
                                _json_dict(_row_first_value(app_row, "state"), self._json_deserializer)
                                if app_row is not None
                                else None
                            )

                        if user_state_delta:
                            await cursor.execute(
                                _mysql_insert_ignore_user_state_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            await cursor.execute(
                                _mysql_select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = await cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), self._json_deserializer) or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            await cursor.execute(
                                _mysql_update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        else:
                            await cursor.execute(
                                _mysql_select_user_state_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = await cursor.fetchone()
                            merged_user_state = (
                                _json_dict(_row_first_value(user_row, "state"), self._json_deserializer)
                                if user_row is not None
                                else None
                            )

                        await conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            await conn.rollback()
                        raise

            except Exception as exc:
                if not _is_retryable_mysql_error(exc) or attempt + 1 >= max_attempts:
                    raise
                await asyncio.sleep(_retry_delay_seconds(attempt))
            else:
                session = _session_record_from_row(row, self._json_deserializer)
                session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
                return session
        msg = "Failed to merge scoped state during create_session after retries."
        raise DeadlockError(msg)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Get session by scoped identifiers."""
        try:
            async with (
                self._provide_read_connection(primary=renew_for is not None) as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                if self._should_renew(renew_for):
                    await cursor.execute(
                        f"""
                        UPDATE {_mysql_table_ref(self._session_table)}
                        SET update_time = UTC_TIMESTAMP(6)
                        WHERE app_name = %s AND user_id = %s AND id = %s
                        """,
                        (app_name, user_id, session_id),
                    )
                    await conn.commit()

                await cursor.execute(
                    f"""
                    SELECT id, app_name, user_id, state, create_time, update_time
                    FROM {_mysql_table_ref(self._session_table)}
                    WHERE app_name = %s AND user_id = %s AND id = %s
                    """,
                    (app_name, user_id, session_id),
                )
                row = await cursor.fetchone()
                return _session_record_from_row(row, self._json_deserializer) if row is not None else None
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        sql = f"""
        UPDATE {_mysql_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (self._json_serializer(state), app_name, user_id, session_id))
            await conn.commit()

    async def list_sessions(
        self,
        app_name: str,
        user_id: "str | None" = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: "int | None" = None,
        offset: "int | None" = None,
    ) -> "list[StoredSession]":
        """List sessions for an app, optionally filtered by user."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )

        try:
            async with (
                self._provide_read_connection(primary=False) as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()
                records = [_session_record_from_row(row, self._json_deserializer) for row in rows]
                return [
                    record
                    for record in records
                    if record["app_name"] == app_name and (user_id is None or record["user_id"] == user_id)
                ]
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events."""
        sql = f"DELETE FROM {_mysql_table_ref(self._session_table)} WHERE app_name = %s AND user_id = %s AND id = %s"

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(sql, (app_name, user_id, session_id))
            await conn.commit()

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = f"""
        INSERT INTO {_mysql_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        )
        SELECT %s, %s, %s, %s, %s, %s, %s
        FROM {_mysql_table_ref(self._session_table)}
        WHERE id = %s AND app_name = %s AND user_id = %s
        """

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(
                sql, (*_event_insert_params(event_record, self._json_serializer), session_id, app_name, user_id)
            )
            if resolve_rowcount(cursor) == 0:
                if hasattr(conn, "rollback"):
                    await conn.rollback()
                msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
                raise ValueError(msg)
            await conn.commit()

    async def append_event_and_update_state(
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

        empty_state_json = self._json_serializer({})
        insert_sql = f"""
        INSERT INTO {_mysql_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        update_sql = f"""
        UPDATE {_mysql_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_mysql_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                async with (
                    self._config.provide_connection() as conn,
                    AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
                ):
                    try:
                        await cursor.execute("BEGIN")
                        await cursor.execute(update_sql, (self._json_serializer(state), app_name, user_id, session_id))
                        await cursor.execute(select_sql, (app_name, user_id, session_id))
                        row = await cursor.fetchone()
                        if row is None:
                            _raise_session_not_found(session_id)
                        await cursor.execute(
                            insert_sql,
                            (
                                event_record["id"],
                                app_name,
                                user_id,
                                session_id,
                                event_record["invocation_id"],
                                event_record["timestamp"],
                                _json_for_storage(event_record["event_data"], self._json_serializer),
                            ),
                        )
                        if app_state_delta:
                            await cursor.execute(
                                _mysql_insert_ignore_app_state_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            await cursor.execute(
                                _mysql_select_app_state_for_update_sql(self._app_state_table), (app_name,)
                            )
                            app_row = await cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), self._json_deserializer) or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            await cursor.execute(
                                _mysql_update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        if user_state_delta:
                            await cursor.execute(
                                _mysql_insert_ignore_user_state_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            await cursor.execute(
                                _mysql_select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = await cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), self._json_deserializer) or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            await cursor.execute(
                                _mysql_update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        await conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            await conn.rollback()
                        raise

            except Exception as exc:
                if not _is_retryable_mysql_error(exc) or attempt + 1 >= max_attempts:
                    raise
                await asyncio.sleep(_retry_delay_seconds(attempt))
            else:
                session = _session_record_from_row(row, self._json_deserializer)
                if merged_app_state is not None or merged_user_state is not None:
                    session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
                return session
        msg = "Failed to merge scoped state during append_event_and_update_state after retries."
        raise DeadlockError(msg)

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        """Get events for a session."""
        if limit == 0:
            return []

        where_clauses = ["s.app_name = %s", "s.user_id = %s", "s.id = %s"]
        params: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            where_clauses.append("e.timestamp > %s")
            params.append(after_timestamp)
        order_direction = "DESC" if limit is not None else "ASC"
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT %s"
            params.append(limit)

        sql = f"""
        SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data
        FROM {_mysql_table_ref(self._events_table)} e
        JOIN {_mysql_table_ref(self._session_table)} s
            ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.timestamp {order_direction}{limit_clause}
        """

        try:
            async with (
                self._provide_read_connection(primary=False) as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()
                if limit is not None:
                    rows = list(reversed(rows))
                return [_event_record_from_row(row, self._json_deserializer) for row in rows]
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: "datetime", app_name: "str | None" = None) -> int:
        """Delete events older than the given timestamp."""
        sql = f"DELETE FROM {_mysql_table_ref(self._events_table)} WHERE timestamp < %s"
        params: list[Any] = [before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, tuple(params))
                await conn.commit()
                return resolve_rowcount(cursor)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_idle_sessions(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete sessions whose update_time predates the threshold."""
        sql = f"DELETE FROM {_mysql_table_ref(self._session_table)} WHERE update_time < %s"
        params: list[Any] = [updated_before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, tuple(params))
                await conn.commit()
                return resolve_rowcount(cursor)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_idle_user_states(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete user state rows whose update_time predates the threshold."""
        sql = f"DELETE FROM {_mysql_table_ref(self._user_state_table)} WHERE update_time < %s"
        params: list[Any] = [updated_before]
        if app_name is not None:
            sql += " AND app_name = %s"
            params.append(app_name)

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, tuple(params))
                await conn.commit()
                return resolve_rowcount(cursor)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return 0
            raise

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        sql = f"SELECT state FROM {_mysql_table_ref(self._app_state_table)} WHERE app_name = %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (app_name,))
                row = await cursor.fetchone()
                return _json_dict(_row_first_value(row, "state"), self._json_deserializer) if row is not None else None
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return None
            raise

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        sql = (
            f"SELECT app_name, user_id, state FROM {_mysql_table_ref(self._user_state_table)} "
            "WHERE app_name = %s AND user_id = %s"
        )

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (app_name, user_id))
                row = await cursor.fetchone()
                return _user_state_from_row(row, app_name, user_id, deserializer=self._json_deserializer)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return None
            raise

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(
                _mysql_upsert_app_state_sql(self._app_state_table), (app_name, self._json_serializer(state))
            )
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(
                _mysql_upsert_user_state_sql(self._user_state_table), (app_name, user_id, self._json_serializer(state))
            )
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        sql = f"SELECT value FROM {_mysql_table_ref(self._metadata_table)} WHERE `key` = %s"

        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, (key,))
                row = await cursor.fetchone()
                return str(_row_first_value(row, "value")) if row is not None else None
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.execute(_mysql_upsert_metadata_sql(self._metadata_table), (key, value))
            await conn.commit()

    async def _sessions_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for sessions."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "session_table_options")
        return _mysql_sessions_ddl(self._session_table, self._owner_id_column_ddl, table_options)

    async def _events_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for events."""
        return _mysql_events_ddl(self._events_table, self._session_table, _adk_config(self._config))

    async def _app_states_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for app-scoped state."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "app_state_table_options")
        return _mysql_app_state_ddl(self._app_state_table, table_options)

    async def _user_states_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for user-scoped state."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "user_state_table_options")
        return _mysql_user_state_ddl(self._user_state_table, table_options)

    async def _metadata_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for ADK metadata."""
        return _mysql_metadata_ddl(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for app-scoped state."""
        return f"DROP TABLE IF EXISTS {_mysql_table_ref(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for user-scoped state."""
        return f"DROP TABLE IF EXISTS {_mysql_table_ref(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        """Get MySQL DROP TABLE SQL for ADK metadata."""
        return f"DROP TABLE IF EXISTS {_mysql_table_ref(self._metadata_table)}"

    def _drop_tables_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements."""
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {_mysql_table_ref(self._events_table)}",
            f"DROP TABLE IF EXISTS {_mysql_table_ref(self._session_table)}",
        ]


class AiomysqlADKMemoryStore(BaseAsyncADKMemoryStore["AiomysqlConfig"]):
    """MySQL/MariaDB ADK memory store using aiomysql driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(self, config: "AiomysqlConfig", *, read_config: "AiomysqlConfig | None" = None) -> None:
        """Initialize aiomysql memory store."""
        super().__init__(config, read_config=read_config)
        self._json_serializer, self._json_deserializer = _resolve_json_codecs(config)

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Reconcile legacy memory identity collations and missing ordinary/FULLTEXT indexes."""
        if not self._enabled:
            return
        sql, params = _mysql_columns_query(self._memory_table)
        column_rows = await driver.select(sql, *params)
        for statement in _mysql_column_alter_statements(
            self._memory_table, _MYSQL_MEMORY_IDENTITY_COLUMNS, list(column_rows)
        ):
            await driver.execute(statement)

        memory_db, memory_bare = _mysql_split_table(self._memory_table)
        memory_rows = await driver.data_dictionary.get_indexes(driver, table=memory_bare, schema=memory_db)
        for statement in _mysql_missing_index_statements(
            self._memory_table, _mysql_memory_index_specs(self._memory_table, use_fts=self._use_fts), list(memory_rows)
        ):
            await driver.execute(statement)

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication on (app_name, user_id, event_id)."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            sql = f"""
            INSERT IGNORE INTO {_mysql_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {_mysql_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        params_list: list[tuple[Any, ...]] = []
        for entry in entries:
            content_json = self._json_serializer(entry["content_json"])
            meta = entry.get("metadata_json")
            metadata_json = self._json_serializer(meta) if meta is not None else None
            if self._owner_id_column_name:
                params_list.append((
                    entry["id"],
                    entry["session_id"],
                    entry["app_name"],
                    entry["user_id"],
                    entry.get("scope", "user"),
                    entry["event_id"],
                    entry.get("author"),
                    owner_id,
                    entry["timestamp"],
                    content_json,
                    entry["content_text"],
                    metadata_json,
                    entry["inserted_at"],
                ))
            else:
                params_list.append((
                    entry["id"],
                    entry["session_id"],
                    entry["app_name"],
                    entry["user_id"],
                    entry.get("scope", "user"),
                    entry["event_id"],
                    entry.get("author"),
                    entry["timestamp"],
                    content_json,
                    entry["content_text"],
                    metadata_json,
                    entry["inserted_at"],
                ))

        if not params_list:
            return 0

        async with (
            self._config.provide_connection() as conn,
            AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
        ):
            await cursor.executemany(sql, params_list)
            inserted_count = resolve_rowcount(cursor)
            await conn.commit()
        return inserted_count

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        where_scope, scope_params = _build_mysql_scope_where(app_name, user_id, scope_filter)
        if self._use_fts:
            sql = f"""
            SELECT * FROM {_mysql_table_ref(self._memory_table)}
            WHERE {where_scope}
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {_mysql_table_ref(self._memory_table)}
            WHERE {where_scope} AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, f"%{query}%", limit_value)

        try:
            async with (
                self._provide_read_connection(primary=False) as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()
                columns = [col[0] for col in cursor.description or []]
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return []
            raise

        records: list[StoredMemory] = []
        for row in rows:
            row_dict = dict(row) if isinstance(row, Mapping) else dict(zip(columns, row, strict=False))
            rec = cast("StoredMemory", row_dict)
            if not _memory_record_matches_scope(rec, app_name, user_id, scope_filter):
                continue
            rec["content_json"] = _json_dict(rec.get("content_json"), self._json_deserializer)
            metadata_val = rec.get("metadata_json")
            rec["metadata_json"] = (
                _json_dict(metadata_val, self._json_deserializer) if metadata_val is not None else None
            )
            rec["embedding"] = None
            records.append(rec)
        return records

    async def delete_entries_by_session(
        self, session_id: str, app_name: "str | None" = None, user_id: "str | None" = None
    ) -> int:
        """Delete memory entries for a specific session, optionally scoped by app and user."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses = ["session_id = %s"]
        params: list[Any] = [session_id]
        if app_name is not None:
            clauses.append("app_name = %s")
            params.append(app_name)
        if user_id is not None:
            clauses.append("user_id = %s")
            params.append(user_id)
        sql = f"DELETE FROM {_mysql_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, tuple(params))
                await conn.commit()
                return resolve_rowcount(cursor)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses = ["inserted_at < (UTC_TIMESTAMP(6) - INTERVAL %s DAY)"]
        params: list[Any] = [days]
        if app_name is not None:
            clauses.append("app_name = %s")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = %s")
            params.append(scope)

        where_sql = " AND ".join(clauses)
        sql = f"DELETE FROM {_mysql_table_ref(self._memory_table)} WHERE {where_sql}"
        try:
            async with (
                self._config.provide_connection() as conn,
                AiomysqlCursor(conn, cursor_class=AiomysqlRawCursor) as cursor,
            ):
                await cursor.execute(sql, tuple(params))
                await conn.commit()
                return resolve_rowcount(cursor)
        except AiomysqlProgrammingError as exc:
            if _is_mysql_table_missing(exc):
                return 0
            raise

    async def _memory_table_ddl(self) -> str:
        """Get MySQL CREATE TABLE SQL for memory entries."""
        adk_config = _adk_config(self._config)
        table_options = _mysql_table_options(adk_config, "memory_table_options")
        owner_id_line = ""
        fk_constraint = ""
        if self._owner_id_column_ddl:
            col_def, fk_def = _mysql_owner_id_column_parts(self._owner_id_column_ddl)
            owner_id_line = f",\n            {col_def}"
            if fk_def:
                fk_constraint = f",\n            {fk_def}"

        unique_key_name = _mysql_index_name(self._memory_table, "app_user_event")
        indexes_sql = ",\n            ".join(
            f"{keyword} {index_name} ({columns})"
            for keyword, index_name, columns in _mysql_memory_index_specs(self._memory_table, use_fts=self._use_fts)
        )

        return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(self._memory_table)} (
            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,
            session_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            scope VARCHAR(16) NOT NULL DEFAULT 'user',
            event_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            UNIQUE KEY {unique_key_name} (app_name, user_id, event_id),
            {indexes_sql}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get MySQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {_mysql_table_ref(self._memory_table)}"]


def _mysql_split_table(table: str) -> tuple[str | None, str]:
    """Split a validated 1- or 2-part MySQL table name into (database, bare_table)."""
    if "." in table:
        database_name, bare_table = table.split(".", 1)
        return database_name, bare_table
    return None, table


def _mysql_table_ref(table: str) -> str:
    """Render database.table via format_identifier while keeping bare names unquoted."""
    if "." in table:
        return format_identifier(table)
    return table


def _mysql_index_name(table: str, suffix: str) -> str:
    """Derive a per-table MySQL index identifier from the bare table name."""
    _, bare_table = _mysql_split_table(table)
    return f"idx_{bare_table}_{suffix}"


def _resolve_json_codecs(config: Any) -> "tuple[Callable[[Any], str], Callable[[Any], Any]]":
    driver_features = getattr(config, "driver_features", None)
    if isinstance(driver_features, Mapping):
        serializer = driver_features.get("json_serializer", to_json)
        deserializer = driver_features.get("json_deserializer", from_json)
        return cast(
            "tuple[Callable[[Any], str], Callable[[Any], Any]]",
            (serializer if callable(serializer) else to_json, deserializer if callable(deserializer) else from_json),
        )
    return to_json, from_json


def _row_first_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    return row[0]


def _mysql_owner_id_column_parts(column_ddl: str) -> "tuple[str, str]":
    """Parse owner ID column DDL for MySQL FOREIGN KEY syntax.

    Args:
        column_ddl: Column DDL like "tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE".

    Returns:
        Tuple of (column_definition, foreign_key_constraint).
    """
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


def _adk_config(config: Any) -> AiomysqlADKConfig:
    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("AiomysqlADKConfig", adk_config)


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


def _is_retryable_mysql_error(exc: BaseException) -> bool:
    if isinstance(exc, DeadlockError):
        return True
    args = getattr(exc, "args", ())
    errno = getattr(exc, "errno", None)
    sqlstate = getattr(exc, "sqlstate", None)
    return errno in _MYSQL_RETRYABLE_ERRNOS or sqlstate == "40001" or bool(args and args[0] in _MYSQL_RETRYABLE_ERRNOS)


def _retry_delay_seconds(attempt: int) -> float:
    base = min(0.05, 0.002 * (attempt + 1))
    jitter = secrets.randbelow(80) / 10_000.0
    return base + jitter


def _json_for_storage(value: Any, serializer: "Callable[[Any], str]" = to_json) -> str:
    return value if isinstance(value, str) else serializer(value)


def _json_dict(value: Any, deserializer: "Callable[[Any], Any]" = from_json) -> "dict[str, Any]":
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        return cast("dict[str, Any]", deserializer(value))
    return cast("dict[str, Any]", value)


def _session_record_from_row(row: Any, deserializer: "Callable[[Any], Any]" = from_json) -> StoredSession:
    if isinstance(row, Mapping):
        return StoredSession(
            id=row["id"],
            app_name=row["app_name"],
            user_id=row["user_id"],
            state=_json_dict(row["state"], deserializer),
            create_time=row["create_time"],
            update_time=row["update_time"],
        )
    return StoredSession(
        id=row[0],
        app_name=row[1],
        user_id=row[2],
        state=_json_dict(row[3], deserializer),
        create_time=row[4],
        update_time=row[5],
    )


def _event_record_from_row(row: Any, deserializer: "Callable[[Any], Any]" = from_json) -> StoredEvent:
    if isinstance(row, Mapping):
        return StoredEvent(
            id=row["id"],
            app_name=row["app_name"],
            user_id=row["user_id"],
            session_id=row["session_id"],
            invocation_id=row["invocation_id"],
            timestamp=row["timestamp"],
            event_data=_json_dict(row["event_data"], deserializer),
        )
    return StoredEvent(
        id=row[0],
        app_name=row[1],
        user_id=row[2],
        session_id=row[3],
        invocation_id=row[4],
        timestamp=row[5],
        event_data=_json_dict(row[6], deserializer),
    )


def _event_insert_params(event_record: StoredEvent, serializer: "Callable[[Any], str]" = to_json) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        event_record["timestamp"],
        _json_for_storage(event_record["event_data"], serializer),
    )


def _mysql_session_index_specs(session_table: str) -> list[tuple[str, str, str]]:
    """Build (keyword, index_name, columns) specs for the MySQL ADK session table."""
    return [
        ("INDEX", _mysql_index_name(session_table, "app_user"), "app_name, user_id"),
        ("INDEX", _mysql_index_name(session_table, "update_time"), "update_time DESC"),
    ]


def _mysql_event_index_specs(
    events_table: str, adk_config: Mapping[str, Any] | None = None
) -> list[tuple[str, str, str]]:
    """Build (keyword, index_name, columns) specs for the MySQL ADK events table."""
    adk_config = adk_config or {}
    covering_column = ", invocation_id" if adk_config.get("enable_covering_indexes", False) else ""
    specs = [
        (
            "INDEX",
            _mysql_index_name(events_table, "scope"),
            f"app_name, user_id, session_id, timestamp ASC{covering_column}",
        ),
        ("INDEX", _mysql_index_name(events_table, "session"), f"session_id, timestamp ASC{covering_column}"),
        ("INDEX", _mysql_index_name(events_table, "app_timestamp"), "app_name, timestamp ASC"),
    ]
    if adk_config.get("enable_event_generated_columns", False):
        specs.extend((
            ("INDEX", _mysql_index_name(events_table, "author_gc"), "session_id, author_gc, timestamp ASC"),
            ("INDEX", _mysql_index_name(events_table, "node_path_gc"), "session_id, node_path_gc, timestamp ASC"),
        ))
    return specs


def _mysql_memory_index_specs(memory_table: str, *, use_fts: bool) -> list[tuple[str, str, str]]:
    """Build (keyword, index_name, columns) specs for the MySQL ADK memory table."""
    specs = [
        ("INDEX", _mysql_index_name(memory_table, "app_scope_user_time"), "app_name, scope, user_id, timestamp"),
        ("INDEX", _mysql_index_name(memory_table, "scope"), "app_name, scope"),
        ("INDEX", _mysql_index_name(memory_table, "session"), "session_id"),
    ]
    if use_fts:
        specs.append(("FULLTEXT INDEX", _mysql_index_name(memory_table, "fts"), "content_text"))
    return specs


def _mysql_missing_index_statements(table: str, specs: list[tuple[str, str, str]], rows: list[Any]) -> list[str]:
    """Build CREATE INDEX statements for specs absent from catalog index metadata."""
    database_name, bare_table = _mysql_split_table(table)
    table_ref = _mysql_table_ref(table)
    return [
        f"CREATE {keyword} {index_name} ON {table_ref} ({columns})"
        for keyword, index_name, columns in specs
        if not metadata_has_index(rows, bare_table, index_name, schema_name=database_name)
    ]


def _mysql_sessions_ddl(session_table: str, owner_id_column_ddl: "str | None", table_options: str = "") -> str:
    owner_id_line = ""
    fk_constraint = ""
    if owner_id_column_ddl:
        col_def, fk_def = _mysql_owner_id_column_parts(owner_id_column_ddl)
        owner_id_line = f"\n            {col_def},"
        if fk_def:
            fk_constraint = f",\n            {fk_def}"

    indexes_sql = ",\n            ".join(
        f"{keyword} {index_name} ({columns})"
        for keyword, index_name, columns in _mysql_session_index_specs(session_table)
    )

    return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(session_table)} (
            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,
            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,{owner_id_line}
            state JSON NOT NULL,
            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            {indexes_sql}{fk_constraint}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_events_ddl(events_table: str, session_table: str, adk_config: Mapping[str, Any] | None = None) -> str:
    adk_config = adk_config or {}
    generated_columns = ""
    if adk_config.get("enable_event_generated_columns", False):
        generated_columns = """,
            author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED,
            node_path_gc VARCHAR(512) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED"""

    indexes_sql = ",\n            ".join(
        f"{keyword} {index_name} ({columns})"
        for keyword, index_name, columns in _mysql_event_index_specs(events_table, adk_config)
    )
    table_options = _mysql_table_options(adk_config, "events_table_options")

    return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(events_table)} (
            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,
            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            session_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            event_data JSON NOT NULL{generated_columns},
            FOREIGN KEY (session_id) REFERENCES {_mysql_table_ref(session_table)}(id) ON DELETE CASCADE,
            {indexes_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_app_state_ddl(app_state_table: str, table_options: str = "") -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(app_state_table)} (
            app_name VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_user_state_ddl(user_state_table: str, table_options: str = "") -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(user_state_table)} (
            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
            state JSON NOT NULL,
            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            PRIMARY KEY (app_name, user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{table_options}
        """


def _mysql_metadata_ddl(metadata_table: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_mysql_table_ref(metadata_table)} (
            `key` VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """


def _mysql_upsert_app_state_sql(app_state_table: str) -> str:
    return f"""
        INSERT INTO {_mysql_table_ref(app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _mysql_upsert_user_state_sql(user_state_table: str) -> str:
    return f"""
        INSERT INTO {_mysql_table_ref(user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _mysql_upsert_metadata_sql(metadata_table: str) -> str:
    return f"""
        INSERT INTO {_mysql_table_ref(metadata_table)} (`key`, value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """


def _mysql_insert_ignore_app_state_sql(app_state_table: str) -> str:
    return f"""
        INSERT IGNORE INTO {_mysql_table_ref(app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        """


def _mysql_select_app_state_sql(app_state_table: str) -> str:
    return f"SELECT state FROM {_mysql_table_ref(app_state_table)} WHERE app_name = %s"


def _mysql_select_app_state_for_update_sql(app_state_table: str) -> str:
    return f"SELECT state FROM {_mysql_table_ref(app_state_table)} WHERE app_name = %s FOR UPDATE"


def _mysql_update_app_state_sql(app_state_table: str) -> str:
    return f"""
        UPDATE {_mysql_table_ref(app_state_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s
        """


def _mysql_insert_ignore_user_state_sql(user_state_table: str) -> str:
    return f"""
        INSERT IGNORE INTO {_mysql_table_ref(user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        """


def _mysql_select_user_state_sql(user_state_table: str) -> str:
    return f"SELECT state FROM {_mysql_table_ref(user_state_table)} WHERE app_name = %s AND user_id = %s"


def _mysql_select_user_state_for_update_sql(user_state_table: str) -> str:
    return f"SELECT state FROM {_mysql_table_ref(user_state_table)} WHERE app_name = %s AND user_id = %s FOR UPDATE"


def _mysql_update_user_state_sql(user_state_table: str) -> str:
    return f"""
        UPDATE {_mysql_table_ref(user_state_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s
        """


def _mysql_columns_query(table: str) -> "tuple[str, tuple[Any, ...]]":
    database_name, bare_table = _mysql_split_table(table)
    if database_name is None:
        return (
            """
            SELECT COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type, COLLATION_NAME AS collation_name
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
            """,
            (bare_table,),
        )
    return (
        """
        SELECT COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type, COLLATION_NAME AS collation_name
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (database_name, bare_table),
    )


def _mysql_row_field(row: Mapping[str, Any], name: str) -> Any:
    if name in row:
        return row[name]
    upper_name = name.upper()
    if upper_name in row:
        return row[upper_name]
    lower_name = name.lower()
    for key, value in row.items():
        if str(key).lower() == lower_name:
            return value
    return None


def _mysql_column_alter_statements(table: str, target_columns: "tuple[str, ...]", rows: "list[Any]") -> "list[str]":
    by_name: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_name = _mysql_row_field(row, "column_name")
        if isinstance(raw_name, str):
            by_name[raw_name.lower()] = row

    table_ref = _mysql_table_ref(table)
    statements: list[str] = []
    for col_name in target_columns:
        row = by_name.get(col_name.lower())
        if row is None:
            continue
        collation = _mysql_row_field(row, "collation_name")
        if isinstance(collation, str) and collation.lower() == "utf8mb4_bin":
            continue
        col_type = _mysql_row_field(row, "column_type")
        resolved_type = str(col_type).strip() if isinstance(col_type, str) and col_type.strip() else "VARCHAR(128)"
        statements.append(
            f"ALTER TABLE {table_ref} MODIFY COLUMN {col_name} {resolved_type} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"
        )
    return statements


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _require_created_session_row(row: Any) -> Any:
    if row is None:
        msg = "Failed to fetch created session"
        raise RuntimeError(msg)
    return row


def _build_mysql_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = %s AND ((scope = 'user' AND user_id = %s) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = %s AND scope = 'user' AND user_id = %s", (app_name, user_id)
    return "app_name = %s AND scope = 'app'", (app_name,)


def _user_state_from_row(
    row: Any, app_name: str, user_id: str, deserializer: "Callable[[Any], Any]" = from_json
) -> "dict[str, Any] | None":
    if row is None:
        return None
    if isinstance(row, Mapping):
        if row.get("app_name") != app_name or row.get("user_id") != user_id:
            return None
        return _json_dict(row["state"], deserializer=deserializer)
    if row[0] != app_name or row[1] != user_id:
        return None
    return _json_dict(row[2], deserializer=deserializer)


def _memory_record_matches_scope(
    rec: Mapping[str, Any], app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> bool:
    if rec.get("app_name") != app_name:
        return False
    scope = rec.get("scope")
    if scope_filter == "app":
        return scope == "app"
    if scope_filter == "user":
        return scope == "user" and rec.get("user_id") == user_id
    return scope == "app" or (scope == "user" and rec.get("user_id") == user_id)


def _session_list_query(
    session_table: str,
    app_name: str,
    user_id: "str | None",
    column: str,
    direction: str,
    limit: "int | None",
    offset: int,
) -> "tuple[str, tuple[Any, ...]]":
    """Return the bounded session-list query and its bound values."""
    params: list[Any] = [app_name]
    where_clause = "app_name = %s"
    if user_id is not None:
        params.append(user_id)
        where_clause = f"{where_clause} AND user_id = %s"

    page_clause = ""
    if limit is not None:
        params.extend((limit, offset))
        page_clause = "\n            LIMIT %s OFFSET %s"

    sql = f"""
    SELECT id, app_name, user_id, state, create_time, update_time
    FROM {_mysql_table_ref(session_table)}
    WHERE {where_clause}
    ORDER BY {column} {direction}, id {direction}{page_clause}
    """
    return sql, tuple(params)
