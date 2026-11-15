"""MysqlConnector ADK store for Google Agent Development Kit session/event storage."""

import asyncio
import contextlib
import re
import secrets
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, cast

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.core import format_identifier
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ConnectionTimeoutError, DeadlockError
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.extensions.adk.converters import merge_scoped_state
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.migrations.schema import metadata_has_index
from sqlspec.protocols import HasErrnoProtocol
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime, timedelta

    from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory


__all__ = (
    "MysqlConnectorADKConfig",
    "MysqlConnectorAsyncADKMemoryStore",
    "MysqlConnectorAsyncADKStore",
    "MysqlConnectorSyncADKMemoryStore",
    "MysqlConnectorSyncADKStore",
)

MYSQL_TABLE_NOT_FOUND_ERROR: Final = 1146
_MYSQL_RETRYABLE_ERRNOS: Final = frozenset({1205, 1213})
_MAX_STATE_MERGE_ATTEMPTS: Final = 16
_MYSQL_SESSION_IDENTITY_COLUMNS: Final = ("id", "app_name", "user_id")
_MYSQL_EVENT_IDENTITY_COLUMNS: Final = ("id", "app_name", "user_id", "session_id")
_MYSQL_APP_STATE_IDENTITY_COLUMNS: Final = ("app_name",)
_MYSQL_USER_STATE_IDENTITY_COLUMNS: Final = ("app_name", "user_id")
_MYSQL_MEMORY_IDENTITY_COLUMNS: Final = ("id", "session_id", "app_name", "user_id", "event_id")

_ADK_MEMORY_TABLE_DDL_TEMPLATE = ",\n            {0}"

_ADK_MEMORY_TABLE_DDL_TEMPLATE_3 = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,\n"
    "            session_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
    "            event_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            author VARCHAR(256){1},\n"
    "            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
    "            content_json JSON NOT NULL,\n"
    "            content_text TEXT NOT NULL,\n"
    "            metadata_json JSON,\n"
    "            inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
    "            {2}{3}\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{4}\n"
    "        "
)

_ADK_MYSQL_SESSIONS_DDL_TEMPLATE = "\n            {0},"

_ADK_MYSQL_SESSIONS_DDL_TEMPLATE_2 = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,\n"
    "            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,{1}\n"
    "            state JSON NOT NULL,\n"
    "            create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
    "            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
    "            {2}{3}\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{4}\n"
    "        "
)

_ADK_MYSQL_EVENTS_DDL_TEMPLATE_2 = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,\n"
    "            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            session_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            invocation_id VARCHAR(256) NOT NULL,\n"
    "            timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n"
    "            event_data JSON NOT NULL{1},\n"
    "            FOREIGN KEY (session_id) REFERENCES {2}(id) ON DELETE CASCADE,\n"
    "            {3}\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{4}\n"
    "        "
)

_ADK_MYSQL_APP_STATE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            app_name VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,\n"
    "            state JSON NOT NULL,\n"
    "            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{1}\n"
    "        "
)

_ADK_MYSQL_USER_STATE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            app_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            user_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,\n"
    "            state JSON NOT NULL,\n"
    "            update_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
    "            PRIMARY KEY (app_name, user_id)\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{1}\n"
    "        "
)

_ADK_MYSQL_METADATA_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            `key` VARCHAR(128) PRIMARY KEY,\n"
    "            value VARCHAR(512) NOT NULL\n"
    "        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci\n"
    "        "
)


class MysqlConnectorADKConfig(ADKConfig):
    """mysql-connector-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with mysql-connector ADK stores.
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


class MysqlConnectorAsyncADKStore(BaseAsyncADKStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB ADK store using mysql-connector async driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(
        self, config: "MysqlConnectorAsyncConfig", *, read_config: "MysqlConnectorAsyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)
        driver_features = getattr(config, "driver_features", None)
        features = driver_features if isinstance(driver_features, Mapping) else {}
        serializer = features.get("json_serializer")
        deserializer = features.get("json_deserializer")
        self._json_serializer: Callable[[Any], str] = (
            cast("Callable[[Any], str]", serializer) if callable(serializer) else to_json
        )
        self._json_deserializer: Callable[[Any], Any] = (
            cast("Callable[[Any], Any]", deserializer) if callable(deserializer) else from_json
        )

    async def create_tables(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())
        await self.reconcile_schema(assume_existing=True)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Reconcile legacy column collations and missing session/event secondary indexes."""
        adk_config = _adk_config(self._config)
        fk_targets = (
            (self._session_table, _MYSQL_SESSION_IDENTITY_COLUMNS),
            (self._events_table, _MYSQL_EVENT_IDENTITY_COLUMNS),
        )
        fk_alter_statements: list[str] = []
        for table_name, columns in fk_targets:
            sql, params = _columns_query(table_name)
            column_rows = await driver.select(sql, *params)
            fk_alter_statements.extend(_column_alter_statements(table_name, columns, list(column_rows)))
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
            sql, params = _columns_query(state_table_name)
            column_rows = await driver.select(sql, *params)
            for statement in _column_alter_statements(state_table_name, state_columns, list(column_rows)):
                await driver.execute(statement)

        session_db, session_bare = _split_table(self._session_table)
        session_rows = await driver.data_dictionary.get_indexes(driver, table=session_bare, schema=session_db)
        for statement in _missing_index_statements(
            self._session_table, _session_index_specs(self._session_table), list(session_rows)
        ):
            await driver.execute(statement)

        events_db, events_bare = _split_table(self._events_table)
        event_rows = await driver.data_dictionary.get_indexes(driver, table=events_bare, schema=events_db)
        for statement in _missing_index_statements(
            self._events_table, _event_index_specs(self._events_table, adk_config), list(event_rows)
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
        params: tuple[Any, ...]
        state_json = self._json_serializer(state)
        empty_state_json = self._json_serializer({})
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {_table_ref(self._session_table)} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {_table_ref(self._session_table)} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, state_json)

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                async with self._config.provide_connection() as conn:
                    cursor = await conn.cursor()
                    try:
                        await cursor.execute("BEGIN")
                        await cursor.execute(sql, params)
                        await cursor.execute(select_sql, (app_name, user_id, session_id))
                        row = await cursor.fetchone()
                        row = _require_created_session_row(row)

                        if app_state_delta:
                            await cursor.execute(
                                _ensure_app_state_row_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            await cursor.execute(_select_app_state_for_update_sql(self._app_state_table), (app_name,))
                            app_row = await cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            await cursor.execute(
                                _update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        else:
                            await cursor.execute(_select_app_state_sql(self._app_state_table), (app_name,))
                            app_row = await cursor.fetchone()
                            merged_app_state = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                if app_row is not None
                                else None
                            )

                        if user_state_delta:
                            await cursor.execute(
                                _ensure_user_state_row_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            await cursor.execute(
                                _select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = await cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            await cursor.execute(
                                _update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        else:
                            await cursor.execute(_select_user_state_sql(self._user_state_table), (app_name, user_id))
                            user_row = await cursor.fetchone()
                            merged_user_state = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                if user_row is not None
                                else None
                            )

                        await conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            await conn.rollback()
                        raise
                    finally:
                        await cursor.close()

            except Exception as exc:
                if not _is_retryable_error(exc) or attempt + 1 >= max_attempts:
                    raise
                await asyncio.sleep(_calculate_backoff_seconds(attempt))
            else:
                session = _session_record_from_row(row, deserializer=self._json_deserializer)
                session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
                return session
        msg = "Failed to merge scoped state during create_session after retries."
        raise DeadlockError(msg)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":

        try:
            async with self._provide_read_connection(primary=renew_for is not None) as conn:
                cursor = await conn.cursor()
                try:
                    if self._should_renew(renew_for):
                        await cursor.execute(
                            f"""
                            UPDATE {_table_ref(self._session_table)}
                            SET update_time = UTC_TIMESTAMP(6)
                            WHERE app_name = %s AND user_id = %s AND id = %s
                            """,
                            (app_name, user_id, session_id),
                        )
                        await conn.commit()

                    await cursor.execute(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {_table_ref(self._session_table)}
                        WHERE app_name = %s AND user_id = %s AND id = %s
                        """,
                        (app_name, user_id, session_id),
                    )
                    row = await cursor.fetchone()
                finally:
                    await cursor.close()

            return _session_record_from_row(row, deserializer=self._json_deserializer) if row is not None else None
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (self._json_serializer(state), app_name, user_id, session_id))
            finally:
                await cursor.close()
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
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )

        try:
            async with self._provide_read_connection(primary=False) as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, params)
                    rows = await cursor.fetchall()
                finally:
                    await cursor.close()
            records = [_session_record_from_row(row, deserializer=self._json_deserializer) for row in rows]
            return [
                record
                for record in records
                if record["app_name"] == app_name and (user_id is None or record["user_id"] == user_id)
            ]
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        sql = f"DELETE FROM {_table_ref(self._session_table)} WHERE app_name = %s AND user_id = %s AND id = %s"
        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (app_name, user_id, session_id))
            finally:
                await cursor.close()
            await conn.commit()

    async def append_event(self, event_record: StoredEvent) -> None:
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = f"""
        INSERT INTO {_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        )
        SELECT %s, %s, %s, %s, %s, %s, %s
        FROM {_table_ref(self._session_table)}
        WHERE id = %s AND app_name = %s AND user_id = %s
        """
        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(
                    sql,
                    (
                        *_event_insert_params(event_record, serializer=self._json_serializer),
                        session_id,
                        app_name,
                        user_id,
                    ),
                )
                inserted = cursor.rowcount
            finally:
                await cursor.close()
            if not inserted or inserted <= 0:
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
        INSERT INTO {_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        update_sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                async with self._config.provide_connection() as conn:
                    cursor = await conn.cursor()
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
                                _json_for_storage(event_record["event_data"], serializer=self._json_serializer),
                            ),
                        )
                        if app_state_delta:
                            await cursor.execute(
                                _ensure_app_state_row_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            await cursor.execute(_select_app_state_for_update_sql(self._app_state_table), (app_name,))
                            app_row = await cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            await cursor.execute(
                                _update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        if user_state_delta:
                            await cursor.execute(
                                _ensure_user_state_row_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            await cursor.execute(
                                _select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = await cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            await cursor.execute(
                                _update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        await conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            await conn.rollback()
                        raise
                    finally:
                        await cursor.close()
            except Exception as exc:
                if not _is_retryable_error(exc) or attempt + 1 >= max_attempts:
                    raise
                await asyncio.sleep(_calculate_backoff_seconds(attempt))
            else:
                session = _session_record_from_row(row, deserializer=self._json_deserializer)
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
        FROM {_table_ref(self._events_table)} e
        JOIN {_table_ref(self._session_table)} s
            ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.timestamp {order_direction}{limit_clause}
        """

        try:
            async with self._provide_read_connection(primary=False) as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, params)
                    rows = await cursor.fetchall()
                finally:
                    await cursor.close()
            if limit is not None:
                rows = list(reversed(rows))
            return [_event_record_from_row(row, deserializer=self._json_deserializer) for row in rows]
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

    async def delete_expired_events(self, before: "datetime", app_name: "str | None" = None) -> int:
        return await _async_delete_before(self, self._events_table, "timestamp", before, app_name)

    async def delete_idle_sessions(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        return await _async_delete_before(self, self._session_table, "update_time", updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        return await _async_delete_before(self, self._user_state_table, "update_time", updated_before, app_name)

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        return await _get_state_async(self, self._app_state_table, "app_name = %s", (app_name,))

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        return await _get_user_state_async(self, app_name, user_id)

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        await _async_execute_commit(
            self, _upsert_app_state_sql(self._app_state_table), (app_name, self._json_serializer(state))
        )

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        await _async_execute_commit(
            self, _upsert_user_state_sql(self._user_state_table), (app_name, user_id, self._json_serializer(state))
        )

    async def get_metadata(self, key: str) -> "str | None":

        sql = f"SELECT value FROM {_table_ref(self._metadata_table)} WHERE `key` = %s"
        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, (key,))
                    row = await cursor.fetchone()
                finally:
                    await cursor.close()
            return str(row[0]) if row is not None else None
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return None
            raise

    async def set_metadata(self, key: str, value: str) -> None:
        await _async_execute_commit(self, _upsert_metadata_sql(self._metadata_table), (key, value))

    async def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "session_table_options")
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, table_options)

    async def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _adk_config(self._config))

    async def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "app_state_table_options")
        return _app_state_ddl(self._app_state_table, table_options)

    async def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "user_state_table_options")
        return _user_state_ddl(self._user_state_table, table_options)

    async def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

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


class MysqlConnectorSyncADKStore(BaseSyncADKStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB ADK store using mysql-connector sync driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(
        self, config: "MysqlConnectorSyncConfig", *, read_config: "MysqlConnectorSyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)
        driver_features = getattr(config, "driver_features", None)
        features = driver_features if isinstance(driver_features, Mapping) else {}
        serializer = features.get("json_serializer")
        deserializer = features.get("json_deserializer")
        self._json_serializer: Callable[[Any], str] = (
            cast("Callable[[Any], str]", serializer) if callable(serializer) else to_json
        )
        self._json_deserializer: Callable[[Any], Any] = (
            cast("Callable[[Any], Any]", deserializer) if callable(deserializer) else from_json
        )

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
        self.reconcile_schema(assume_existing=True)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Reconcile legacy column collations and missing session/event secondary indexes."""
        adk_config = _adk_config(self._config)
        fk_targets = (
            (self._session_table, _MYSQL_SESSION_IDENTITY_COLUMNS),
            (self._events_table, _MYSQL_EVENT_IDENTITY_COLUMNS),
        )
        fk_alter_statements: list[str] = []
        for table_name, columns in fk_targets:
            sql, params = _columns_query(table_name)
            column_rows = driver.select(sql, *params)
            fk_alter_statements.extend(_column_alter_statements(table_name, columns, list(column_rows)))
        if fk_alter_statements:
            driver.execute("SET FOREIGN_KEY_CHECKS = 0")
            try:
                for statement in fk_alter_statements:
                    driver.execute(statement)
            finally:
                driver.execute("SET FOREIGN_KEY_CHECKS = 1")

        other_targets: tuple[tuple[str, tuple[str, ...]], ...] = (
            (self._app_state_table, _MYSQL_APP_STATE_IDENTITY_COLUMNS),
            (self._user_state_table, _MYSQL_USER_STATE_IDENTITY_COLUMNS),
        )
        for state_table_name, state_columns in other_targets:
            sql, params = _columns_query(state_table_name)
            column_rows = driver.select(sql, *params)
            for statement in _column_alter_statements(state_table_name, state_columns, list(column_rows)):
                driver.execute(statement)

        session_db, session_bare = _split_table(self._session_table)
        session_rows = driver.data_dictionary.get_indexes(driver, table=session_bare, schema=session_db)
        for statement in _missing_index_statements(
            self._session_table, _session_index_specs(self._session_table), list(session_rows)
        ):
            driver.execute(statement)

        events_db, events_bare = _split_table(self._events_table)
        event_rows = driver.data_dictionary.get_indexes(driver, table=events_bare, schema=events_db)
        for statement in _missing_index_statements(
            self._events_table, _event_index_specs(self._events_table, adk_config), list(event_rows)
        ):
            driver.execute(statement)

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
        """Create a new session and atomically apply/read scoped state."""
        params: tuple[Any, ...]
        state_json = self._json_serializer(state)
        empty_state_json = self._json_serializer({})
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {_table_ref(self._session_table)} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {_table_ref(self._session_table)} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
            """
            params = (session_id, app_name, user_id, state_json)

        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                with self._config.provide_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute("BEGIN")
                        cursor.execute(sql, params)
                        cursor.execute(select_sql, (app_name, user_id, session_id))
                        row = cursor.fetchone()
                        row = _require_created_session_row(row)

                        if app_state_delta:
                            cursor.execute(
                                _ensure_app_state_row_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            cursor.execute(_select_app_state_for_update_sql(self._app_state_table), (app_name,))
                            app_row = cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            cursor.execute(
                                _update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        else:
                            cursor.execute(_select_app_state_sql(self._app_state_table), (app_name,))
                            app_row = cursor.fetchone()
                            merged_app_state = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                if app_row is not None
                                else None
                            )

                        if user_state_delta:
                            cursor.execute(
                                _ensure_user_state_row_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            cursor.execute(
                                _select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            cursor.execute(
                                _update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        else:
                            cursor.execute(_select_user_state_sql(self._user_state_table), (app_name, user_id))
                            user_row = cursor.fetchone()
                            merged_user_state = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                if user_row is not None
                                else None
                            )

                        conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            conn.rollback()
                        raise
                    finally:
                        cursor.close()

            except Exception as exc:
                if not _is_retryable_error(exc) or attempt + 1 >= max_attempts:
                    raise
                time.sleep(_calculate_backoff_seconds(attempt))
            else:
                session = _session_record_from_row(row, deserializer=self._json_deserializer)
                session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
                return session
        msg = "Failed to merge scoped state during create_session after retries."
        raise DeadlockError(msg)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Get session by ID."""

        try:
            with self._provide_read_connection(primary=renew_for is not None) as conn:
                cursor = conn.cursor()
                try:
                    if self._should_renew(renew_for):
                        cursor.execute(
                            f"""
                            UPDATE {_table_ref(self._session_table)}
                            SET update_time = UTC_TIMESTAMP(6)
                            WHERE app_name = %s AND user_id = %s AND id = %s
                            """,
                            (app_name, user_id, session_id),
                        )
                        conn.commit()

                    cursor.execute(
                        f"""
                        SELECT id, app_name, user_id, state, create_time, update_time
                        FROM {_table_ref(self._session_table)}
                        WHERE app_name = %s AND user_id = %s AND id = %s
                        """,
                        (app_name, user_id, session_id),
                    )
                    row = cursor.fetchone()
                finally:
                    cursor.close()
            return _session_record_from_row(row, deserializer=self._json_deserializer) if row is not None else None
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (self._json_serializer(state), app_name, user_id, session_id))
            finally:
                cursor.close()
            conn.commit()

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
        """List sessions for an app."""
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )

        try:
            with self._provide_read_connection(primary=False) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
            records = [_session_record_from_row(row, deserializer=self._json_deserializer) for row in rows]
            return [
                record
                for record in records
                if record["app_name"] == app_name and (user_id is None or record["user_id"] == user_id)
            ]
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        sql = f"DELETE FROM {_table_ref(self._session_table)} WHERE app_name = %s AND user_id = %s AND id = %s"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (app_name, user_id, session_id))
            finally:
                cursor.close()
            conn.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        sql = f"""
        INSERT INTO {_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        )
        SELECT %s, %s, %s, %s, %s, %s, %s
        FROM {_table_ref(self._session_table)}
        WHERE id = %s AND app_name = %s AND user_id = %s
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        *_event_insert_params(event_record, serializer=self._json_serializer),
                        session_id,
                        app_name,
                        user_id,
                    ),
                )
                inserted = cursor.rowcount
            finally:
                cursor.close()
            if not inserted or inserted <= 0:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
                raise ValueError(msg)
            conn.commit()

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
        INSERT INTO {_table_ref(self._events_table)} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        update_sql = f"""
        UPDATE {_table_ref(self._session_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s AND id = %s
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {_table_ref(self._session_table)}
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        has_deltas = bool(app_state_delta or user_state_delta)
        max_attempts = _MAX_STATE_MERGE_ATTEMPTS if has_deltas else 3
        for attempt in range(max_attempts):
            merged_app_state: dict[str, Any] | None = None
            merged_user_state: dict[str, Any] | None = None
            try:
                with self._config.provide_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute("BEGIN")
                        cursor.execute(update_sql, (self._json_serializer(state), app_name, user_id, session_id))
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
                                _json_for_storage(event_record["event_data"], serializer=self._json_serializer),
                            ),
                        )
                        if app_state_delta:
                            cursor.execute(
                                _ensure_app_state_row_sql(self._app_state_table), (app_name, empty_state_json)
                            )
                            cursor.execute(_select_app_state_for_update_sql(self._app_state_table), (app_name,))
                            app_row = cursor.fetchone()
                            current_app = (
                                _json_dict(_row_first_value(app_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if app_row is not None
                                else {}
                            )
                            merged_app_state = {**current_app, **app_state_delta}
                            cursor.execute(
                                _update_app_state_sql(self._app_state_table),
                                (self._json_serializer(merged_app_state), app_name),
                            )
                        if user_state_delta:
                            cursor.execute(
                                _ensure_user_state_row_sql(self._user_state_table),
                                (app_name, user_id, empty_state_json),
                            )
                            cursor.execute(
                                _select_user_state_for_update_sql(self._user_state_table), (app_name, user_id)
                            )
                            user_row = cursor.fetchone()
                            current_user = (
                                _json_dict(_row_first_value(user_row, "state"), deserializer=self._json_deserializer)
                                or {}
                                if user_row is not None
                                else {}
                            )
                            merged_user_state = {**current_user, **user_state_delta}
                            cursor.execute(
                                _update_user_state_sql(self._user_state_table),
                                (self._json_serializer(merged_user_state), app_name, user_id),
                            )
                        conn.commit()
                    except Exception:
                        with contextlib.suppress(Exception):
                            conn.rollback()
                        raise
                    finally:
                        cursor.close()

            except Exception as exc:
                if not _is_retryable_error(exc) or attempt + 1 >= max_attempts:
                    raise
                time.sleep(_calculate_backoff_seconds(attempt))
            else:
                session = _session_record_from_row(row, deserializer=self._json_deserializer)
                if merged_app_state is not None or merged_user_state is not None:
                    session["state"] = merge_scoped_state(session["state"], merged_app_state, merged_user_state)
                return session
        msg = "Failed to merge scoped state during append_event_and_update_state after retries."
        raise DeadlockError(msg)

    def get_events(
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
        FROM {_table_ref(self._events_table)} e
        JOIN {_table_ref(self._session_table)} s
            ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.timestamp {order_direction}{limit_clause}
        """

        try:
            with self._provide_read_connection(primary=False) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, tuple(params))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
            if limit is not None:
                rows = list(reversed(rows))
            return [_event_record_from_row(row, deserializer=self._json_deserializer) for row in rows]
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

    def delete_expired_events(self, before: "datetime", app_name: "str | None" = None) -> int:
        """Delete events older than the given timestamp."""
        return _sync_delete_before(self, self._events_table, "timestamp", before, app_name)

    def delete_idle_sessions(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete sessions whose update_time predates the threshold."""
        return _sync_delete_before(self, self._session_table, "update_time", updated_before, app_name)

    def delete_idle_user_states(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete user state rows whose update_time predates the threshold."""
        return _sync_delete_before(self, self._user_state_table, "update_time", updated_before, app_name)

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        return _get_state_sync(self, self._app_state_table, "app_name = %s", (app_name,))

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        return _get_user_state_sync(self, app_name, user_id)

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        _sync_execute_commit(
            self, _upsert_app_state_sql(self._app_state_table), (app_name, self._json_serializer(state))
        )

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        _sync_execute_commit(
            self, _upsert_user_state_sql(self._user_state_table), (app_name, user_id, self._json_serializer(state))
        )

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""

        sql = f"SELECT value FROM {_table_ref(self._metadata_table)} WHERE `key` = %s"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (key,))
                    row = cursor.fetchone()
                finally:
                    cursor.close()
            return str(row[0]) if row is not None else None
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        _sync_execute_commit(self, _upsert_metadata_sql(self._metadata_table), (key, value))

    def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "session_table_options")
        return _sessions_ddl(self._session_table, self._owner_id_column_ddl, table_options)

    def _events_table_ddl(self) -> str:
        return _events_ddl(self._events_table, self._session_table, _adk_config(self._config))

    def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "app_state_table_options")
        return _app_state_ddl(self._app_state_table, table_options)

    def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        table_options = _table_options(adk_config, "user_state_table_options")
        return _user_state_ddl(self._user_state_table, table_options)

    def _metadata_table_ddl(self) -> str:
        return _metadata_ddl(self._metadata_table)

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


class MysqlConnectorAsyncADKMemoryStore(BaseAsyncADKMemoryStore["MysqlConnectorAsyncConfig"]):
    """MySQL/MariaDB ADK memory store using mysql-connector async driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(
        self, config: "MysqlConnectorAsyncConfig", *, read_config: "MysqlConnectorAsyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)
        driver_features = getattr(config, "driver_features", None)
        features = driver_features if isinstance(driver_features, Mapping) else {}
        serializer = features.get("json_serializer")
        deserializer = features.get("json_deserializer")
        self._json_serializer: Callable[[Any], str] = (
            cast("Callable[[Any], str]", serializer) if callable(serializer) else to_json
        )
        self._json_deserializer: Callable[[Any], Any] = (
            cast("Callable[[Any], Any]", deserializer) if callable(deserializer) else from_json
        )

    async def create_tables(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())
        await self.reconcile_schema(assume_existing=True)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Reconcile legacy memory identity collations and missing ordinary/FULLTEXT indexes."""
        if not self._enabled:
            return
        sql, params = _columns_query(self._memory_table)
        column_rows = await driver.select(sql, *params)
        for statement in _column_alter_statements(
            self._memory_table, _MYSQL_MEMORY_IDENTITY_COLUMNS, list(column_rows)
        ):
            await driver.execute(statement)

        memory_db, memory_bare = _split_table(self._memory_table)
        memory_rows = await driver.data_dictionary.get_indexes(driver, table=memory_bare, schema=memory_db)
        for statement in _missing_index_statements(
            self._memory_table, _memory_index_specs(self._memory_table, use_fts=self._use_fts), list(memory_rows)
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
            INSERT IGNORE INTO {_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        async with self._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                for entry in entries:
                    params: tuple[Any, ...]
                    content_json = self._json_serializer(entry["content_json"])
                    metadata_val = entry.get("metadata_json")
                    metadata_json = self._json_serializer(metadata_val) if metadata_val is not None else None
                    if self._owner_id_column_name:
                        params = (
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
                        )
                    else:
                        params = (
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
                        )
                    await cursor.execute(sql, params)
                    inserted_count += max(0, int(cursor.rowcount or 0))
            finally:
                await cursor.close()
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
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        limit_value = limit or self._max_results
        where_scope, scope_params = _build_mysql_scope_where(app_name, user_id, scope_filter)
        if self._use_fts:
            sql = f"""
            SELECT * FROM {_table_ref(self._memory_table)}
            WHERE {where_scope}
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {_table_ref(self._memory_table)}
            WHERE {where_scope} AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, f"%{query}%", limit_value)

        try:
            async with self._provide_read_connection(primary=False) as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, params)
                    rows = await cursor.fetchall()
                    columns = [col[0] for col in cursor.description or []]
                finally:
                    await cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

        records: list[StoredMemory] = []
        for row in rows:
            rec = cast("StoredMemory", dict(zip(columns, row, strict=False)))
            if not _memory_record_matches_scope(rec, app_name, user_id, scope_filter):
                continue
            rec["content_json"] = _json_dict(rec.get("content_json"), deserializer=self._json_deserializer)
            metadata_val = rec.get("metadata_json")
            rec["metadata_json"] = (
                _json_dict(metadata_val, deserializer=self._json_deserializer) if metadata_val is not None else None
            )
            rec["embedding"] = None
            records.append(rec)
        return records

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a specific session, owned by an app and user."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses = ["session_id = %s"]
        params: list[Any] = [session_id]
        clauses.append("app_name = %s")
        params.append(app_name)
        clauses.append("user_id = %s")
        params.append(user_id)
        sql = f"DELETE FROM {_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, tuple(params))
                    await conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    await cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return 0
            raise

    async def delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
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
        sql = f"DELETE FROM {_table_ref(self._memory_table)} WHERE {where_sql}"
        try:
            async with self._config.provide_connection() as conn:
                cursor = await conn.cursor()
                try:
                    await cursor.execute(sql, tuple(params))
                    await conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    await cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return 0
            raise

    async def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        return _memory_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            use_fts=self._use_fts,
            table_options=_table_options(adk_config, "memory_table_options"),
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {_table_ref(self._memory_table)}"]


class MysqlConnectorSyncADKMemoryStore(BaseSyncADKMemoryStore["MysqlConnectorSyncConfig"]):
    """MySQL/MariaDB ADK memory store using mysql-connector sync driver."""

    __slots__ = ("_json_deserializer", "_json_serializer")

    _max_table_name_parts: ClassVar[int] = 2

    def __init__(
        self, config: "MysqlConnectorSyncConfig", *, read_config: "MysqlConnectorSyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)
        driver_features = getattr(config, "driver_features", None)
        features = driver_features if isinstance(driver_features, Mapping) else {}
        serializer = features.get("json_serializer")
        deserializer = features.get("json_deserializer")
        self._json_serializer: Callable[[Any], str] = (
            cast("Callable[[Any], str]", serializer) if callable(serializer) else to_json
        )
        self._json_deserializer: Callable[[Any], Any] = (
            cast("Callable[[Any], Any]", deserializer) if callable(deserializer) else from_json
        )

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
        self.reconcile_schema(assume_existing=True)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Reconcile legacy memory identity collations and missing ordinary/FULLTEXT indexes."""
        if not self._enabled:
            return
        sql, params = _columns_query(self._memory_table)
        column_rows = driver.select(sql, *params)
        for statement in _column_alter_statements(
            self._memory_table, _MYSQL_MEMORY_IDENTITY_COLUMNS, list(column_rows)
        ):
            driver.execute(statement)

        memory_db, memory_bare = _split_table(self._memory_table)
        memory_rows = driver.data_dictionary.get_indexes(driver, table=memory_bare, schema=memory_db)
        for statement in _missing_index_statements(
            self._memory_table, _memory_index_specs(self._memory_table, use_fts=self._use_fts), list(memory_rows)
        ):
            driver.execute(statement)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication on (app_name, user_id, event_id)."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            sql = f"""
            INSERT IGNORE INTO {_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                {self._owner_id_column_name}, timestamp, content_json,
                content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            sql = f"""
            INSERT IGNORE INTO {_table_ref(self._memory_table)} (
                id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                for entry in entries:
                    params: tuple[Any, ...]
                    content_json = self._json_serializer(entry["content_json"])
                    metadata_val = entry.get("metadata_json")
                    metadata_json = self._json_serializer(metadata_val) if metadata_val is not None else None
                    if self._owner_id_column_name:
                        params = (
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
                        )
                    else:
                        params = (
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
                        )
                    cursor.execute(sql, cast("tuple[Any, ...]", params))
                    inserted_count += max(0, int(cursor.rowcount or 0))
            finally:
                cursor.close()
            conn.commit()
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
            SELECT * FROM {_table_ref(self._memory_table)}
            WHERE {where_scope}
              AND MATCH(content_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, query, limit_value)
        else:
            sql = f"""
            SELECT * FROM {_table_ref(self._memory_table)}
            WHERE {where_scope} AND content_text LIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
            params = (*scope_params, f"%{query}%", limit_value)

        try:
            with self._provide_read_connection(primary=False) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    columns = [col[0] for col in cursor.description or []]
                finally:
                    cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return []
            raise

        records: list[StoredMemory] = []
        for row in rows:
            rec = cast("StoredMemory", dict(zip(columns, row, strict=False)))
            if not _memory_record_matches_scope(rec, app_name, user_id, scope_filter):
                continue
            rec["content_json"] = _json_dict(rec.get("content_json"), deserializer=self._json_deserializer)
            metadata_val = rec.get("metadata_json")
            rec["metadata_json"] = (
                _json_dict(metadata_val, deserializer=self._json_deserializer) if metadata_val is not None else None
            )
            rec["embedding"] = None
            records.append(rec)
        return records

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a specific session, owned by an app and user."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses = ["session_id = %s"]
        params: list[Any] = [session_id]
        clauses.append("app_name = %s")
        params.append(app_name)
        clauses.append("user_id = %s")
        params.append(user_id)
        sql = f"DELETE FROM {_table_ref(self._memory_table)} WHERE {' AND '.join(clauses)}"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, tuple(params))
                    conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return 0
            raise

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
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
        sql = f"DELETE FROM {_table_ref(self._memory_table)} WHERE {where_sql}"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, tuple(params))
                    conn.commit()
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                finally:
                    cursor.close()
        except MysqlConnectorError as exc:
            if _is_table_missing(exc):
                return 0
            raise

    def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        return _memory_ddl(
            self._memory_table,
            self._owner_id_column_ddl,
            use_fts=self._use_fts,
            table_options=_table_options(adk_config, "memory_table_options"),
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {_table_ref(self._memory_table)}"]


def _split_table(table: str) -> tuple[str | None, str]:
    """Split a validated 1- or 2-part MySQL table name into (database, bare_table)."""
    if "." in table:
        database_name, bare_table = table.split(".", 1)
        return database_name, bare_table
    return None, table


def _table_ref(table: str) -> str:
    """Render database.table via format_identifier while keeping bare names unquoted."""
    if "." in table:
        return format_identifier(table)
    return table


def _index_name(table: str, suffix: str) -> str:
    """Derive a per-table MySQL index identifier from the bare table name."""
    _, bare_table = _split_table(table)
    return f"idx_{bare_table}_{suffix}"


def _owner_id_column_parts(column_ddl: str) -> "tuple[str, str]":
    references_match = re.search(r"\s+REFERENCES\s+(.+)", column_ddl, re.IGNORECASE)
    if not references_match:
        return (column_ddl.strip(), "")

    col_def = column_ddl[: references_match.start()].strip()
    fk_clause = references_match.group(1).strip()
    col_name = col_def.split()[0]
    fk_constraint = f"FOREIGN KEY ({col_name}) REFERENCES {fk_clause}"
    return (col_def, fk_constraint)


def _adk_config(config: Any) -> MysqlConnectorADKConfig:
    """Return mysql-connector ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("MysqlConnectorADKConfig", adk_config)


def _table_options(adk_config: Mapping[str, Any], key: str) -> str:
    value = adk_config.get(key)
    if not isinstance(value, str):
        return ""
    value = value.strip()
    return f" {value}" if value else ""


def _is_table_missing(exc: BaseException) -> bool:
    args = exc.args
    errno = exc.errno if isinstance(exc, HasErrnoProtocol) else None
    return (
        errno == MYSQL_TABLE_NOT_FOUND_ERROR
        or "doesn't exist" in str(exc)
        or bool(args and args[0] == MYSQL_TABLE_NOT_FOUND_ERROR)
    )


def _is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionTimeoutError, DeadlockError)):
        return True
    if type(exc).__name__ == "PoolError":
        return True
    args = exc.args
    errno = exc.errno if isinstance(exc, HasErrnoProtocol) else None
    sqlstate = getattr(exc, "sqlstate", None)
    return errno in _MYSQL_RETRYABLE_ERRNOS or sqlstate == "40001" or bool(args and args[0] in _MYSQL_RETRYABLE_ERRNOS)


def _calculate_backoff_seconds(attempt: int) -> float:
    base = min(0.05, 0.002 * (attempt + 1))
    jitter = secrets.randbelow(80) / 10_000.0
    return base + jitter


def _json_for_storage(value: Any, *, serializer: Callable[[Any], str] = to_json) -> str:
    return value if isinstance(value, str) else serializer(value)


def _json_dict(value: Any, *, deserializer: Callable[[Any], Any] = from_json) -> "dict[str, Any]":
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, (bytes, str)):
        return cast("dict[str, Any]", deserializer(value))
    return cast("dict[str, Any]", value)


def _session_record_from_row(row: Any, *, deserializer: Callable[[Any], Any] = from_json) -> StoredSession:
    return StoredSession(
        id=row[0],
        app_name=row[1],
        user_id=row[2],
        state=_json_dict(row[3], deserializer=deserializer),
        create_time=row[4],
        update_time=row[5],
    )


def _event_record_from_row(row: Any, *, deserializer: Callable[[Any], Any] = from_json) -> StoredEvent:
    return StoredEvent(
        id=row[0],
        app_name=row[1],
        user_id=row[2],
        session_id=row[3],
        invocation_id=row[4],
        timestamp=row[5],
        event_data=_json_dict(row[6], deserializer=deserializer),
    )


def _event_insert_params(event_record: StoredEvent, *, serializer: Callable[[Any], str] = to_json) -> "tuple[Any, ...]":
    return (
        event_record["id"],
        event_record["app_name"],
        event_record["user_id"],
        event_record["session_id"],
        event_record["invocation_id"],
        event_record["timestamp"],
        _json_for_storage(event_record["event_data"], serializer=serializer),
    )


def _raise_session_not_found(session_id: str) -> None:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _require_created_session_row(row: Any) -> Any:
    if row is None:
        msg = "Failed to fetch created session"
        raise RuntimeError(msg)
    return row


async def _async_delete_before(
    store: MysqlConnectorAsyncADKStore,
    table_name: str,
    column_name: str,
    threshold: "datetime",
    app_name: "str | None" = None,
) -> int:

    sql = f"DELETE FROM {_table_ref(table_name)} WHERE {column_name} < %s"
    params: list[Any] = [threshold]
    if app_name is not None:
        sql += " AND app_name = %s"
        params.append(app_name)
    try:
        async with store._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, tuple(params))
                rowcount = cursor.rowcount
            finally:
                await cursor.close()
            await conn.commit()
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return 0
        raise
    else:
        return rowcount if rowcount and rowcount > 0 else 0


async def _get_state_async(
    store: MysqlConnectorAsyncADKStore, table_name: str, where_clause: str, params: "tuple[Any, ...]"
) -> "dict[str, Any] | None":

    sql = f"SELECT state FROM {_table_ref(table_name)} WHERE {where_clause} LIMIT 1"
    try:
        async with store._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, params)
                row = await cursor.fetchone()
            finally:
                await cursor.close()
        return _json_dict(row[0], deserializer=store._json_deserializer) if row is not None else None
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return None
        raise


async def _get_user_state_async(
    store: MysqlConnectorAsyncADKStore, app_name: str, user_id: str
) -> "dict[str, Any] | None":
    sql = (
        f"SELECT app_name, user_id, state FROM {_table_ref(store._user_state_table)} "
        "WHERE app_name = %s AND user_id = %s LIMIT 1"
    )
    try:
        async with store._config.provide_connection() as conn:
            cursor = await conn.cursor()
            try:
                await cursor.execute(sql, (app_name, user_id))
                row = await cursor.fetchone()
            finally:
                await cursor.close()
        return _user_state_from_row(row, app_name, user_id, deserializer=store._json_deserializer)
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return None
        raise


async def _async_execute_commit(store: MysqlConnectorAsyncADKStore, sql: str, params: "tuple[Any, ...]") -> None:
    async with store._config.provide_connection() as conn:
        cursor = await conn.cursor()
        try:
            await cursor.execute(sql, params)
        finally:
            await cursor.close()
        await conn.commit()


def _sync_delete_before(
    store: MysqlConnectorSyncADKStore,
    table_name: str,
    column_name: str,
    threshold: "datetime",
    app_name: "str | None" = None,
) -> int:

    sql = f"DELETE FROM {_table_ref(table_name)} WHERE {column_name} < %s"
    params: list[Any] = [threshold]
    if app_name is not None:
        sql += " AND app_name = %s"
        params.append(app_name)
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                rowcount = cursor.rowcount
            finally:
                cursor.close()
            conn.commit()
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return 0
        raise
    else:
        return rowcount if rowcount and rowcount > 0 else 0


def _get_state_sync(
    store: MysqlConnectorSyncADKStore, table_name: str, where_clause: str, params: "tuple[Any, ...]"
) -> "dict[str, Any] | None":

    sql = f"SELECT state FROM {_table_ref(table_name)} WHERE {where_clause} LIMIT 1"
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            finally:
                cursor.close()
        return _json_dict(row[0], deserializer=store._json_deserializer) if row is not None else None
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return None
        raise


def _get_user_state_sync(store: MysqlConnectorSyncADKStore, app_name: str, user_id: str) -> "dict[str, Any] | None":
    sql = (
        f"SELECT app_name, user_id, state FROM {_table_ref(store._user_state_table)} "
        "WHERE app_name = %s AND user_id = %s LIMIT 1"
    )
    try:
        with store._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (app_name, user_id))
                row = cursor.fetchone()
            finally:
                cursor.close()
        return _user_state_from_row(row, app_name, user_id, deserializer=store._json_deserializer)
    except MysqlConnectorError as exc:
        if _is_table_missing(exc):
            return None
        raise


def _sync_execute_commit(store: MysqlConnectorSyncADKStore, sql: str, params: "tuple[Any, ...]") -> None:
    with store._config.provide_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
        finally:
            cursor.close()
        conn.commit()


def _session_index_specs(session_table: str) -> list[tuple[str, str, str]]:
    """Return (keyword, index_name, columns) specs for the ADK sessions table."""
    return [
        ("INDEX", _index_name(session_table, "app_user"), "app_name, user_id"),
        ("INDEX", _index_name(session_table, "update_time"), "update_time DESC"),
    ]


def _event_index_specs(events_table: str, adk_config: Mapping[str, Any] | None = None) -> list[tuple[str, str, str]]:
    """Return (keyword, index_name, columns) specs for the ADK events table."""
    resolved = adk_config or {}
    covering_column = ", invocation_id" if resolved.get("enable_covering_indexes", False) else ""
    specs: list[tuple[str, str, str]] = [
        ("INDEX", _index_name(events_table, "scope"), f"app_name, user_id, session_id, timestamp ASC{covering_column}"),
        ("INDEX", _index_name(events_table, "session"), f"session_id, timestamp ASC{covering_column}"),
        ("INDEX", _index_name(events_table, "app_timestamp"), "app_name, timestamp ASC"),
    ]
    if resolved.get("enable_event_generated_columns", False):
        specs.extend((
            ("INDEX", _index_name(events_table, "author_gc"), "session_id, author_gc, timestamp ASC"),
            ("INDEX", _index_name(events_table, "node_path_gc"), "session_id, node_path_gc, timestamp ASC"),
        ))
    return specs


def _memory_index_specs(memory_table: str, *, use_fts: bool) -> list[tuple[str, str, str]]:
    """Return (keyword, index_name, columns) specs for the ADK memory table."""
    specs: list[tuple[str, str, str]] = [
        ("INDEX", _index_name(memory_table, "app_scope_user_time"), "app_name, scope, user_id, timestamp"),
        ("INDEX", _index_name(memory_table, "scope"), "app_name, scope"),
        ("INDEX", _index_name(memory_table, "session"), "session_id"),
    ]
    if use_fts:
        specs.append(("FULLTEXT INDEX", _index_name(memory_table, "fts"), "content_text"))
    return specs


def _missing_index_statements(table: str, specs: list[tuple[str, str, str]], rows: list[Any]) -> list[str]:
    """Build CREATE INDEX statements for specs absent from catalog index metadata."""
    database_name, bare_table = _split_table(table)
    table_ref = _table_ref(table)
    return [
        f"CREATE {keyword} {index_name} ON {table_ref} ({columns})"
        for keyword, index_name, columns in specs
        if not metadata_has_index(rows, bare_table, index_name, schema_name=database_name)
    ]


def _memory_ddl(memory_table: str, owner_id_column_ddl: "str | None", *, use_fts: bool, table_options: str = "") -> str:
    owner_id_line = ""
    fk_constraint = ""
    if owner_id_column_ddl:
        col_def, fk_def = _owner_id_column_parts(owner_id_column_ddl)
        owner_id_line = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(col_def)
        if fk_def:
            fk_constraint = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(fk_def)

    index_clauses = [
        f"UNIQUE KEY {_index_name(memory_table, 'app_user_event')} (app_name, user_id, event_id)",
        *(
            f"{keyword} {index_name} ({columns})"
            for keyword, index_name, columns in _memory_index_specs(memory_table, use_fts=use_fts)
        ),
    ]
    indexes_sql = ",\n            ".join(index_clauses)
    return _ADK_MEMORY_TABLE_DDL_TEMPLATE_3.format(
        _table_ref(memory_table), owner_id_line, indexes_sql, fk_constraint, table_options
    )


def _sessions_ddl(session_table: str, owner_id_column_ddl: "str | None", table_options: str = "") -> str:
    owner_id_line = ""
    fk_constraint = ""
    if owner_id_column_ddl:
        col_def, fk_def = _owner_id_column_parts(owner_id_column_ddl)
        owner_id_line = _ADK_MYSQL_SESSIONS_DDL_TEMPLATE.format(col_def)
        if fk_def:
            fk_constraint = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(fk_def)

    indexes_sql = ",\n            ".join(
        f"{keyword} {index_name} ({columns})" for keyword, index_name, columns in _session_index_specs(session_table)
    )

    return _ADK_MYSQL_SESSIONS_DDL_TEMPLATE_2.format(
        _table_ref(session_table), owner_id_line, indexes_sql, fk_constraint, table_options
    )


def _events_ddl(events_table: str, session_table: str, adk_config: Mapping[str, Any] | None = None) -> str:
    adk_config = adk_config or {}
    generated_columns = ""
    if adk_config.get("enable_event_generated_columns", False):
        generated_columns = """,
            author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED,
            node_path_gc VARCHAR(512) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED"""

    indexes_sql = ",\n            ".join(
        f"{keyword} {index_name} ({columns})"
        for keyword, index_name, columns in _event_index_specs(events_table, adk_config)
    )
    table_options = _table_options(adk_config, "events_table_options")

    return _ADK_MYSQL_EVENTS_DDL_TEMPLATE_2.format(
        _table_ref(events_table), generated_columns, _table_ref(session_table), indexes_sql, table_options
    )


def _app_state_ddl(app_state_table: str, table_options: str = "") -> str:
    return _ADK_MYSQL_APP_STATE_DDL_TEMPLATE.format(_table_ref(app_state_table), table_options)


def _user_state_ddl(user_state_table: str, table_options: str = "") -> str:
    return _ADK_MYSQL_USER_STATE_DDL_TEMPLATE.format(_table_ref(user_state_table), table_options)


def _metadata_ddl(metadata_table: str) -> str:
    return _ADK_MYSQL_METADATA_DDL_TEMPLATE.format(_table_ref(metadata_table))


def _upsert_app_state_sql(app_state_table: str) -> str:
    return f"""
        INSERT INTO {_table_ref(app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _upsert_user_state_sql(user_state_table: str) -> str:
    return f"""
        INSERT INTO {_table_ref(user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE state = VALUES(state), update_time = UTC_TIMESTAMP(6)
        """


def _upsert_metadata_sql(metadata_table: str) -> str:
    return f"""
        INSERT INTO {_table_ref(metadata_table)} (`key`, value)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """


def _row_first_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    return row[0]


def _ensure_app_state_row_sql(app_state_table: str) -> str:
    return f"""
        INSERT IGNORE INTO {_table_ref(app_state_table)} (app_name, state, update_time)
        VALUES (%s, %s, UTC_TIMESTAMP(6))
        """


def _select_app_state_sql(app_state_table: str) -> str:
    return f"SELECT state FROM {_table_ref(app_state_table)} WHERE app_name = %s"


def _select_app_state_for_update_sql(app_state_table: str) -> str:
    return f"SELECT state FROM {_table_ref(app_state_table)} WHERE app_name = %s FOR UPDATE"


def _update_app_state_sql(app_state_table: str) -> str:
    return f"""
        UPDATE {_table_ref(app_state_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s
        """


def _ensure_user_state_row_sql(user_state_table: str) -> str:
    return f"""
        INSERT IGNORE INTO {_table_ref(user_state_table)} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, UTC_TIMESTAMP(6))
        """


def _select_user_state_sql(user_state_table: str) -> str:
    return f"SELECT state FROM {_table_ref(user_state_table)} WHERE app_name = %s AND user_id = %s"


def _select_user_state_for_update_sql(user_state_table: str) -> str:
    return f"SELECT state FROM {_table_ref(user_state_table)} WHERE app_name = %s AND user_id = %s FOR UPDATE"


def _update_user_state_sql(user_state_table: str) -> str:
    return f"""
        UPDATE {_table_ref(user_state_table)}
        SET state = %s, update_time = UTC_TIMESTAMP(6)
        WHERE app_name = %s AND user_id = %s
        """


def _columns_query(table: str) -> "tuple[str, tuple[Any, ...]]":
    database_name, bare_table = _split_table(table)
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


def _row_field(row: Mapping[str, Any], name: str) -> Any:
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


def _column_alter_statements(table: str, target_columns: "tuple[str, ...]", rows: "list[Any]") -> "list[str]":
    by_name: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_name = _row_field(row, "column_name")
        if isinstance(raw_name, str):
            by_name[raw_name.lower()] = row

    table_ref = _table_ref(table)
    statements: list[str] = []
    for col_name in target_columns:
        row = by_name.get(col_name.lower())
        if row is None:
            continue
        collation = _row_field(row, "collation_name")
        if isinstance(collation, str) and collation.lower() == "utf8mb4_bin":
            continue
        col_type = _row_field(row, "column_type")
        resolved_type = str(col_type).strip() if isinstance(col_type, str) and col_type.strip() else "VARCHAR(128)"
        statements.append(
            f"ALTER TABLE {table_ref} MODIFY COLUMN {col_name} {resolved_type} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"
        )
    return statements


def _build_mysql_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = %s AND ((scope = 'user' AND user_id = %s) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = %s AND scope = 'user' AND user_id = %s", (app_name, user_id)
    return "app_name = %s AND scope = 'app'", (app_name,)


def _user_state_from_row(
    row: Any, app_name: str, user_id: str, *, deserializer: Callable[[Any], Any] = from_json
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
    FROM {_table_ref(session_table)}
    WHERE {where_clause}
    ORDER BY {column} {direction}, id {direction}{page_clause}
    """
    return sql, tuple(params)
