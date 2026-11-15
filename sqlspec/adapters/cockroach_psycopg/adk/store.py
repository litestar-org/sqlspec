"""CockroachDB ADK store for Google Agent Development Kit session/event storage (psycopg)."""

import asyncio
import re
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, Literal, NoReturn, NotRequired, cast

from sqlspec.adapters.cockroach_psycopg._typing import CockroachPsycopgJsonb as Jsonb
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_dict_row as dict_row
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_errors as errors
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_sql as pg_sql
from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    calculate_backoff_seconds,
    is_retryable_error,
)
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.extensions.adk.converters import merge_scoped_state
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from datetime import datetime, timedelta

    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory


__all__ = (
    "CockroachPsycopgADKConfig",
    "CockroachPsycopgAsyncADKMemoryStore",
    "CockroachPsycopgAsyncADKStore",
    "CockroachPsycopgSyncADKMemoryStore",
    "CockroachPsycopgSyncADKStore",
)

logger = get_logger("sqlspec.adapters.cockroach_psycopg.adk.store")
_SIMPLE_LOWER_IDENTIFIER: Final[re.Pattern[str]] = re.compile(r"\A[a-z_][a-z0-9_]*\Z")
_LOCALITY_CLAUSE_PATTERN = re.compile(
    r"\ALOCALITY\s+(?:GLOBAL|REGIONAL(?:\s+BY\s+(?:TABLE(?:\s+IN\s+(?:PRIMARY\s+REGION|[A-Za-z0-9_-]+|\"[A-Za-z0-9_-]+\"))?|ROW(?:\s+AS\s+(?:[A-Za-z_][A-Za-z0-9_]*|\"[A-Za-z_][A-Za-z0-9_]*\"))?))?)\Z",
    re.IGNORECASE,
)


_ADK_SESSIONS_TABLE_DDL_TEMPLATE = ",\n            {0}"

_ADK_SESSIONS_TABLE_DDL_TEMPLATE_2 = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) PRIMARY KEY,\n"
    "            app_name VARCHAR(128) NOT NULL,\n"
    "            user_id VARCHAR(128) NOT NULL{1},\n"
    "            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,\n"
    "            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
    "            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
    "        ){2};\n"
    "\n"
    "        {3}\n"
    "        "
)

_ADK_EVENTS_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) PRIMARY KEY,\n"
    "            app_name VARCHAR(128) NOT NULL,\n"
    "            user_id VARCHAR(128) NOT NULL,\n"
    "            session_id VARCHAR(128) NOT NULL,\n"
    "            invocation_id VARCHAR(256),\n"
    "            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
    "            event_data JSONB NOT NULL,\n"
    "            FOREIGN KEY (session_id) REFERENCES {1}(id) ON DELETE CASCADE\n"
    "        ){2};\n"
    "\n"
    "        {3}\n"
    "        "
)

_ADK_APP_STATES_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            app_name VARCHAR(128) PRIMARY KEY,\n"
    "            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,\n"
    "            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
    "        ){1};\n"
    "        "
)

_ADK_USER_STATES_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            app_name VARCHAR(128) NOT NULL,\n"
    "            user_id VARCHAR(128) NOT NULL,\n"
    "            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,\n"
    "            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
    "            PRIMARY KEY (app_name, user_id)\n"
    "        ){1};\n"
    "        "
)

_ADK_METADATA_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            key VARCHAR(128) PRIMARY KEY,\n"
    "            value VARCHAR(512) NOT NULL\n"
    "        ){1};\n"
    "        "
)

_ADK_MEMORY_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) PRIMARY KEY,\n"
    "            session_id VARCHAR(128) NOT NULL,\n"
    "            app_name VARCHAR(128) NOT NULL,\n"
    "            user_id VARCHAR(128) NOT NULL,\n"
    "            scope VARCHAR(16) NOT NULL DEFAULT 'user',\n"
    "            event_id VARCHAR(128) NOT NULL,\n"
    "            author VARCHAR(256){1},\n"
    "            timestamp TIMESTAMPTZ NOT NULL,\n"
    "            content_json JSONB NOT NULL,\n"
    "            content_text TEXT NOT NULL,\n"
    "            metadata_json JSONB,\n"
    "            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
    "            UNIQUE (app_name, user_id, event_id)\n"
    "        ){2};\n"
    "\n"
    "        {3}\n"
    "        "
)


class CockroachPsycopgADKConfig(ADKConfig):
    """CockroachDB psycopg ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the CockroachDB psycopg ADK stores.
    """

    table_locality: NotRequired[str]
    """Default raw CockroachDB LOCALITY clause for ADK tables."""

    session_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK session table."""

    events_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK events table."""

    app_state_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK app state table."""

    user_state_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK user state table."""

    metadata_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK metadata table."""

    memory_table_locality: NotRequired[str]
    """Raw CockroachDB LOCALITY clause for the ADK memory table."""

    enable_hash_sharded_indexes: NotRequired[bool]
    """Create CockroachDB hash-sharded secondary indexes on hot timestamp access paths."""

    hash_shard_bucket_count: NotRequired[int]
    """Optional bucket count for CockroachDB hash-sharded secondary indexes."""

    enable_storing_indexes: NotRequired[bool]
    """Add CockroachDB STORING columns to common ADK replay/session indexes."""

    enable_memory_trigram_index: NotRequired[bool]
    """Create a CockroachDB trigram GIN index for simple ILIKE memory search."""


class CockroachPsycopgAsyncADKStore(BaseAsyncADKStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB ADK store using psycopg async driver."""

    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(
        self, config: "CockroachPsycopgAsyncConfig", *, read_config: "CockroachPsycopgAsyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)

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
            await driver.commit()

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Ensure CockroachDB session and event companion indexes exist on the async migration driver."""
        adk_config = _adk_config(self._config)
        for statement in (
            *_cockroach_session_index_statements(adk_config, self._session_table),
            *_cockroach_event_index_statements(adk_config, self._events_table),
        ):
            await driver.execute_script(statement)

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
        """Create a new session and atomically apply optional scoped state deltas."""
        state_json = Jsonb(state)
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(
                table=_build_table_identifier(self._session_table),
                owner_id_col=pg_sql.Identifier(self._owner_id_column_name),
            )
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=_build_table_identifier(self._session_table))
            params = (session_id, app_name, user_id, state_json)

        async def _attempt(cur: Any) -> StoredSession:
            await cur.execute(query, params)
            row = await cur.fetchone()
            if row is None:
                msg = "Session creation failed"
                raise RuntimeError(msg)

            resolved_app = (
                await self._apply_app_state_delta(cur, app_name, app_state_delta)
                if app_state_delta
                else await self._select_app_state(cur, app_name)
            )
            resolved_user = (
                await self._apply_user_state_delta(cur, app_name, user_id, user_state_delta)
                if user_state_delta
                else await self._select_user_state(cur, app_name, user_id)
            )

            persisted_state = _decode_json_state(row["state"])
            merged_state = merge_scoped_state(persisted_state, resolved_app, resolved_user)
            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=merged_state,
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

        async with self._config.provide_connection() as conn:
            return await _run_transaction_with_retry_async(self._config, conn, _attempt)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        should_touch = self._should_renew(renew_for)
        if should_touch:
            query = pg_sql.SQL("""
            UPDATE {table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=_build_table_identifier(self._session_table))
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """).format(table=_build_table_identifier(self._session_table))

        try:
            async with (
                self._provide_replica_connection(primary=renew_for is not None) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, (app_name, user_id, session_id))
                row = await cur.fetchone()
                if should_touch:
                    await conn.commit()

            if row is None:
                return None

            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )
        except errors.UndefinedTable:
            return None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        """).format(table=_build_table_identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (Jsonb(state), app_name, user_id, session_id))
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

        query, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )

        try:
            async with (
                self._provide_replica_connection(primary=False) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, params)
                rows = await cur.fetchall()
        except errors.UndefinedTable:
            return []

        return [
            StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )
            for row in rows
        ]

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        query = pg_sql.SQL("DELETE FROM {table} WHERE app_name = %s AND user_id = %s AND id = %s").format(
            table=_build_table_identifier(self._session_table)
        )

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, user_id, session_id))
            await conn.commit()

    async def append_event(self, event_record: StoredEvent) -> None:
        session_id = event_record["session_id"]
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        query = pg_sql.SQL("""
        INSERT INTO {events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        )
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE EXISTS (
            SELECT 1 FROM {session_table}
            WHERE id = %s AND app_name = %s AND user_id = %s
        )
        """).format(
            events_table=_build_table_identifier(self._events_table),
            session_table=_build_table_identifier(self._session_table),
        )
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query,
                (
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                    session_id,
                    app_name,
                    user_id,
                ),
            )
            if not cur.rowcount or cur.rowcount <= 0:
                rollback: Any = getattr(conn, "rollback", None)
                if rollback is not None:
                    await rollback()
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
        """Atomically append an event and update session plus optional scoped deltas."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)

        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """).format(table=_build_table_identifier(self._events_table))
        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """).format(table=_build_table_identifier(self._session_table))
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async def _attempt(cur: Any) -> StoredSession:
            await cur.execute(update_query, (Jsonb(state), app_name, user_id, session_id))
            row = await cur.fetchone()
            if row is None:
                _raise_missing_session(session_id)
            await cur.execute(
                insert_query,
                (
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            merged_app = await self._apply_app_state_delta(cur, app_name, app_state_delta) if app_state_delta else None
            merged_user = (
                await self._apply_user_state_delta(cur, app_name, user_id, user_state_delta)
                if user_state_delta
                else None
            )
            persisted_state = _decode_json_state(row["state"])
            merged_state = merge_scoped_state(persisted_state, merged_app, merged_user)
            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=merged_state,
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

        async with self._config.provide_connection() as conn:
            return await _run_transaction_with_retry_async(self._config, conn, _attempt)

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

        where_clauses = [pg_sql.SQL("s.app_name = %s"), pg_sql.SQL("s.user_id = %s"), pg_sql.SQL("s.id = %s")]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(pg_sql.SQL("e.timestamp > %s"))
            params.append(after_timestamp)

        where_clause = pg_sql.SQL(" AND ").join(where_clauses)
        if limit is not None:
            order_clause = pg_sql.SQL("ORDER BY e.timestamp DESC, e.id DESC LIMIT %s")
            params.append(limit)
        else:
            order_clause = pg_sql.SQL("ORDER BY e.timestamp ASC, e.id ASC")

        query = pg_sql.SQL("""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {events_table} e
        JOIN {session_table} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
        WHERE {where_clause}
        {order_clause}
        """).format(
            events_table=_build_table_identifier(self._events_table),
            session_table=_build_table_identifier(self._session_table),
            where_clause=where_clause,
            order_clause=order_clause,
        )

        try:
            async with (
                self._provide_replica_connection(primary=False) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, tuple(params))
                rows = await cur.fetchall()
        except errors.UndefinedTable:
            return []

        if limit is not None:
            rows = list(reversed(rows))

        return [
            StoredEvent(
                id=row["id"],
                session_id=row["session_id"],
                invocation_id=row["invocation_id"],
                timestamp=row["timestamp"],
                event_data=row["event_data"],
                app_name=row["app_name"],
                user_id=row["user_id"],
            )
            for row in rows
        ]

    async def delete_expired_events(self, before: "datetime", app_name: "str | None" = None) -> int:
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s AND app_name = %s").format(
                table=_build_table_identifier(self._events_table)
            )
            params: tuple[Any, ...] = (before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s").format(
                table=_build_table_identifier(self._events_table)
            )
            params = (before,)

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, params)
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def delete_idle_sessions(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s AND app_name = %s").format(
                table=_build_table_identifier(self._session_table)
            )
            params: tuple[Any, ...] = (updated_before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
                table=_build_table_identifier(self._session_table)
            )
            params = (updated_before,)

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, params)
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def delete_idle_user_states(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s AND app_name = %s").format(
                table=_build_table_identifier(self._user_state_table)
            )
            params: tuple[Any, ...] = (updated_before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
                table=_build_table_identifier(self._user_state_table)
            )
            params = (updated_before,)

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, params)
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s").format(
            table=_build_table_identifier(self._app_state_table)
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (app_name,))
                row = await cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s AND user_id = %s").format(
            table=_build_table_identifier(self._user_state_table)
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (app_name, user_id))
                row = await cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        UPSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """).format(table=_build_table_identifier(self._app_state_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, Jsonb(state)))
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        UPSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """).format(table=_build_table_identifier(self._user_state_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, user_id, Jsonb(state)))
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        query = pg_sql.SQL("SELECT value FROM {table} WHERE key = %s").format(
            table=_build_table_identifier(self._metadata_table)
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (key,))
                row = await cur.fetchone()
                return row["value"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def set_metadata(self, key: str, value: str) -> None:
        query = pg_sql.SQL("""
        UPSERT INTO {table} (key, value)
        VALUES (%s, %s)
        """).format(table=_build_table_identifier(self._metadata_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (key, value))
            await conn.commit()

    async def _select_app_state(self, cur: Any, app_name: str) -> "dict[str, Any] | None":
        return await self._select_state(
            cur, table=self._app_state_table, key_columns=("app_name",), key_values=(app_name,)
        )

    async def _select_user_state(self, cur: Any, app_name: str, user_id: str) -> "dict[str, Any] | None":
        return await self._select_state(
            cur, table=self._user_state_table, key_columns=("app_name", "user_id"), key_values=(app_name, user_id)
        )

    async def _select_state(
        self, cur: Any, *, table: str, key_columns: "tuple[str, ...]", key_values: "tuple[Any, ...]"
    ) -> "dict[str, Any] | None":
        select_query, _, _, _ = _cockroach_scoped_state_queries(table, key_columns)
        await cur.execute(select_query, key_values)
        row = await cur.fetchone() if hasattr(cur, "fetchone") else None
        if row is None:
            return None
        return _decode_json_state(row["state"])

    async def _apply_app_state_delta(self, cur: Any, app_name: str, delta: "dict[str, Any]") -> "dict[str, Any]":
        return await self._apply_state_delta(
            cur, table=self._app_state_table, key_columns=("app_name",), key_values=(app_name,), delta=delta
        )

    async def _apply_user_state_delta(
        self, cur: Any, app_name: str, user_id: str, delta: "dict[str, Any]"
    ) -> "dict[str, Any]":
        return await self._apply_state_delta(
            cur,
            table=self._user_state_table,
            key_columns=("app_name", "user_id"),
            key_values=(app_name, user_id),
            delta=delta,
        )

    async def _apply_state_delta(
        self,
        cur: Any,
        *,
        table: str,
        key_columns: "tuple[str, ...]",
        key_values: "tuple[Any, ...]",
        delta: "dict[str, Any]",
    ) -> "dict[str, Any]":
        _, ensure_query, lock_query, update_query = _cockroach_scoped_state_queries(table, key_columns)
        await cur.execute(ensure_query, (*key_values, Jsonb({})))
        await cur.execute(lock_query, key_values)
        row = await cur.fetchone() if hasattr(cur, "fetchone") else None
        existing = _decode_json_state(row["state"]) if row is not None else {}
        merged = dict(existing)
        merged.update(delta)
        await cur.execute(update_query, (Jsonb(merged), *key_values))
        return merged

    async def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        session_locality = _cockroach_table_locality_clause(adk_config, "session_table_locality")
        indexes_sql = "\n\n        ".join(_cockroach_session_index_statements(adk_config, self._session_table))

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            _format_ddl_identifier(self._session_table), owner_id_line, session_locality, indexes_sql
        )

    async def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        events_locality = _cockroach_table_locality_clause(adk_config, "events_table_locality")
        indexes_sql = "\n\n        ".join(_cockroach_event_index_statements(adk_config, self._events_table))

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._events_table),
            _format_ddl_identifier(self._session_table),
            events_locality,
            indexes_sql,
        )

    async def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        app_state_locality = _cockroach_table_locality_clause(adk_config, "app_state_table_locality")

        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._app_state_table), app_state_locality
        )

    async def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        user_state_locality = _cockroach_table_locality_clause(adk_config, "user_state_table_locality")

        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._user_state_table), user_state_locality
        )

    async def _metadata_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        metadata_locality = _cockroach_table_locality_clause(adk_config, "metadata_table_locality")

        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(_format_ddl_identifier(self._metadata_table), metadata_locality)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._metadata_table)}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._events_table)}",
            f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._session_table)}",
        ]


class CockroachPsycopgSyncADKStore(BaseSyncADKStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK store using psycopg sync driver."""

    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(
        self, config: "CockroachPsycopgSyncConfig", *, read_config: "CockroachPsycopgSyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)

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
            driver.commit()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Ensure CockroachDB session and event companion indexes exist on the sync migration driver."""
        adk_config = _adk_config(self._config)
        for statement in (
            *_cockroach_session_index_statements(adk_config, self._session_table),
            *_cockroach_event_index_statements(adk_config, self._events_table),
        ):
            driver.execute_script(statement)

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
        """Create a new session and atomically apply optional scoped state deltas."""
        state_json = Jsonb(state)
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(
                table=_build_table_identifier(self._session_table),
                owner_id_col=pg_sql.Identifier(self._owner_id_column_name),
            )
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=_build_table_identifier(self._session_table))
            params = (session_id, app_name, user_id, state_json)

        def _attempt(cur: Any) -> StoredSession:
            cur.execute(query, params)
            row = cur.fetchone()
            if row is None:
                msg = "Session creation failed"
                raise RuntimeError(msg)

            resolved_app = (
                self._apply_app_state_delta(cur, app_name, app_state_delta)
                if app_state_delta
                else self._select_app_state(cur, app_name)
            )
            resolved_user = (
                self._apply_user_state_delta(cur, app_name, user_id, user_state_delta)
                if user_state_delta
                else self._select_user_state(cur, app_name, user_id)
            )

            persisted_state = _decode_json_state(row["state"])
            merged_state = merge_scoped_state(persisted_state, resolved_app, resolved_user)
            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=merged_state,
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

        with self._config.provide_connection() as conn:
            return _run_transaction_with_retry_sync(self._config, conn, _attempt)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Get session by ID."""
        should_touch = self._should_renew(renew_for)
        if should_touch:
            query = pg_sql.SQL("""
            UPDATE {table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=_build_table_identifier(self._session_table))
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """).format(table=_build_table_identifier(self._session_table))

        try:
            with (
                self._provide_replica_connection(primary=renew_for is not None) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                cur.execute(query, (app_name, user_id, session_id))
                row = cur.fetchone()
                if should_touch:
                    conn.commit()

            if row is None:
                return None

            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=row["state"],
                create_time=row["create_time"],
                update_time=row["update_time"],
            )
        except errors.UndefinedTable:
            return None

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        """).format(table=_build_table_identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (Jsonb(state), app_name, user_id, session_id))
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

        query, params = _session_list_query(
            self._session_table, app_name, user_id, column, direction, page_limit, page_offset
        )

        try:
            with self._provide_replica_connection(primary=False) as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            return [
                StoredSession(
                    id=row["id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    state=row["state"],
                    create_time=row["create_time"],
                    update_time=row["update_time"],
                )
                for row in rows
            ]
        except errors.UndefinedTable:
            return []

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        query = pg_sql.SQL("DELETE FROM {table} WHERE app_name = %s AND user_id = %s AND id = %s").format(
            table=_build_table_identifier(self._session_table)
        )

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, user_id, session_id))
            conn.commit()

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        self._insert_event(event_record)

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
        """Atomically append an event and update session plus optional scoped deltas."""
        if (event_record["app_name"], event_record["user_id"], event_record["session_id"]) != (
            app_name,
            user_id,
            session_id,
        ):
            msg = "event_record scope does not match target session arguments."
            raise ValueError(msg)

        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """).format(table=_build_table_identifier(self._events_table))
        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """).format(table=_build_table_identifier(self._session_table))
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        def _attempt(cur: Any) -> StoredSession:
            cur.execute(update_query, (Jsonb(state), app_name, user_id, session_id))
            row = cur.fetchone()
            if row is None:
                _raise_missing_session(session_id)
            cur.execute(
                insert_query,
                (
                    event_record["id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            merged_app = self._apply_app_state_delta(cur, app_name, app_state_delta) if app_state_delta else None
            merged_user = (
                self._apply_user_state_delta(cur, app_name, user_id, user_state_delta) if user_state_delta else None
            )
            persisted_state = _decode_json_state(row["state"])
            merged_state = merge_scoped_state(persisted_state, merged_app, merged_user)
            return StoredSession(
                id=row["id"],
                app_name=row["app_name"],
                user_id=row["user_id"],
                state=merged_state,
                create_time=row["create_time"],
                update_time=row["update_time"],
            )

        with self._config.provide_connection() as conn:
            return _run_transaction_with_retry_sync(self._config, conn, _attempt)

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

        where_clauses = [pg_sql.SQL("s.app_name = %s"), pg_sql.SQL("s.user_id = %s"), pg_sql.SQL("s.id = %s")]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(pg_sql.SQL("e.timestamp > %s"))
            params.append(after_timestamp)

        where_clause = pg_sql.SQL(" AND ").join(where_clauses)
        if limit is not None:
            order_clause = pg_sql.SQL("ORDER BY e.timestamp DESC, e.id DESC LIMIT %s")
            params.append(limit)
        else:
            order_clause = pg_sql.SQL("ORDER BY e.timestamp ASC, e.id ASC")

        query = pg_sql.SQL("""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {events_table} e
        JOIN {session_table} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id
        WHERE {where_clause}
        {order_clause}
        """).format(
            events_table=_build_table_identifier(self._events_table),
            session_table=_build_table_identifier(self._session_table),
            where_clause=where_clause,
            order_clause=order_clause,
        )

        try:
            with self._provide_replica_connection(primary=False) as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall()

            if limit is not None:
                rows = list(reversed(rows))

            return [
                StoredEvent(
                    id=row["id"],
                    session_id=row["session_id"],
                    invocation_id=row["invocation_id"],
                    timestamp=row["timestamp"],
                    event_data=row["event_data"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                )
                for row in rows
            ]
        except errors.UndefinedTable:
            return []

    def delete_expired_events(self, before: "datetime", app_name: "str | None" = None) -> int:
        """Delete events older than the given timestamp."""
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s AND app_name = %s").format(
                table=_build_table_identifier(self._events_table)
            )
            params: tuple[Any, ...] = (before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s").format(
                table=_build_table_identifier(self._events_table)
            )
            params = (before,)

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def delete_idle_sessions(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete sessions whose update_time predates the given threshold."""
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s AND app_name = %s").format(
                table=_build_table_identifier(self._session_table)
            )
            params: tuple[Any, ...] = (updated_before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
                table=_build_table_identifier(self._session_table)
            )
            params = (updated_before,)

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def delete_idle_user_states(self, updated_before: "datetime", app_name: "str | None" = None) -> int:
        """Delete user-scoped state rows whose update_time predates the given threshold."""
        if app_name is not None:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s AND app_name = %s").format(
                table=_build_table_identifier(self._user_state_table)
            )
            params: tuple[Any, ...] = (updated_before, app_name)
        else:
            query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
                table=_build_table_identifier(self._user_state_table)
            )
            params = (updated_before,)

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s").format(
            table=_build_table_identifier(self._app_state_table)
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (app_name,))
                row = cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s AND user_id = %s").format(
            table=_build_table_identifier(self._user_state_table)
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (app_name, user_id))
                row = cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        query = pg_sql.SQL("""
        UPSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """).format(table=_build_table_identifier(self._app_state_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, Jsonb(state)))
            conn.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        query = pg_sql.SQL("""
        UPSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """).format(table=_build_table_identifier(self._user_state_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, user_id, Jsonb(state)))
            conn.commit()

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        query = pg_sql.SQL("SELECT value FROM {table} WHERE key = %s").format(
            table=_build_table_identifier(self._metadata_table)
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (key,))
                row = cur.fetchone()
                return row["value"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        query = pg_sql.SQL("""
        UPSERT INTO {table} (key, value)
        VALUES (%s, %s)
        """).format(table=_build_table_identifier(self._metadata_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (key, value))
            conn.commit()

    def _select_app_state(self, cur: Any, app_name: str) -> "dict[str, Any] | None":
        return self._select_state(cur, table=self._app_state_table, key_columns=("app_name",), key_values=(app_name,))

    def _select_user_state(self, cur: Any, app_name: str, user_id: str) -> "dict[str, Any] | None":
        return self._select_state(
            cur, table=self._user_state_table, key_columns=("app_name", "user_id"), key_values=(app_name, user_id)
        )

    def _select_state(
        self, cur: Any, *, table: str, key_columns: "tuple[str, ...]", key_values: "tuple[Any, ...]"
    ) -> "dict[str, Any] | None":
        select_query, _, _, _ = _cockroach_scoped_state_queries(table, key_columns)
        cur.execute(select_query, key_values)
        row = cur.fetchone() if hasattr(cur, "fetchone") else None
        if row is None:
            return None
        return _decode_json_state(row["state"])

    def _apply_app_state_delta(self, cur: Any, app_name: str, delta: "dict[str, Any]") -> "dict[str, Any]":
        return self._apply_state_delta(
            cur, table=self._app_state_table, key_columns=("app_name",), key_values=(app_name,), delta=delta
        )

    def _apply_user_state_delta(
        self, cur: Any, app_name: str, user_id: str, delta: "dict[str, Any]"
    ) -> "dict[str, Any]":
        return self._apply_state_delta(
            cur,
            table=self._user_state_table,
            key_columns=("app_name", "user_id"),
            key_values=(app_name, user_id),
            delta=delta,
        )

    def _apply_state_delta(
        self,
        cur: Any,
        *,
        table: str,
        key_columns: "tuple[str, ...]",
        key_values: "tuple[Any, ...]",
        delta: "dict[str, Any]",
    ) -> "dict[str, Any]":
        _, ensure_query, lock_query, update_query = _cockroach_scoped_state_queries(table, key_columns)
        cur.execute(ensure_query, (*key_values, Jsonb({})))
        cur.execute(lock_query, key_values)
        row = cur.fetchone() if hasattr(cur, "fetchone") else None
        existing = _decode_json_state(row["state"]) if row is not None else {}
        merged = dict(existing)
        merged.update(delta)
        cur.execute(update_query, (Jsonb(merged), *key_values))
        return merged

    def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        session_locality = _cockroach_table_locality_clause(adk_config, "session_table_locality")
        indexes_sql = "\n\n        ".join(_cockroach_session_index_statements(adk_config, self._session_table))

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            _format_ddl_identifier(self._session_table), owner_id_line, session_locality, indexes_sql
        )

    def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        events_locality = _cockroach_table_locality_clause(adk_config, "events_table_locality")
        indexes_sql = "\n\n        ".join(_cockroach_event_index_statements(adk_config, self._events_table))

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._events_table),
            _format_ddl_identifier(self._session_table),
            events_locality,
            indexes_sql,
        )

    def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        app_state_locality = _cockroach_table_locality_clause(adk_config, "app_state_table_locality")

        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._app_state_table), app_state_locality
        )

    def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        user_state_locality = _cockroach_table_locality_clause(adk_config, "user_state_table_locality")

        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._user_state_table), user_state_locality
        )

    def _metadata_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        metadata_locality = _cockroach_table_locality_clause(adk_config, "metadata_table_locality")

        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(_format_ddl_identifier(self._metadata_table), metadata_locality)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._app_state_table)}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._user_state_table)}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._metadata_table)}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._events_table)}",
            f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._session_table)}",
        ]

    def _insert_event(self, event_record: StoredEvent) -> None:
        session_id = event_record["session_id"]
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        query = pg_sql.SQL("""
        INSERT INTO {events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        )
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE EXISTS (
            SELECT 1 FROM {session_table}
            WHERE id = %s AND app_name = %s AND user_id = %s
        )
        """).format(
            events_table=_build_table_identifier(self._events_table),
            session_table=_build_table_identifier(self._session_table),
        )
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                query,
                (
                    event_record["id"],
                    app_name,
                    user_id,
                    session_id,
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                    session_id,
                    app_name,
                    user_id,
                ),
            )
            if not cur.rowcount or cur.rowcount <= 0:
                rollback = getattr(conn, "rollback", None)
                if callable(rollback):
                    rollback()
                msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
                raise ValueError(msg)
            conn.commit()


class CockroachPsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB ADK memory store using psycopg async driver."""

    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(
        self, config: "CockroachPsycopgAsyncConfig", *, read_config: "CockroachPsycopgAsyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)

    async def create_tables(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())
            await driver.commit()

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Ensure CockroachDB memory companion indexes exist on the async migration driver."""
        if not self._enabled:
            return
        adk_config = _adk_config(self._config)
        for statement in _cockroach_memory_index_statements(adk_config, self._memory_table, use_fts=self._use_fts):
            await driver.execute_script(statement)

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        ids = [entry["id"] for entry in entries]
        session_ids = [entry["session_id"] for entry in entries]
        app_names = [entry["app_name"] for entry in entries]
        user_ids = [entry["user_id"] for entry in entries]
        scopes = [entry.get("scope", "user") for entry in entries]
        event_ids = [entry["event_id"] for entry in entries]
        authors = [entry["author"] for entry in entries]
        timestamps = [entry["timestamp"] for entry in entries]
        content_jsons = [Jsonb(entry["content_json"]) for entry in entries]
        content_texts = [entry["content_text"] for entry in entries]
        metadata_jsons = [
            Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None for entry in entries
        ]
        inserted_ats = [entry["inserted_at"] for entry in entries]

        table_ident = _build_table_identifier(self._memory_table)
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, scope, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            )
            SELECT id, session_id, app_name, user_id, scope, event_id, author,
                %s, timestamp, content_json, content_text, metadata_json, inserted_at
            FROM unnest(
                %s::varchar[], %s::varchar[], %s::varchar[], %s::varchar[],
                %s::varchar[], %s::varchar[], %s::varchar[],
                %s::timestamptz[], %s::jsonb[], %s::text[], %s::jsonb[], %s::timestamptz[]
            ) AS batch(id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at)
            ON CONFLICT DO NOTHING
            """).format(table=table_ident, owner_id_col=pg_sql.Identifier(self._owner_id_column_name))
            params: tuple[Any, ...] = (
                owner_id,
                ids,
                session_ids,
                app_names,
                user_ids,
                scopes,
                event_ids,
                authors,
                timestamps,
                content_jsons,
                content_texts,
                metadata_jsons,
                inserted_ats,
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            )
            SELECT * FROM unnest(
                %s::varchar[], %s::varchar[], %s::varchar[], %s::varchar[],
                %s::varchar[], %s::varchar[], %s::varchar[],
                %s::timestamptz[], %s::jsonb[], %s::text[], %s::jsonb[], %s::timestamptz[]
            )
            ON CONFLICT DO NOTHING
            """).format(table=table_ident)
            params = (
                ids,
                session_ids,
                app_names,
                user_ids,
                scopes,
                event_ids,
                authors,
                timestamps,
                content_jsons,
                content_texts,
                metadata_jsons,
                inserted_ats,
            )

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, params)
            inserted_count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
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

        effective_limit = limit if limit is not None else self._max_results

        where_scope, scope_params = _build_cockroach_scope_where(app_name, user_id, scope_filter)
        if self._use_fts:
            sql_query = pg_sql.SQL("""
            SELECT * FROM {table}
            WHERE {where_scope}
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY timestamp DESC
            LIMIT %s
            """).format(table=_build_table_identifier(self._memory_table), where_scope=where_scope)
            search_param = query
            params = (*scope_params, search_param, effective_limit)
        else:
            sql_query = pg_sql.SQL("""
            SELECT * FROM {table}
            WHERE {where_scope} AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """).format(table=_build_table_identifier(self._memory_table), where_scope=where_scope)
            search_param = f"%{query}%"
            params = (*scope_params, search_param, effective_limit)

        try:
            async with self._provide_replica_connection(primary=False) as conn, conn.cursor() as cur:
                await cur.execute(sql_query, params)
                rows = await cur.fetchall()
                columns = [col[0] for col in cur.description or []]
        except errors.UndefinedTable:
            return []

        records: list[StoredMemory] = []
        for row in rows:
            rec = cast("StoredMemory", dict(zip(columns, row, strict=False)))
            rec["embedding"] = None
            records.append(rec)
        return records

    async def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses: list[pg_sql.Composable] = [pg_sql.SQL("session_id = %s")]
        params: list[Any] = [session_id]
        clauses.append(pg_sql.SQL("app_name = %s"))
        params.append(app_name)
        clauses.append(pg_sql.SQL("user_id = %s"))
        params.append(user_id)

        sql_query = pg_sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=_build_table_identifier(self._memory_table), where=pg_sql.SQL(" AND ").join(clauses)
        )
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql_query, tuple(params))
            await conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses: list[pg_sql.Composable] = [pg_sql.SQL("inserted_at < (CURRENT_TIMESTAMP - (%s * INTERVAL '1 day'))")]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append(pg_sql.SQL("app_name = %s"))
            params.append(app_name)
        if scope is not None:
            clauses.append(pg_sql.SQL("scope = %s"))
            params.append(scope)
        where_sql = pg_sql.SQL(" AND ").join(clauses)
        sql_query = pg_sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=_build_table_identifier(self._memory_table), where=where_sql
        )
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql_query, tuple(params))
            await conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        memory_locality = _cockroach_table_locality_clause(adk_config, "memory_table_locality")
        indexes_sql = "\n\n        ".join(
            _cockroach_memory_index_statements(adk_config, self._memory_table, use_fts=self._use_fts)
        )

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._memory_table), owner_id_line, memory_locality, indexes_sql
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._memory_table)}"]


class CockroachPsycopgSyncADKMemoryStore(BaseSyncADKMemoryStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK memory store using psycopg sync driver."""

    __slots__ = ()

    _max_table_name_parts = 2

    def __init__(
        self, config: "CockroachPsycopgSyncConfig", *, read_config: "CockroachPsycopgSyncConfig | None" = None
    ) -> None:
        super().__init__(config, read_config=read_config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())
            driver.commit()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Ensure CockroachDB memory companion indexes exist on the sync migration driver."""
        if not self._enabled:
            return
        adk_config = _adk_config(self._config)
        for statement in _cockroach_memory_index_statements(adk_config, self._memory_table, use_fts=self._use_fts):
            driver.execute_script(statement)

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries deduplicated by (app_name, user_id, event_id)."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        ids = [entry["id"] for entry in entries]
        session_ids = [entry["session_id"] for entry in entries]
        app_names = [entry["app_name"] for entry in entries]
        user_ids = [entry["user_id"] for entry in entries]
        scopes = [entry.get("scope", "user") for entry in entries]
        event_ids = [entry["event_id"] for entry in entries]
        authors = [entry["author"] for entry in entries]
        timestamps = [entry["timestamp"] for entry in entries]
        content_jsons = [Jsonb(entry["content_json"]) for entry in entries]
        content_texts = [entry["content_text"] for entry in entries]
        metadata_jsons = [
            Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None for entry in entries
        ]
        inserted_ats = [entry["inserted_at"] for entry in entries]

        table_ident = _build_table_identifier(self._memory_table)
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, scope, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            )
            SELECT id, session_id, app_name, user_id, scope, event_id, author,
                %s, timestamp, content_json, content_text, metadata_json, inserted_at
            FROM unnest(
                %s::varchar[], %s::varchar[], %s::varchar[], %s::varchar[],
                %s::varchar[], %s::varchar[], %s::varchar[],
                %s::timestamptz[], %s::jsonb[], %s::text[], %s::jsonb[], %s::timestamptz[]
            ) AS batch(id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at)
            ON CONFLICT DO NOTHING
            """).format(table=table_ident, owner_id_col=pg_sql.Identifier(self._owner_id_column_name))
            params: tuple[Any, ...] = (
                owner_id,
                ids,
                session_ids,
                app_names,
                user_ids,
                scopes,
                event_ids,
                authors,
                timestamps,
                content_jsons,
                content_texts,
                metadata_jsons,
                inserted_ats,
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, scope, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            )
            SELECT * FROM unnest(
                %s::varchar[], %s::varchar[], %s::varchar[], %s::varchar[],
                %s::varchar[], %s::varchar[], %s::varchar[],
                %s::timestamptz[], %s::jsonb[], %s::text[], %s::jsonb[], %s::timestamptz[]
            )
            ON CONFLICT DO NOTHING
            """).format(table=table_ident)
            params = (
                ids,
                session_ids,
                app_names,
                user_ids,
                scopes,
                event_ids,
                authors,
                timestamps,
                content_jsons,
                content_texts,
                metadata_jsons,
                inserted_ats,
            )

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            inserted_count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
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

        effective_limit = limit if limit is not None else self._max_results

        where_scope, scope_params = _build_cockroach_scope_where(app_name, user_id, scope_filter)
        if self._use_fts:
            sql_query = pg_sql.SQL("""
            SELECT * FROM {table}
            WHERE {where_scope}
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY timestamp DESC
            LIMIT %s
            """).format(table=_build_table_identifier(self._memory_table), where_scope=where_scope)
            search_param = query
            params = (*scope_params, search_param, effective_limit)
        else:
            sql_query = pg_sql.SQL("""
            SELECT * FROM {table}
            WHERE {where_scope} AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """).format(table=_build_table_identifier(self._memory_table), where_scope=where_scope)
            search_param = f"%{query}%"
            params = (*scope_params, search_param, effective_limit)

        try:
            with self._provide_replica_connection(primary=False) as conn, conn.cursor() as cur:
                cur.execute(sql_query, params)
                rows = cur.fetchall()
                columns = [col[0] for col in cur.description or []]
        except errors.UndefinedTable:
            return []

        records: list[StoredMemory] = []
        for row in rows:
            rec = cast("StoredMemory", dict(zip(columns, row, strict=False)))
            rec["embedding"] = None
            records.append(rec)
        return records

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memory entries for a specific session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses: list[pg_sql.Composable] = [pg_sql.SQL("session_id = %s")]
        params: list[Any] = [session_id]
        clauses.append(pg_sql.SQL("app_name = %s"))
        params.append(app_name)
        clauses.append(pg_sql.SQL("user_id = %s"))
        params.append(user_id)

        sql_query = pg_sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=_build_table_identifier(self._memory_table), where=pg_sql.SQL(" AND ").join(clauses)
        )
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql_query, tuple(params))
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        clauses: list[pg_sql.Composable] = [pg_sql.SQL("inserted_at < (CURRENT_TIMESTAMP - (%s * INTERVAL '1 day'))")]
        params: list[Any] = [int(days)]
        if app_name is not None:
            clauses.append(pg_sql.SQL("app_name = %s"))
            params.append(app_name)
        if scope is not None:
            clauses.append(pg_sql.SQL("scope = %s"))
            params.append(scope)
        where_sql = pg_sql.SQL(" AND ").join(clauses)
        sql_query = pg_sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=_build_table_identifier(self._memory_table), where=where_sql
        )
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql_query, tuple(params))
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        memory_locality = _cockroach_table_locality_clause(adk_config, "memory_table_locality")
        indexes_sql = "\n\n        ".join(
            _cockroach_memory_index_statements(adk_config, self._memory_table, use_fts=self._use_fts)
        )

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(
            _format_ddl_identifier(self._memory_table), owner_id_line, memory_locality, indexes_sql
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {_format_ddl_identifier(self._memory_table)}"]


def _build_table_identifier(table_name: str) -> pg_sql.Identifier:
    """Build a schema-aware Psycopg Identifier from a potentially qualified table name."""
    parts = split_qualified_identifier(table_name, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        return pg_sql.Identifier(table_name)
    return pg_sql.Identifier(*parts)


def _format_identifier_part(part: str) -> str:
    """Quote an identifier segment when it contains uppercase or special characters."""
    if _SIMPLE_LOWER_IDENTIFIER.fullmatch(part) is not None:
        return part
    return quote_identifier(part)


def _format_ddl_identifier(table_name: str) -> str:
    """Format a potentially qualified table name for DDL matching Postgres folding rules."""
    parts = split_qualified_identifier(table_name, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        return quote_identifier(table_name)
    return ".".join(_format_identifier_part(part) for part in parts)


def _format_ddl_index_name(table_name: str, suffix: str) -> str:
    """Format a safe companion index name derived from a potentially qualified table name."""
    parts = split_qualified_identifier(table_name, quote_chars='"', allow_bracket_quotes=False)
    slug = "_".join(parts) if parts else table_name.replace(".", "_")
    return _format_identifier_part(f"idx_{slug}_{suffix}")


def _cockroach_session_index_statements(adk_config: Mapping[str, Any], session_table: str) -> list[str]:
    """Build guarded CockroachDB session companion index DDL statements."""
    hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
    session_storing_clause = _cockroach_storing_clause(adk_config, ("state", "create_time", "update_time"))
    session_sql = _format_ddl_identifier(session_table)
    idx_app_user = _format_ddl_index_name(session_table, "app_user")
    idx_update_time = _format_ddl_index_name(session_table, "update_time")
    idx_state = _format_ddl_index_name(session_table, "state")
    return [
        f"CREATE INDEX IF NOT EXISTS {idx_app_user} ON {session_sql}(app_name, user_id){session_storing_clause};",
        f"CREATE INDEX IF NOT EXISTS {idx_update_time} ON {session_sql}(update_time DESC){hash_shard_clause};",
        f"CREATE INDEX IF NOT EXISTS {idx_state} ON {session_sql} USING GIN (state) WHERE state != '{{}}'::jsonb;",
    ]


def _cockroach_event_index_statements(adk_config: Mapping[str, Any], events_table: str) -> list[str]:
    """Build guarded CockroachDB event companion index DDL statements."""
    hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
    events_storing_clause = _cockroach_storing_clause(adk_config, ("invocation_id", "event_data"))
    events_sql = _format_ddl_identifier(events_table)
    idx_session = _format_ddl_index_name(events_table, "session")
    idx_event_data = _format_ddl_index_name(events_table, "event_data")
    idx_app_timestamp = _format_ddl_index_name(events_table, "app_timestamp")
    return [
        f"CREATE INDEX IF NOT EXISTS {idx_session} ON {events_sql}(session_id, timestamp ASC){hash_shard_clause}{events_storing_clause};",
        f"CREATE INDEX IF NOT EXISTS {idx_event_data} ON {events_sql} USING GIN (event_data);",
        f"CREATE INDEX IF NOT EXISTS {idx_app_timestamp} ON {events_sql}(app_name, timestamp ASC);",
    ]


def _cockroach_memory_index_statements(adk_config: Mapping[str, Any], memory_table: str, *, use_fts: bool) -> list[str]:
    """Build guarded CockroachDB memory companion index DDL statements."""
    hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
    memory_sql = _format_ddl_identifier(memory_table)
    idx_app_scope = _format_ddl_index_name(memory_table, "app_scope_user_time")
    idx_scope = _format_ddl_index_name(memory_table, "scope")
    idx_session = _format_ddl_index_name(memory_table, "session")
    statements = [
        f"CREATE INDEX IF NOT EXISTS {idx_app_scope} ON {memory_sql}(app_name, scope, user_id, timestamp DESC){hash_shard_clause};",
        f"CREATE INDEX IF NOT EXISTS {idx_scope} ON {memory_sql}(app_name, scope);",
        f"CREATE INDEX IF NOT EXISTS {idx_session} ON {memory_sql}(session_id);",
    ]
    if use_fts:
        idx_fts = _format_ddl_index_name(memory_table, "fts")
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {idx_fts} ON {memory_sql} USING GIN (to_tsvector('english', content_text));"
        )
    if adk_config.get("enable_memory_trigram_index", False):
        idx_trgm = _format_ddl_index_name(memory_table, "content_trgm")
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {idx_trgm} ON {memory_sql} USING GIN (content_text gin_trgm_ops);"
        )
    return statements


def _build_insert_params(entry: "StoredMemory") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry.get("scope", "user"),
        entry["event_id"],
        entry["author"],
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


def _build_insert_params_with_owner(entry: "StoredMemory", owner_id: "object | None") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry.get("scope", "user"),
        entry["event_id"],
        entry["author"],
        owner_id,
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


def _raise_missing_session(session_id: str) -> NoReturn:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)


def _adk_config(config: Any) -> CockroachPsycopgADKConfig:
    """Return CockroachDB psycopg ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("CockroachPsycopgADKConfig", adk_config)


def _cockroach_table_locality_clause(adk_config: CockroachPsycopgADKConfig, table_key: str) -> str:
    locality = adk_config.get(table_key) or adk_config.get("table_locality")
    if not isinstance(locality, str):
        return ""
    clause = locality.strip()
    if not clause:
        return ""
    if not clause.upper().startswith("LOCALITY "):
        clause = f"LOCALITY {clause}"
    if not _LOCALITY_CLAUSE_PATTERN.fullmatch(clause):
        msg = f"Invalid CockroachDB LOCALITY clause: {locality!r}"
        raise ImproperConfigurationError(msg)
    return f"\n        {clause}"


def _cockroach_hash_shard_clause(adk_config: Mapping[str, Any]) -> str:
    if not adk_config.get("enable_hash_sharded_indexes", False):
        return ""
    bucket_count = adk_config.get("hash_shard_bucket_count")
    if bucket_count is None:
        return " USING HASH"
    if not isinstance(bucket_count, int) or isinstance(bucket_count, bool) or bucket_count <= 0:
        msg = "extension_config['adk']['hash_shard_bucket_count'] must be a positive integer"
        raise ValueError(msg)
    return f" USING HASH WITH (bucket_count = {bucket_count})"


def _cockroach_storing_clause(adk_config: Mapping[str, Any], columns: tuple[str, ...]) -> str:
    if not adk_config.get("enable_storing_indexes", False):
        return ""
    return f" STORING ({', '.join(columns)})"


def _build_cockroach_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[pg_sql.Composable, tuple[Any, ...]]:
    if scope_filter == "all":
        where = pg_sql.SQL("app_name = %s AND ((scope = 'user' AND user_id = %s) OR scope = 'app')")
        return where, (app_name, user_id)
    if scope_filter == "user":
        where = pg_sql.SQL("app_name = %s AND scope = 'user' AND user_id = %s")
        return where, (app_name, user_id)
    where = pg_sql.SQL("app_name = %s AND scope = 'app'")
    return where, (app_name,)


def _session_list_query(
    session_table: str,
    app_name: str,
    user_id: "str | None",
    column: str,
    direction: str,
    limit: "int | None",
    offset: int,
) -> "tuple[pg_sql.Composed, tuple[Any, ...]]":
    """Return the bounded session-list query and its bound values."""
    params: list[Any] = [app_name]
    user_filter = pg_sql.SQL("")
    if user_id is not None:
        params.append(user_id)
        user_filter = pg_sql.SQL(" AND user_id = %s")

    page_clause = pg_sql.SQL("")
    if limit is not None:
        params.extend((limit, offset))
        page_clause = pg_sql.SQL(" LIMIT %s OFFSET %s")

    column_sql = pg_sql.SQL("create_time") if column == "create_time" else pg_sql.SQL("update_time")
    direction_sql = pg_sql.SQL("DESC") if direction == "DESC" else pg_sql.SQL("ASC")

    query = pg_sql.SQL("""
    SELECT id, app_name, user_id, state, create_time, update_time
    FROM {table}
    WHERE app_name = %s{user_filter}
    ORDER BY {column} {direction}, id {direction}{page}
    """).format(
        table=_build_table_identifier(session_table),
        user_filter=user_filter,
        column=column_sql,
        direction=direction_sql,
        page=page_clause,
    )
    return query, tuple(params)


def _resolve_retry_config(config: Any) -> "tuple[CockroachPsycopgRetryConfig, bool]":
    driver_features = getattr(config, "driver_features", None)
    if isinstance(driver_features, Mapping):
        return (
            CockroachPsycopgRetryConfig.from_features(driver_features),
            bool(driver_features.get("enable_auto_retry", True)),
        )
    return CockroachPsycopgRetryConfig(), True


async def _run_transaction_with_retry_async(
    config: Any, conn: Any, operation: "Callable[[Any], Awaitable[StoredSession]]"
) -> StoredSession:
    retry_config, enable_retry = _resolve_retry_config(config)
    attempt = 0
    while True:
        tx_factory = getattr(conn, "transaction", None)
        try:
            if callable(tx_factory) and getattr(conn, "autocommit", False):
                tx_ctx: Any = tx_factory()
                async with tx_ctx, conn.cursor(row_factory=dict_row) as cur:
                    return await operation(cur)
            async with conn.cursor(row_factory=dict_row) as cur:
                try:
                    result = await operation(cur)
                    await conn.commit()
                except Exception as inner_exc:
                    rollback: Any = getattr(conn, "rollback", None)
                    if rollback is not None:
                        try:
                            await rollback()
                        except Exception:
                            raise inner_exc from None
                    raise
                else:
                    return result
        except Exception as exc:
            if not enable_retry or not is_retryable_error(exc) or attempt >= retry_config.max_retries:
                raise
            delay = calculate_backoff_seconds(attempt, retry_config)
            if retry_config.enable_logging:
                logger.debug("CockroachDB ADK retry %s/%s after %.3fs", attempt + 1, retry_config.max_retries, delay)
            await asyncio.sleep(delay)
            attempt += 1


def _run_transaction_with_retry_sync(
    config: Any, conn: Any, operation: "Callable[[Any], StoredSession]"
) -> StoredSession:
    retry_config, enable_retry = _resolve_retry_config(config)
    attempt = 0
    while True:
        tx_factory = getattr(conn, "transaction", None)
        try:
            if callable(tx_factory) and getattr(conn, "autocommit", False):
                tx_ctx: Any = tx_factory()
                with tx_ctx, conn.cursor(row_factory=dict_row) as cur:
                    return operation(cur)
            with conn.cursor(row_factory=dict_row) as cur:
                try:
                    result = operation(cur)
                    conn.commit()
                except Exception as inner_exc:
                    rollback = getattr(conn, "rollback", None)
                    if callable(rollback):
                        try:
                            rollback()
                        except Exception:
                            raise inner_exc from None
                    raise
                else:
                    return result
        except Exception as exc:
            if not enable_retry or not is_retryable_error(exc) or attempt >= retry_config.max_retries:
                raise
            delay = calculate_backoff_seconds(attempt, retry_config)
            if retry_config.enable_logging:
                logger.debug("CockroachDB ADK retry %s/%s after %.3fs", attempt + 1, retry_config.max_retries, delay)
            time.sleep(delay)
            attempt += 1


def _cockroach_scoped_state_queries(
    table: str, key_columns: "tuple[str, ...]"
) -> "tuple[pg_sql.Composed, pg_sql.Composed, pg_sql.Composed, pg_sql.Composed]":
    """Build parameterized CockroachDB queries for reading, ensuring, locking, and updating scoped state."""
    table_ident = _build_table_identifier(table)
    where_clause = pg_sql.SQL(" AND ").join(
        pg_sql.SQL("{col} = %s").format(col=pg_sql.Identifier(col)) for col in key_columns
    )
    cols = pg_sql.SQL(", ").join([
        *(pg_sql.Identifier(col) for col in key_columns),
        pg_sql.Identifier("state"),
        pg_sql.Identifier("update_time"),
    ])
    placeholders = pg_sql.SQL(", ").join(pg_sql.SQL("%s") for _ in range(len(key_columns) + 1))
    conflict_cols = pg_sql.SQL(", ").join(pg_sql.Identifier(col) for col in key_columns)

    select_query = pg_sql.SQL("SELECT state FROM {table} WHERE {where_clause}").format(
        table=table_ident, where_clause=where_clause
    )
    ensure_query = pg_sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({placeholders}, CURRENT_TIMESTAMP) "
        "ON CONFLICT ({conflict_cols}) DO NOTHING"
    ).format(table=table_ident, cols=cols, placeholders=placeholders, conflict_cols=conflict_cols)
    lock_query = pg_sql.SQL("SELECT state FROM {table} WHERE {where_clause} FOR UPDATE").format(
        table=table_ident, where_clause=where_clause
    )
    update_query = pg_sql.SQL(
        "UPDATE {table} SET state = %s, update_time = CURRENT_TIMESTAMP WHERE {where_clause}"
    ).format(table=table_ident, where_clause=where_clause)
    return select_query, ensure_query, lock_query, update_query


def _decode_json_state(raw: Any) -> "dict[str, Any]":
    """Normalize a JSONB state value from CockroachDB psycopg into a dictionary."""
    if isinstance(raw, str):
        decoded = from_json(raw)
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}
