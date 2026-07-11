"""Psycopg ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any, NoReturn, cast

from psycopg import errors
from psycopg import sql as pg_sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from typing_extensions import NotRequired

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = (
    "PsycopgADKConfig",
    "PsycopgAsyncADKMemoryStore",
    "PsycopgAsyncADKStore",
    "PsycopgSyncADKMemoryStore",
    "PsycopgSyncADKStore",
)

logger = get_logger("sqlspec.adapters.psycopg.adk.store")


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
    "        ) WITH (fillfactor = 80);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{2}_app_user\n"
    "            ON {3}(app_name, user_id);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{4}_update_time\n"
    "            ON {5}(update_time DESC);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{6}_state\n"
    "            ON {7} USING GIN (state)\n"
    "            WHERE state != '{{}}'::jsonb;\n"
    "        "
)

_ADK_EVENTS_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) PRIMARY KEY,\n"
    "            session_id VARCHAR(128) NOT NULL,\n"
    "            invocation_id VARCHAR(256),\n"
    "            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
    "            event_data JSONB NOT NULL{1},\n"
    "            FOREIGN KEY (session_id) REFERENCES {2}(id) ON DELETE CASCADE\n"
    "        ) WITH (fillfactor = 80);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{3}_session\n"
    "            ON {4}(session_id, timestamp ASC){5};\n"
    "        {6}\n"
    "        "
)

_ADK_APP_STATES_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            app_name VARCHAR(128) PRIMARY KEY,\n"
    "            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,\n"
    "            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
    "        ) WITH (fillfactor = 80);\n"
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
    "        ) WITH (fillfactor = 80);\n"
    "        "
)

_ADK_METADATA_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            key VARCHAR(128) PRIMARY KEY,\n"
    "            value VARCHAR(512) NOT NULL\n"
    "        );\n"
    "        "
)

_ADK_METADATA_SEED_SQL_TEMPLATE = (
    "\n"
    "        INSERT INTO {0} (key, value)\n"
    "        VALUES ('schema_version', '1')\n"
    "        ON CONFLICT (key) DO NOTHING\n"
    "        "
)

_ADK_MEMORY_TABLE_DDL_TEMPLATE = (
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{0}_fts\n"
    "            ON {1} USING GIN (to_tsvector('english', content_text));\n"
    "            "
)

_ADK_MEMORY_TABLE_DDL_TEMPLATE_2 = (
    "\n"
    "        CREATE TABLE IF NOT EXISTS {0} (\n"
    "            id VARCHAR(128) PRIMARY KEY,\n"
    "            session_id VARCHAR(128) NOT NULL,\n"
    "            app_name VARCHAR(128) NOT NULL,\n"
    "            user_id VARCHAR(128) NOT NULL,\n"
    "            event_id VARCHAR(128) NOT NULL UNIQUE,\n"
    "            author VARCHAR(256){1},\n"
    "            timestamp TIMESTAMPTZ NOT NULL,\n"
    "            content_json JSONB NOT NULL,\n"
    "            content_text TEXT NOT NULL,\n"
    "            metadata_json JSONB,\n"
    "            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
    "        );\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{2}_app_user_time\n"
    "            ON {3}(app_name, user_id, timestamp DESC);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{4}_session\n"
    "            ON {5}(session_id);\n"
    "        {6}\n"
    "        "
)

_ADK_POSTGRES_EVENT_DDL_OPTIONS_TEMPLATE = (
    "\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{0}_author_gc\n"
    "            ON {1}(session_id, author_gc, timestamp ASC);\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{2}_node_path_gc\n"
    "            ON {3}(session_id, node_path_gc, timestamp ASC);\n"
    "        "
)


class PsycopgADKConfig(ADKConfig):
    """Psycopg-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the psycopg ADK stores.
    """

    enable_event_generated_columns: NotRequired[bool]
    """Create PostgreSQL generated columns and indexes for common ADK event JSON paths."""

    enable_covering_indexes: NotRequired[bool]
    """Add PostgreSQL INCLUDE columns to ADK event replay indexes."""


class PsycopgAsyncADKStore(BaseAsyncADKStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK store using Psycopg3 async driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via psycopg3 with native async/await support.
    Events are stored as a single JSONB blob (``event_data``) alongside
    indexed scalar columns for efficient querying.

    Provides:
        - Session state management with JSONB storage
        - Full-fidelity event storage via ``event_data`` JSONB column
        - Atomic ``append_event_and_update_state`` for durable session mutations
        - Microsecond-precision timestamps with TIMESTAMPTZ
        - Foreign key constraints with cascade delete
        - GIN indexes for JSONB queries
        - HOT updates with FILLFACTOR 80

    Args:
        config: PsycopgAsyncConfig with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        super().__init__(config)

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._sessions_table_ddl())
            await driver.execute_script(await self._events_table_ddl())
            await driver.execute_script(await self._app_states_table_ddl())
            await driver.execute_script(await self._user_states_table_ddl())
            await driver.execute_script(await self._metadata_table_ddl())
            await driver.execute_script(await self._metadata_seed_sql())

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(
                table=pg_sql.Identifier(self._session_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
            params = (session_id, app_name, user_id, owner_id, Jsonb(state))
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (session_id, app_name, user_id, Jsonb(state))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)

        result = await self.get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            query = pg_sql.SQL("""
            UPDATE {table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id, session_id)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id, session_id)

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()

                if row is None:
                    return None

                return SessionRecord(
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
        """).format(table=pg_sql.Identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (Jsonb(state), app_name, user_id, session_id))

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        if user_id is None:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params: tuple[str, ...] = (app_name,)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()

                return [
                    SessionRecord(
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

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        query = pg_sql.SQL("DELETE FROM {table} WHERE app_name = %s AND user_id = %s AND id = %s").format(
            table=pg_sql.Identifier(self._session_table)
        )

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, user_id, session_id))

    async def append_event(self, event_record: EventRecord) -> None:
        query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )

    async def append_event_and_update_state(
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
        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """).format(table=pg_sql.Identifier(self._session_table))

        app_upsert_query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._app_state_table))

        user_upsert_query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._user_state_table))

        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            try:
                await cur.execute(
                    insert_query,
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        jsonb_value,
                    ),
                )
                await cur.execute(update_query, (Jsonb(state), app_name, user_id, session_id))
                row = await cur.fetchone()
                if row is None:
                    _raise_missing_session(session_id)
                if app_state is not None:
                    await cur.execute(app_upsert_query, (app_name, Jsonb(app_state)))
                if user_state is not None:
                    await cur.execute(user_upsert_query, (app_name, user_id, Jsonb(user_state)))
            except Exception:
                await conn.rollback()
                raise
            await conn.commit()

        return SessionRecord(
            id=row["id"],
            app_name=row["app_name"],
            user_id=row["user_id"],
            state=row["state"],
            create_time=row["create_time"],
            update_time=row["update_time"],
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        if limit == 0:
            return []

        where_clauses = [pg_sql.SQL("s.app_name = %s"), pg_sql.SQL("s.user_id = %s"), pg_sql.SQL("e.session_id = %s")]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(pg_sql.SQL("e.timestamp > %s"))
            params.append(after_timestamp)

        where_clause = pg_sql.SQL(" AND ").join(where_clauses)
        if limit is not None:
            params.append(limit)

        query = pg_sql.SQL(
            """
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {events_table} e
        JOIN {session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """
        ).format(
            events_table=pg_sql.Identifier(self._events_table),
            session_table=pg_sql.Identifier(self._session_table),
            where_clause=where_clause,
            limit_clause=pg_sql.SQL(" LIMIT %s" if limit is not None else ""),
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, tuple(params))
                rows = await cur.fetchall()

                return [
                    EventRecord(
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

    async def delete_expired_events(self, before: "datetime") -> int:
        query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s").format(
            table=pg_sql.Identifier(self._events_table)
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (before,))
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
            table=pg_sql.Identifier(self._session_table)
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query, (updated_before,))
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s").format(
            table=pg_sql.Identifier(self._app_state_table)
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
            table=pg_sql.Identifier(self._user_state_table)
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
        INSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._app_state_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, Jsonb(state)))

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._user_state_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (app_name, user_id, Jsonb(state)))

    async def get_metadata(self, key: str) -> "str | None":
        query = pg_sql.SQL("SELECT value FROM {table} WHERE key = %s").format(
            table=pg_sql.Identifier(self._metadata_table)
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
        INSERT INTO {table} (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """).format(table=pg_sql.Identifier(self._metadata_table))

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (key, value))

    async def _sessions_table_ddl(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            self._session_table,
            owner_id_line,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
        )

    async def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        generated_columns, generated_indexes, covering_columns = _postgres_event_ddl_options(
            adk_config, self._events_table
        )

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            self._events_table,
            generated_columns,
            self._session_table,
            self._events_table,
            self._events_table,
            covering_columns,
            generated_indexes,
        )

    async def _app_states_table_ddl(self) -> str:
        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(self._app_state_table)

    async def _user_states_table_ddl(self) -> str:
        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(self._user_state_table)

    async def _metadata_table_ddl(self) -> str:
        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(self._metadata_table)

    async def _metadata_seed_sql(self) -> str:
        return _ADK_METADATA_SEED_SQL_TEMPLATE.format(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class PsycopgSyncADKStore(BaseSyncADKStore["PsycopgSyncConfig"]):
    """PostgreSQL synchronous ADK store using Psycopg3 driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        with self._config.provide_session() as driver:
            driver.execute_script(self._sessions_table_ddl())
            driver.execute_script(self._events_table_ddl())
            driver.execute_script(self._app_states_table_ddl())
            driver.execute_script(self._user_states_table_ddl())
            driver.execute_script(self._metadata_table_ddl())
            driver.execute_script(self._metadata_seed_sql())

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, {owner_id_col}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(
                table=pg_sql.Identifier(self._session_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
            params = (session_id, app_name, user_id, owner_id, Jsonb(state))
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (session_id, app_name, user_id, Jsonb(state))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)

        result = self.get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID."""
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            query = pg_sql.SQL("""
            UPDATE {table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id, session_id)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id, session_id)

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                row = cur.fetchone()

                if row is None:
                    return None

                return SessionRecord(
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
        """).format(table=pg_sql.Identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (Jsonb(state), app_name, user_id, session_id))

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        if user_id is None:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params: tuple[str, ...] = (app_name,)
        else:
            query = pg_sql.SQL("""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """).format(table=pg_sql.Identifier(self._session_table))
            params = (app_name, user_id)

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

                return [
                    SessionRecord(
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
            table=pg_sql.Identifier(self._session_table)
        )

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, user_id, session_id))

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)

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
        """Atomically append an event and update session + scoped state."""
        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """).format(table=pg_sql.Identifier(self._session_table))

        app_upsert_query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._app_state_table))

        user_upsert_query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._user_state_table))

        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            try:
                cur.execute(
                    insert_query,
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        jsonb_value,
                    ),
                )
                cur.execute(update_query, (Jsonb(state), app_name, user_id, session_id))
                row = cur.fetchone()
                if row is None:
                    _raise_missing_session(session_id)
                if app_state is not None:
                    cur.execute(app_upsert_query, (app_name, Jsonb(app_state)))
                if user_state is not None:
                    cur.execute(user_upsert_query, (app_name, user_id, Jsonb(user_state)))
            except Exception:
                conn.rollback()
                raise
            conn.commit()

        return SessionRecord(
            id=row["id"],
            app_name=row["app_name"],
            user_id=row["user_id"],
            state=row["state"],
            create_time=row["create_time"],
            update_time=row["update_time"],
        )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        """Get events for a session."""
        if limit == 0:
            return []

        where_clauses = [pg_sql.SQL("s.app_name = %s"), pg_sql.SQL("s.user_id = %s"), pg_sql.SQL("e.session_id = %s")]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(pg_sql.SQL("e.timestamp > %s"))
            params.append(after_timestamp)

        where_clause = pg_sql.SQL(" AND ").join(where_clauses)
        if limit is not None:
            params.append(limit)

        query = pg_sql.SQL(
            """
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {events_table} e
        JOIN {session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """
        ).format(
            events_table=pg_sql.Identifier(self._events_table),
            session_table=pg_sql.Identifier(self._session_table),
            where_clause=where_clause,
            limit_clause=pg_sql.SQL(" LIMIT %s" if limit is not None else ""),
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall()

                return [
                    EventRecord(
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

    def delete_expired_events(self, before: "datetime") -> int:
        """Delete events older than the given timestamp."""
        query = pg_sql.SQL("DELETE FROM {table} WHERE timestamp < %s").format(
            table=pg_sql.Identifier(self._events_table)
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (before,))
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def delete_idle_sessions(self, updated_before: "datetime") -> int:
        """Delete sessions whose update_time predates the given threshold."""
        query = pg_sql.SQL("DELETE FROM {table} WHERE update_time < %s").format(
            table=pg_sql.Identifier(self._session_table)
        )

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (updated_before,))
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        query = pg_sql.SQL("SELECT state FROM {table} WHERE app_name = %s").format(
            table=pg_sql.Identifier(self._app_state_table)
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
            table=pg_sql.Identifier(self._user_state_table)
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
        INSERT INTO {table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._app_state_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, Jsonb(state)))
            conn.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        query = pg_sql.SQL("""
        INSERT INTO {table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (app_name, user_id) DO UPDATE SET
            state = EXCLUDED.state,
            update_time = CURRENT_TIMESTAMP
        """).format(table=pg_sql.Identifier(self._user_state_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (app_name, user_id, Jsonb(state)))
            conn.commit()

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        query = pg_sql.SQL("SELECT value FROM {table} WHERE key = %s").format(
            table=pg_sql.Identifier(self._metadata_table)
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
        INSERT INTO {table} (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """).format(table=pg_sql.Identifier(self._metadata_table))

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (key, value))
            conn.commit()

    def _sessions_table_ddl(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            self._session_table,
            owner_id_line,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
            self._session_table,
        )

    def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        generated_columns, generated_indexes, covering_columns = _postgres_event_ddl_options(
            adk_config, self._events_table
        )

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            self._events_table,
            generated_columns,
            self._session_table,
            self._events_table,
            self._events_table,
            covering_columns,
            generated_indexes,
        )

    def _app_states_table_ddl(self) -> str:
        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(self._app_state_table)

    def _user_states_table_ddl(self) -> str:
        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(self._user_state_table)

    def _metadata_table_ddl(self) -> str:
        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(self._metadata_table)

    def _metadata_seed_sql(self) -> str:
        return _ADK_METADATA_SEED_SQL_TEMPLATE.format(self._metadata_table)

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]

    def _insert_event(self, event_record: EventRecord) -> None:
        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                insert_query,
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            conn.commit()


class PsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 async driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg async memory store."""
        super().__init__(config)

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(
                table=pg_sql.Identifier(self._memory_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(table=pg_sql.Identifier(self._memory_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            for entry in entries:
                if self._owner_id_column_name:
                    await cur.execute(query, _build_insert_params_with_owner(entry, owner_id))
                else:
                    await cur.execute(query, _build_insert_params(entry))
                if cur.rowcount and cur.rowcount > 0:
                    inserted_count += cur.rowcount

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                try:
                    return await self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return await self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        sql = pg_sql.SQL("DELETE FROM {table} WHERE session_id = %s").format(
            table=pg_sql.Identifier(self._memory_table)
        )

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, (session_id,))
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        sql = pg_sql.SQL(
            """
        DELETE FROM {table}
        WHERE inserted_at < CURRENT_TIMESTAMP - {interval}::interval
        """
        ).format(table=pg_sql.Identifier(self._memory_table), interval=pg_sql.Literal(f"{days} days"))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def _memory_table_ddl(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)

        fts_index = ""
        if self._use_fts:
            fts_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(self._memory_table, self._memory_table)

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE_2.format(
            self._memory_table,
            owner_id_line,
            self._memory_table,
            self._memory_table,
            self._memory_table,
            self._memory_table,
            fts_index,
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
            SELECT id, session_id, app_name, user_id, event_id, author,
             timestamp, content_json, content_text, metadata_json, inserted_at,
             ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', %s)) as rank
            FROM {table}
            WHERE app_name = %s
             AND user_id = %s
             AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY rank DESC, timestamp DESC
            LIMIT %s
            """
        ).format(table=pg_sql.Identifier(self._memory_table))
        params: tuple[str, str, str, str, int] = (query, app_name, user_id, query, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return _rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
            SELECT id, session_id, app_name, user_id, event_id, author,
             timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {table}
            WHERE app_name = %s
             AND user_id = %s
             AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
        ).format(table=pg_sql.Identifier(self._memory_table))
        pattern = f"%{query}%"
        params: tuple[str, str, str, int] = (app_name, user_id, pattern, limit)
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return _rows_to_records(rows)


class PsycopgSyncADKMemoryStore(BaseSyncADKMemoryStore["PsycopgSyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 sync driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg sync memory store."""
        super().__init__(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        if self._owner_id_column_name:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                {owner_id_col}, timestamp, content_json, content_text,
                metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(
                table=pg_sql.Identifier(self._memory_table), owner_id_col=pg_sql.Identifier(self._owner_id_column_name)
            )
        else:
            query = pg_sql.SQL("""
            INSERT INTO {table} (
                id, session_id, app_name, user_id, event_id, author,
                timestamp, content_json, content_text, metadata_json, inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
            """).format(table=pg_sql.Identifier(self._memory_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            for entry in entries:
                if self._owner_id_column_name:
                    cur.execute(query, _build_insert_params_with_owner(entry, owner_id))
                else:
                    cur.execute(query, _build_insert_params(entry))
                if cur.rowcount and cur.rowcount > 0:
                    inserted_count += cur.rowcount

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                try:
                    return self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        """Delete all memory entries for a specific session."""
        sql = pg_sql.SQL("DELETE FROM {table} WHERE session_id = %s").format(
            table=pg_sql.Identifier(self._memory_table)
        )

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (session_id,))
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        """Delete memory entries older than specified days."""
        sql = pg_sql.SQL(
            """
        DELETE FROM {table}
        WHERE inserted_at < CURRENT_TIMESTAMP - {interval}::interval
        """
        ).format(table=pg_sql.Identifier(self._memory_table), interval=pg_sql.Literal(f"{days} days"))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def _memory_table_ddl(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)

        fts_index = ""
        if self._use_fts:
            fts_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(self._memory_table, self._memory_table)

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE_2.format(
            self._memory_table,
            owner_id_line,
            self._memory_table,
            self._memory_table,
            self._memory_table,
            self._memory_table,
            fts_index,
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
            SELECT id, session_id, app_name, user_id, event_id, author,
             timestamp, content_json, content_text, metadata_json, inserted_at,
             ts_rank(to_tsvector('english', content_text), plainto_tsquery('english', %s)) as rank
            FROM {table}
            WHERE app_name = %s
             AND user_id = %s
             AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY rank DESC, timestamp DESC
            LIMIT %s
            """
        ).format(table=pg_sql.Identifier(self._memory_table))
        params: tuple[str, str, str, str, int] = (query, app_name, user_id, query, limit)
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return _rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = pg_sql.SQL(
            """
            SELECT id, session_id, app_name, user_id, event_id, author,
             timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {table}
            WHERE app_name = %s
             AND user_id = %s
             AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """
        ).format(table=pg_sql.Identifier(self._memory_table))
        pattern = f"%{query}%"
        params: tuple[str, str, str, int] = (app_name, user_id, pattern, limit)
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return _rows_to_records(rows)


def _build_insert_params(entry: "MemoryRecord") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry["event_id"],
        entry["author"],
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


def _build_insert_params_with_owner(entry: "MemoryRecord", owner_id: "object | None") -> "tuple[object, ...]":
    return (
        entry["id"],
        entry["session_id"],
        entry["app_name"],
        entry["user_id"],
        entry["event_id"],
        entry["author"],
        owner_id,
        entry["timestamp"],
        Jsonb(entry["content_json"]),
        entry["content_text"],
        Jsonb(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
        entry["inserted_at"],
    )


def _rows_to_records(rows: "list[Any]") -> "list[MemoryRecord]":
    return [
        {
            "id": row["id"],
            "session_id": row["session_id"],
            "app_name": row["app_name"],
            "user_id": row["user_id"],
            "event_id": row["event_id"],
            "author": row["author"],
            "timestamp": row["timestamp"],
            "content_json": row["content_json"],
            "content_text": row["content_text"],
            "metadata_json": row["metadata_json"],
            "inserted_at": row["inserted_at"],
        }
        for row in rows
    ]


def _adk_config(config: Any) -> PsycopgADKConfig:
    """Return psycopg ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("PsycopgADKConfig", adk_config)


def _postgres_event_ddl_options(adk_config: PsycopgADKConfig, events_table: str) -> "tuple[str, str, str]":
    generated_columns = ""
    generated_indexes = ""
    if adk_config.get("enable_event_generated_columns", False):
        generated_columns = """,
            author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED,
            node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED"""
        generated_indexes = _ADK_POSTGRES_EVENT_DDL_OPTIONS_TEMPLATE.format(
            events_table, events_table, events_table, events_table
        )

    covering_columns = ""
    if adk_config.get("enable_covering_indexes", False):
        covering_columns = " INCLUDE (invocation_id)"

    return generated_columns, generated_indexes, covering_columns


def _raise_missing_session(session_id: str) -> NoReturn:
    msg = f"Session {session_id} not found during append_event_and_update_state."
    raise ValueError(msg)
