"""Psycopg ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING, Any

from psycopg import errors
from psycopg import sql as pg_sql
from psycopg.types.json import Jsonb

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("PsycopgAsyncADKMemoryStore", "PsycopgAsyncADKStore", "PsycopgSyncADKMemoryStore", "PsycopgSyncADKStore")

logger = get_logger("sqlspec.adapters.psycopg.adk.store")


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


class PsycopgAsyncADKStore(BaseAsyncADKStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK store using Psycopg3 async driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via psycopg3 with native async/await support.
    Events are stored as a single JSONB blob (``event_json``) alongside
    indexed scalar columns for efficient querying.

    Provides:
    - Session state management with JSONB storage
    - Full-fidelity event storage via ``event_json`` JSONB column
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

    async def _get_create_sessions_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    async def create_tables(self) -> None:
        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_sessions_table_sql())
            await driver.execute_script(await self._get_create_events_table_sql())

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

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, params)

        return await self.get_session(session_id)  # type: ignore[return-value]

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        query = pg_sql.SQL("""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {table}
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(query, (session_id,))
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

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, (Jsonb(state), session_id))

    async def delete_session(self, session_id: str) -> None:
        query = pg_sql.SQL("DELETE FROM {table} WHERE id = %s").format(table=pg_sql.Identifier(self._session_table))

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(query, (session_id,))

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
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
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

    async def append_event(self, event_record: EventRecord) -> None:
        query = pg_sql.SQL("""
        INSERT INTO {table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        event_json_value = event_record["event_json"]
        jsonb_value = Jsonb(event_json_value) if isinstance(event_json_value, dict) else event_json_value

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                query,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        event_json_value = event_record["event_json"]
        jsonb_value = Jsonb(event_json_value) if isinstance(event_json_value, dict) else event_json_value

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                insert_query,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            await cur.execute(update_query, (Jsonb(state), session_id))
            await conn.commit()

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        where_clauses = ["session_id = %s"]
        params: list[Any] = [session_id]

        if after_timestamp is not None:
            where_clauses.append("timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        if limit:
            params.append(limit)

        query = pg_sql.SQL(
            """
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {table}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """
        ).format(
            table=pg_sql.Identifier(self._events_table),
            where_clause=pg_sql.SQL(where_clause),  # pyright: ignore[reportArgumentType]
            limit_clause=pg_sql.SQL(" LIMIT %s" if limit else ""),  # pyright: ignore[reportArgumentType]
        )

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(query, tuple(params))
                rows = await cur.fetchall()

                return [
                    EventRecord(
                        session_id=row["session_id"],
                        invocation_id=row["invocation_id"],
                        author=row["author"],
                        timestamp=row["timestamp"],
                        event_json=row["event_json"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []


class PsycopgSyncADKStore(BaseAsyncADKStore["PsycopgSyncConfig"]):
    """PostgreSQL synchronous ADK store using Psycopg3 driver.

    Implements session and event storage for Google Agent Development Kit
    using PostgreSQL via psycopg3 with synchronous execution.
    Events are stored as a single JSONB blob (``event_json``) alongside
    indexed scalar columns for efficient querying.

    Provides:
    - Session state management with JSONB storage
    - Full-fidelity event storage via ``event_json`` JSONB column
    - Atomic ``create_event_and_update_state`` for durable session mutations
    - Microsecond-precision timestamps with TIMESTAMPTZ
    - Foreign key constraints with cascade delete
    - GIN indexes for JSONB queries
    - HOT updates with FILLFACTOR 80

    Args:
        config: PsycopgSyncConfig with extension_config["adk"] settings.
    """

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        super().__init__(config)

    async def _get_create_sessions_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_json JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def _create_tables(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_sessions_table_sql)())
            driver.execute_script(run_(self._get_create_events_table_sql)())

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _create_session(
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

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)

        result = self._get_session(session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        query = pg_sql.SQL("""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {table}
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(query, (session_id,))
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

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID."""
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (Jsonb(state), session_id))

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await async_(self._update_session_state)(session_id, state)

    def _delete_session(self, session_id: str) -> None:
        query = pg_sql.SQL("DELETE FROM {table} WHERE id = %s").format(table=pg_sql.Identifier(self._session_table))

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (session_id,))

    async def delete_session(self, session_id: str) -> None:
        """Delete session and associated events."""
        await async_(self._delete_session)(session_id)

    def _list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
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
            with self._config.provide_connection() as conn, conn.cursor() as cur:
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

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        return await async_(self._list_sessions)(app_name, user_id)

    def _append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        insert_query = pg_sql.SQL("""
        INSERT INTO {table} (
            session_id, invocation_id, author, timestamp, event_json
        ) VALUES (%s, %s, %s, %s, %s)
        """).format(table=pg_sql.Identifier(self._events_table))

        update_query = pg_sql.SQL("""
        UPDATE {table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """).format(table=pg_sql.Identifier(self._session_table))

        event_json_value = event_record["event_json"]
        jsonb_value = Jsonb(event_json_value) if isinstance(event_json_value, dict) else event_json_value

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(
                insert_query,
                (
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            cur.execute(update_query, (Jsonb(state), session_id))
            conn.commit()

    async def append_event_and_update_state(
        self, event_record: EventRecord, session_id: str, state: "dict[str, Any]"
    ) -> None:
        """Atomically append an event and update the session's durable state."""
        await async_(self._append_event_and_update_state)(event_record, session_id, state)

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        query = pg_sql.SQL("""
        SELECT session_id, invocation_id, author, timestamp, event_json
        FROM {table}
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """).format(table=pg_sql.Identifier(self._events_table))

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(query, (session_id,))
                rows = cur.fetchall()

                return [
                    EventRecord(
                        session_id=row["session_id"],
                        invocation_id=row["invocation_id"],
                        author=row["author"],
                        timestamp=row["timestamp"],
                        event_json=row["event_json"],
                    )
                    for row in rows
                ]
        except errors.UndefinedTable:
            return []

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session."""
        return await async_(self._get_events)(session_id, after_timestamp, limit)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._append_event_and_update_state(event_record, event_record["session_id"], {})

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
        await async_(self._append_event)(event_record)


class PsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["PsycopgAsyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 async driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgAsyncConfig") -> None:
        """Initialize Psycopg async memory store."""
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

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
                except Exception as exc:  # pragma: no cover - defensive fallback
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return await self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

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


class PsycopgSyncADKMemoryStore(BaseAsyncADKMemoryStore["PsycopgSyncConfig"]):
    """PostgreSQL ADK memory store using Psycopg3 sync driver."""

    __slots__ = ()

    def __init__(self, config: "PsycopgSyncConfig") -> None:
        """Initialize Psycopg sync memory store."""
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        """Get PostgreSQL CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_line},
            timestamp TIMESTAMPTZ NOT NULL,
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC);

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get PostgreSQL DROP TABLE SQL statements."""
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def _create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_memory_table_sql)())

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        await async_(self._create_tables)()

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return await async_(self._insert_memory_entries)(entries, owner_id)

    def _search_entries(
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
                    return self._search_entries_fts(query, app_name, user_id, effective_limit)
                except Exception as exc:  # pragma: no cover - defensive fallback
                    logger.warning("FTS search failed; falling back to simple search: %s", exc)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except errors.UndefinedTable:
            return []

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return await async_(self._search_entries)(query, app_name, user_id, limit)

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

    def _delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        sql = pg_sql.SQL("DELETE FROM {table} WHERE session_id = %s").format(
            table=pg_sql.Identifier(self._memory_table)
        )

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (session_id,))
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

    def _delete_entries_older_than(self, days: int) -> int:
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

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)


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
