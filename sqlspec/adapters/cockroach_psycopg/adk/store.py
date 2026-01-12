"""CockroachDB ADK store for Google Agent Development Kit session/event storage (psycopg)."""

from typing import TYPE_CHECKING, Any, cast

from psycopg import errors
from psycopg import sql as pg_sql
from psycopg.types.json import Jsonb

from sqlspec.extensions.adk import BaseAsyncADKStore, BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = (
    "CockroachPsycopgAsyncADKMemoryStore",
    "CockroachPsycopgAsyncADKStore",
    "CockroachPsycopgSyncADKMemoryStore",
    "CockroachPsycopgSyncADKStore",
)

logger = get_logger("sqlspec.adapters.cockroach_psycopg.adk.store")


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


class CockroachPsycopgAsyncADKStore(BaseAsyncADKStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB ADK store using psycopg async driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)

    async def _get_create_sessions_table_sql(self) -> str:
        """Get CockroachDB CREATE TABLE SQL for sessions."""
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
        );

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    async def _get_create_events_table_sql(self) -> str:
        """Get CockroachDB CREATE TABLE SQL for events."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            actions BYTEA NOT NULL,
            long_running_tool_ids_json JSONB,
            branch VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSONB,
            grounding_metadata JSONB,
            custom_metadata JSONB,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );

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
        state_json = Jsonb(state)

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state_json)

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), params)
            await conn.commit()

        result = await self.get_session(session_id)
        if result is None:
            msg = "Session creation failed"
            raise RuntimeError(msg)
        return result

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(sql.encode(), (session_id,))
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
        sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (Jsonb(state), session_id))
            await conn.commit()

    async def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (session_id,))
            await conn.commit()

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params: tuple[str, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(sql.encode(), params)
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
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                sql.encode(),
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["actions"],
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    event_record["timestamp"],
                    event_record.get("content"),
                    event_record.get("grounding_metadata"),
                    event_record.get("custom_metadata"),
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ),
            )
            await conn.commit()

    async def list_events(self, session_id: str) -> "list[EventRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(sql.encode(), (session_id,))
                rows = await cur.fetchall()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(row["actions"]),
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=row["timestamp"],
                    content=row["content"],
                    grounding_metadata=row["grounding_metadata"],
                    custom_metadata=row["custom_metadata"],
                    partial=row["partial"],
                    turn_complete=row["turn_complete"],
                    interrupted=row["interrupted"],
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]
        except errors.UndefinedTable:
            return []


class CockroachPsycopgSyncADKStore(BaseSyncADKStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK store using psycopg sync driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    def _get_create_sessions_table_sql(self) -> str:
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
        );

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id);

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC);
        """

    def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256) NOT NULL,
            author VARCHAR(256) NOT NULL,
            actions BYTEA NOT NULL,
            long_running_tool_ids_json JSONB,
            branch VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            content JSONB,
            grounding_metadata JSONB,
            custom_metadata JSONB,
            partial BOOLEAN,
            turn_complete BOOLEAN,
            interrupted BOOLEAN,
            error_code VARCHAR(256),
            error_message VARCHAR(1024),
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC);
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._events_table}", f"DROP TABLE IF EXISTS {self._session_table}"]

    def create_tables(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_sessions_table_sql())
            driver.execute_script(self._get_create_events_table_sql())

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        state_json = Jsonb(state)

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state_json)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state_json)

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), params)
            conn.commit()

        result = self.get_session(session_id)
        if result is None:
            msg = "Session creation failed"
            raise RuntimeError(msg)
        return result

    def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE id = %s
        """

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(sql.encode(), (session_id,))
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

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE id = %s
        """

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (Jsonb(state), session_id))
            conn.commit()

    def delete_session(self, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE id = %s"

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (session_id,))
            conn.commit()

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s
            ORDER BY update_time DESC
            """
            params: tuple[str, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(sql.encode(), params)
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

    def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(
                sql.encode(),
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["app_name"],
                    event_record["user_id"],
                    event_record["invocation_id"],
                    event_record["author"],
                    event_record["actions"],
                    event_record.get("long_running_tool_ids_json"),
                    event_record.get("branch"),
                    event_record["timestamp"],
                    event_record.get("content"),
                    event_record.get("grounding_metadata"),
                    event_record.get("custom_metadata"),
                    event_record.get("partial"),
                    event_record.get("turn_complete"),
                    event_record.get("interrupted"),
                    event_record.get("error_code"),
                    event_record.get("error_message"),
                ),
            )
            conn.commit()

    def list_events(self, session_id: str) -> "list[EventRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp, content,
               grounding_metadata, custom_metadata, partial, turn_complete,
               interrupted, error_code, error_message
        FROM {self._events_table}
        WHERE session_id = %s
        ORDER BY timestamp ASC
        """

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(sql.encode(), (session_id,))
                rows = cur.fetchall()

            return [
                EventRecord(
                    id=row["id"],
                    session_id=row["session_id"],
                    app_name=row["app_name"],
                    user_id=row["user_id"],
                    invocation_id=row["invocation_id"],
                    author=row["author"],
                    actions=bytes(row["actions"]),
                    long_running_tool_ids_json=row["long_running_tool_ids_json"],
                    branch=row["branch"],
                    timestamp=row["timestamp"],
                    content=row["content"],
                    grounding_metadata=row["grounding_metadata"],
                    custom_metadata=row["custom_metadata"],
                    partial=row["partial"],
                    turn_complete=row["turn_complete"],
                    interrupted=row["interrupted"],
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
                for row in rows
            ]
        except errors.UndefinedTable:
            return []


class CockroachPsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB ADK memory store using psycopg async driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)

    async def _get_create_memory_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

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
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    async def create_tables(self) -> None:
        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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
            await conn.commit()

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        effective_limit = limit if limit is not None else self._max_results
        if self._use_fts:
            logger.debug("CockroachDB full-text search not supported; using simple search")

        sql = f"""
        SELECT * FROM {self._memory_table}
        WHERE app_name = %s AND user_id = %s AND content_text ILIKE %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        params = (app_name, user_id, f"%{query}%", effective_limit)

        try:
            async with self._config.provide_connection() as conn, conn.cursor() as cur:
                await cur.execute(sql.encode(), params)
                rows = await cur.fetchall()
                columns = [col[0] for col in cur.description or []]
        except errors.UndefinedTable:
            return []

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (session_id,))
            await conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - INTERVAL %s DAY)
        """
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode(), (days,))
            await conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


class CockroachPsycopgSyncADKMemoryStore(BaseSyncADKMemoryStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK memory store using psycopg sync driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    def _get_create_memory_table_sql(self) -> str:
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

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
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    def create_tables(self) -> None:
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_memory_table_sql())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        effective_limit = limit if limit is not None else self._max_results
        if self._use_fts:
            logger.debug("CockroachDB full-text search not supported; using simple search")

        sql = f"""
        SELECT * FROM {self._memory_table}
        WHERE app_name = %s AND user_id = %s AND content_text ILIKE %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        params = (app_name, user_id, f"%{query}%", effective_limit)

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(sql.encode(), params)
                rows = cur.fetchall()
                columns = [col[0] for col in cur.description or []]
        except errors.UndefinedTable:
            return []

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (session_id,))
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - INTERVAL %s DAY)
        """
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (days,))
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
