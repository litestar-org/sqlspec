"""CockroachDB ADK store for Google Agent Development Kit session/event storage (asyncpg)."""

from typing import TYPE_CHECKING, Any, cast

import asyncpg
from typing_extensions import NotRequired

from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("CockroachAsyncpgADKConfig", "CockroachAsyncpgADKMemoryStore", "CockroachAsyncpgADKStore")

logger = get_logger("sqlspec.adapters.cockroach_asyncpg.adk.store")


class CockroachAsyncpgADKConfig(ADKConfig):
    """CockroachDB asyncpg ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the CockroachDB asyncpg ADK stores.
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


class CockroachAsyncpgADKStore(BaseAsyncADKStore["CockroachAsyncpgConfig"]):
    """CockroachDB ADK store using asyncpg driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        super().__init__(config)

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

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, owner_id, state)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            params = (session_id, app_name, user_id, state)

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, *params)

        result = await self.get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Session creation failed"
            raise RuntimeError(msg)
        return result

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = $1 AND user_id = $2 AND id = $3
            RETURNING id, app_name, user_id, state, create_time, update_time
            """
            params = (app_name, user_id, session_id)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2 AND id = $3
            """
            params = (app_name, user_id, session_id)

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, *params)
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
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE app_name = $2 AND user_id = $3 AND id = $4
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, state, app_name, user_id, session_id)

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1
            ORDER BY update_time DESC
            """
            params: tuple[Any, ...] = (app_name,)
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = $1 AND user_id = $2
            ORDER BY update_time DESC
            """
            params = (app_name, user_id)

        try:
            async with self._config.provide_connection() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.exceptions.UndefinedTableError:
            return []

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

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        sql = f"DELETE FROM {self._session_table} WHERE app_name = $1 AND user_id = $2 AND id = $3"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, user_id, session_id)

    async def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES ($1, $2, $3, $4, $5)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(
                sql,
                event_record["id"],
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["timestamp"],
                event_record["event_data"],
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
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES ($1, $2, $3, $4, $5)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = $1, update_time = CURRENT_TIMESTAMP
        WHERE app_name = $2 AND user_id = $3 AND id = $4
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = $1 AND user_id = $2 AND id = $3
        """
        app_upsert_sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        """
        user_upsert_sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn, conn.transaction():
            await conn.execute(
                insert_sql,
                event_record["id"],
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["timestamp"],
                event_record["event_data"],
            )
            result = await conn.execute(update_sql, state, app_name, user_id, session_id)
            if result == "UPDATE 0":
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)
            if app_state is not None:
                await conn.execute(app_upsert_sql, app_name, app_state)
            if user_state is not None:
                await conn.execute(user_upsert_sql, app_name, user_id, user_state)
            row = await conn.fetchrow(select_sql, app_name, user_id, session_id)
            if row is None:
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)

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

        where_clauses = ["s.app_name = $1", "s.user_id = $2", "e.session_id = $3"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append(f"e.timestamp > ${len(params) + 1}")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT ${len(params) + 1}" if limit is not None else ""
        if limit is not None:
            params.append(limit)

        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._events_table} e
        JOIN {self._session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """

        try:
            async with self._config.provide_connection() as conn:
                rows = await conn.fetch(sql, *params)
        except asyncpg.exceptions.UndefinedTableError:
            return []

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

    async def delete_expired_events(self, before: "datetime") -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < $1"

        try:
            async with self._config.provide_connection() as conn:
                result = await conn.execute(sql, before)
                return int(result.split()[-1]) if result else 0
        except asyncpg.exceptions.UndefinedTableError:
            return 0

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < $1"

        try:
            async with self._config.provide_connection() as conn:
                result = await conn.execute(sql, updated_before)
                return int(result.split()[-1]) if result else 0
        except asyncpg.exceptions.UndefinedTableError:
            return 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = $1"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, app_name)
                return row["state"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = $1 AND user_id = $2"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, app_name, user_id)
                return row["state"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, state)

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, app_name, user_id, state)

    async def get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = $1"

        try:
            async with self._config.provide_connection() as conn:
                row = await conn.fetchrow(sql, key)
                return row["value"] if row is not None else None
        except asyncpg.exceptions.UndefinedTableError:
            return None

    async def set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        UPSERT INTO {self._metadata_table} (key, value)
        VALUES ($1, $2)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, key, value)

    async def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"
        session_locality = _cockroach_table_locality_clause(adk_config, "session_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        session_storing_clause = _cockroach_storing_clause(adk_config, ("state", "create_time", "update_time"))

        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_line},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){session_locality};

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_app_user
            ON {self._session_table}(app_name, user_id){session_storing_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time
            ON {self._session_table}(update_time DESC){hash_shard_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._session_table}_state
            ON {self._session_table} USING GIN (state)
            WHERE state != '{{}}'::jsonb;
        """

    async def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        events_locality = _cockroach_table_locality_clause(adk_config, "events_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        events_storing_clause = _cockroach_storing_clause(adk_config, ("invocation_id", "event_data"))

        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        ){events_locality};

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_session
            ON {self._events_table}(session_id, timestamp ASC){hash_shard_clause}{events_storing_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._events_table}_event_data
            ON {self._events_table} USING GIN (event_data);
        """

    async def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        app_state_locality = _cockroach_table_locality_clause(adk_config, "app_state_table_locality")

        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ){app_state_locality};
        """

    async def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        user_state_locality = _cockroach_table_locality_clause(adk_config, "user_state_table_locality")

        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (app_name, user_id)
        ){user_state_locality};
        """

    async def _metadata_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        metadata_locality = _cockroach_table_locality_clause(adk_config, "metadata_table_locality")

        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        ){metadata_locality};
        """

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


class CockroachAsyncpgADKMemoryStore(BaseAsyncADKMemoryStore["CockroachAsyncpgConfig"]):
    """CockroachDB ADK memory store using asyncpg driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        super().__init__(config)

    async def create_tables(self) -> None:
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        if not self._enabled:
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._memory_table_ddl())

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        async with self._config.provide_connection() as conn:
            for entry in entries:
                if self._owner_id_column_name:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     {self._owner_id_column_name}, timestamp, content_json,
                     content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    result = await conn.execute(
                        sql,
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        owner_id,
                        entry["timestamp"],
                        entry["content_json"],
                        entry["content_text"],
                        entry["metadata_json"],
                        entry["inserted_at"],
                    )
                else:
                    sql = f"""
                    INSERT INTO {self._memory_table}
                    (id, session_id, app_name, user_id, event_id, author,
                     timestamp, content_json, content_text, metadata_json, inserted_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                    result = await conn.execute(
                        sql,
                        entry["id"],
                        entry["session_id"],
                        entry["app_name"],
                        entry["user_id"],
                        entry["event_id"],
                        entry["author"],
                        entry["timestamp"],
                        entry["content_json"],
                        entry["content_text"],
                        entry["metadata_json"],
                        entry["inserted_at"],
                    )

                if result and result.split()[-1].isdigit() and int(result.split()[-1]) > 0:
                    inserted_count += 1

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
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', $3)
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params: tuple[Any, ...] = (app_name, user_id, query, effective_limit)
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = $1 AND user_id = $2 AND content_text ILIKE $3
            ORDER BY timestamp DESC
            LIMIT $4
            """
            params = (app_name, user_id, f"%{query}%", effective_limit)

        async with self._config.provide_connection() as conn:
            rows = await conn.fetch(sql, *params)

        return [cast("MemoryRecord", dict(row)) for row in rows]

    async def delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = $1"
        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql, session_id)
            return int(result.split()[-1]) if result else 0

    async def delete_entries_older_than(self, days: int) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < (CURRENT_TIMESTAMP - INTERVAL '{days} days')
        """
        async with self._config.provide_connection() as conn:
            result = await conn.execute(sql)
            return int(result.split()[-1]) if result else 0

    async def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"
        memory_locality = _cockroach_table_locality_clause(adk_config, "memory_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts
            ON {self._memory_table} USING GIN (to_tsvector('english', content_text));
            """
        trigram_index = ""
        if adk_config.get("enable_memory_trigram_index", False):
            trigram_index = f"""
        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_content_trgm
            ON {self._memory_table} USING GIN (content_text gin_trgm_ops);
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
        ){memory_locality};

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_app_user_time
            ON {self._memory_table}(app_name, user_id, timestamp DESC){hash_shard_clause};

        CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_session
            ON {self._memory_table}(session_id);
        {fts_index}
        {trigram_index}
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]


def _adk_config(config: Any) -> CockroachAsyncpgADKConfig:
    """Return CockroachDB asyncpg ADK extension settings from ``extension_config["adk"]``."""

    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("CockroachAsyncpgADKConfig", adk_config)


def _cockroach_table_locality_clause(adk_config: CockroachAsyncpgADKConfig, table_key: str) -> str:
    locality = adk_config.get(table_key) or adk_config.get("table_locality")
    if not isinstance(locality, str):
        return ""
    clause = locality.strip()
    if not clause:
        return ""
    if not clause.upper().startswith("LOCALITY "):
        clause = f"LOCALITY {clause}"
    return f"\n        {clause}"


def _cockroach_hash_shard_clause(adk_config: CockroachAsyncpgADKConfig) -> str:
    if not adk_config.get("enable_hash_sharded_indexes", False):
        return ""
    bucket_count = adk_config.get("hash_shard_bucket_count")
    if isinstance(bucket_count, int) and not isinstance(bucket_count, bool) and bucket_count > 0:
        return f" USING HASH WITH (bucket_count = {bucket_count})"
    return " USING HASH"


def _cockroach_storing_clause(adk_config: CockroachAsyncpgADKConfig, columns: tuple[str, ...]) -> str:
    if not adk_config.get("enable_storing_indexes", False):
        return ""
    return f" STORING ({', '.join(columns)})"
