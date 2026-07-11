"""CockroachDB ADK store for Google Agent Development Kit session/event storage (psycopg)."""

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

    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = (
    "CockroachPsycopgADKConfig",
    "CockroachPsycopgAsyncADKMemoryStore",
    "CockroachPsycopgAsyncADKStore",
    "CockroachPsycopgSyncADKMemoryStore",
    "CockroachPsycopgSyncADKStore",
)

logger = get_logger("sqlspec.adapters.cockroach_psycopg.adk.store")


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
    "        CREATE INDEX IF NOT EXISTS idx_{3}_app_user\n"
    "            ON {4}(app_name, user_id){5};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{6}_update_time\n"
    "            ON {7}(update_time DESC){8};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{9}_state\n"
    "            ON {10} USING GIN (state)\n"
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
    "            event_data JSONB NOT NULL,\n"
    "            FOREIGN KEY (session_id) REFERENCES {1}(id) ON DELETE CASCADE\n"
    "        ){2};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{3}_session\n"
    "            ON {4}(session_id, timestamp ASC){5}{6};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{7}_event_data\n"
    "            ON {8} USING GIN (event_data);\n"
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
    "        CREATE INDEX IF NOT EXISTS idx_{0}_content_trgm\n"
    "            ON {1} USING GIN (content_text gin_trgm_ops);\n"
    "            "
)

_ADK_MEMORY_TABLE_DDL_TEMPLATE_3 = (
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
    "        ){2};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{3}_app_user_time\n"
    "            ON {4}(app_name, user_id, timestamp DESC){5};\n"
    "\n"
    "        CREATE INDEX IF NOT EXISTS idx_{6}_session\n"
    "            ON {7}(session_id);\n"
    "        {8}\n"
    "        {9}\n"
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

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
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

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), params)
            await conn.commit()

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
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (app_name, user_id, session_id))
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
        sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), (Jsonb(state), app_name, user_id, session_id))
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
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), params)
                rows = await cur.fetchall()
        except errors.UndefinedTable:
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
        sql = f"DELETE FROM {self._session_table} WHERE app_name = %s AND user_id = %s AND id = %s"

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), (app_name, user_id, session_id))
            await conn.commit()

    async def append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                sql.encode(),
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            await conn.commit()

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
        ) VALUES (%s, %s, %s, %s, %s)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """
        app_upsert_sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """
        user_upsert_sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            try:
                await cur.execute(
                    insert_sql.encode(),
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        jsonb_value,
                    ),
                )
                await cur.execute(update_sql.encode(), (Jsonb(state), app_name, user_id, session_id))
                row = await cur.fetchone()
                if row is None:
                    _raise_missing_session(session_id)
                if app_state is not None:
                    await cur.execute(app_upsert_sql.encode(), (app_name, Jsonb(app_state)))
                if user_state is not None:
                    await cur.execute(user_upsert_sql.encode(), (app_name, user_id, Jsonb(user_state)))
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

        where_clauses = ["s.app_name = %s", "s.user_id = %s", "e.session_id = %s"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("e.timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = " LIMIT %s" if limit is not None else ""
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
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), tuple(params))
                rows = await cur.fetchall()
        except errors.UndefinedTable:
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
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < %s"

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (before,))
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def delete_idle_sessions(self, updated_before: "datetime") -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < %s"

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (updated_before,))
                await conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = %s"

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (app_name,))
                row = await cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = %s AND user_id = %s"

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (app_name, user_id))
                row = await cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), (app_name, Jsonb(state)))
            await conn.commit()

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), (app_name, user_id, Jsonb(state)))
            await conn.commit()

    async def get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = %s"

        try:
            async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql.encode(), (key,))
                row = await cur.fetchone()
                return row["value"] if row is not None else None
        except errors.UndefinedTable:
            return None

    async def set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        UPSERT INTO {self._metadata_table} (key, value)
        VALUES (%s, %s)
        """

        async with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.encode(), (key, value))
            await conn.commit()

    async def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        session_locality = _cockroach_table_locality_clause(adk_config, "session_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        session_storing_clause = _cockroach_storing_clause(adk_config, ("state", "create_time", "update_time"))

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            self._session_table,
            owner_id_line,
            session_locality,
            self._session_table,
            self._session_table,
            session_storing_clause,
            self._session_table,
            self._session_table,
            hash_shard_clause,
            self._session_table,
            self._session_table,
        )

    async def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        events_locality = _cockroach_table_locality_clause(adk_config, "events_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        events_storing_clause = _cockroach_storing_clause(adk_config, ("invocation_id", "event_data"))

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            self._events_table,
            self._session_table,
            events_locality,
            self._events_table,
            self._events_table,
            hash_shard_clause,
            events_storing_clause,
            self._events_table,
            self._events_table,
        )

    async def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        app_state_locality = _cockroach_table_locality_clause(adk_config, "app_state_table_locality")

        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(self._app_state_table, app_state_locality)

    async def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        user_state_locality = _cockroach_table_locality_clause(adk_config, "user_state_table_locality")

        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(self._user_state_table, user_state_locality)

    async def _metadata_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        metadata_locality = _cockroach_table_locality_clause(adk_config, "metadata_table_locality")

        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(self._metadata_table, metadata_locality)

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


class CockroachPsycopgSyncADKStore(BaseSyncADKStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK store using psycopg sync driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
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

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), params)
            conn.commit()

        result = self.get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Session creation failed"
            raise RuntimeError(msg)
        return result

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID."""
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = CURRENT_TIMESTAMP
            WHERE app_name = %s AND user_id = %s AND id = %s
            RETURNING id, app_name, user_id, state, create_time, update_time
            """
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = %s AND user_id = %s AND id = %s
            """

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (app_name, user_id, session_id))
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
        sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        """

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), (Jsonb(state), app_name, user_id, session_id))
            conn.commit()

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
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
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
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

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        sql = f"DELETE FROM {self._session_table} WHERE app_name = %s AND user_id = %s AND id = %s"

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), (app_name, user_id, session_id))
            conn.commit()

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
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = %s, update_time = CURRENT_TIMESTAMP
        WHERE app_name = %s AND user_id = %s AND id = %s
        RETURNING id, app_name, user_id, state, create_time, update_time
        """
        app_upsert_sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """
        user_upsert_sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            try:
                cur.execute(
                    insert_sql.encode(),
                    (
                        event_record["id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        event_record["timestamp"],
                        jsonb_value,
                    ),
                )
                cur.execute(update_sql.encode(), (Jsonb(state), app_name, user_id, session_id))
                row = cur.fetchone()
                if row is None:
                    _raise_missing_session(session_id)
                if app_state is not None:
                    cur.execute(app_upsert_sql.encode(), (app_name, Jsonb(app_state)))
                if user_state is not None:
                    cur.execute(user_upsert_sql.encode(), (app_name, user_id, Jsonb(user_state)))
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

        where_clauses = ["s.app_name = %s", "s.user_id = %s", "e.session_id = %s"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("e.timestamp > %s")
            params.append(after_timestamp)

        where_clause = " AND ".join(where_clauses)
        limit_clause = " LIMIT %s" if limit is not None else ""
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
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), tuple(params))
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
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < %s"

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (before,))
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def delete_idle_sessions(self, updated_before: "datetime") -> int:
        """Delete sessions whose update_time predates the given threshold."""
        sql = f"DELETE FROM {self._session_table} WHERE update_time < %s"

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (updated_before,))
                conn.commit()
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except errors.UndefinedTable:
            return 0

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state for an application."""
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = %s"

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (app_name,))
                row = cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state for an application user."""
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = %s AND user_id = %s"

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (app_name, user_id))
                row = cur.fetchone()
                return row["state"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state for an application."""
        sql = f"""
        UPSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), (app_name, Jsonb(state)))
            conn.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state for an application user."""
        sql = f"""
        UPSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), (app_name, user_id, Jsonb(state)))
            conn.commit()

    def get_metadata(self, key: str) -> "str | None":
        """Return a value from the ADK internal metadata table."""
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = %s"

        try:
            with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql.encode(), (key,))
                row = cur.fetchone()
                return row["value"] if row is not None else None
        except errors.UndefinedTable:
            return None

    def set_metadata(self, key: str, value: str) -> None:
        """Set a value in the ADK internal metadata table."""
        sql = f"""
        UPSERT INTO {self._metadata_table} (key, value)
        VALUES (%s, %s)
        """

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql.encode(), (key, value))
            conn.commit()

    def _sessions_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        session_locality = _cockroach_table_locality_clause(adk_config, "session_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        session_storing_clause = _cockroach_storing_clause(adk_config, ("state", "create_time", "update_time"))

        return _ADK_SESSIONS_TABLE_DDL_TEMPLATE_2.format(
            self._session_table,
            owner_id_line,
            session_locality,
            self._session_table,
            self._session_table,
            session_storing_clause,
            self._session_table,
            self._session_table,
            hash_shard_clause,
            self._session_table,
            self._session_table,
        )

    def _events_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        events_locality = _cockroach_table_locality_clause(adk_config, "events_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)
        events_storing_clause = _cockroach_storing_clause(adk_config, ("invocation_id", "event_data"))

        return _ADK_EVENTS_TABLE_DDL_TEMPLATE.format(
            self._events_table,
            self._session_table,
            events_locality,
            self._events_table,
            self._events_table,
            hash_shard_clause,
            events_storing_clause,
            self._events_table,
            self._events_table,
        )

    def _app_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        app_state_locality = _cockroach_table_locality_clause(adk_config, "app_state_table_locality")

        return _ADK_APP_STATES_TABLE_DDL_TEMPLATE.format(self._app_state_table, app_state_locality)

    def _user_states_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        user_state_locality = _cockroach_table_locality_clause(adk_config, "user_state_table_locality")

        return _ADK_USER_STATES_TABLE_DDL_TEMPLATE.format(self._user_state_table, user_state_locality)

    def _metadata_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        metadata_locality = _cockroach_table_locality_clause(adk_config, "metadata_table_locality")

        return _ADK_METADATA_TABLE_DDL_TEMPLATE.format(self._metadata_table, metadata_locality)

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
        sql = f"""
        INSERT INTO {self._events_table} (
            id, session_id, invocation_id, timestamp, event_data
        ) VALUES (%s, %s, %s, %s, %s)
        """
        event_data_value = event_record["event_data"]
        jsonb_value = Jsonb(event_data_value) if isinstance(event_data_value, dict) else event_data_value

        with self._config.provide_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql.encode(),
                (
                    event_record["id"],
                    event_record["session_id"],
                    event_record["invocation_id"],
                    event_record["timestamp"],
                    jsonb_value,
                ),
            )
            conn.commit()


class CockroachPsycopgAsyncADKMemoryStore(BaseAsyncADKMemoryStore["CockroachPsycopgAsyncConfig"]):
    """CockroachDB ADK memory store using psycopg async driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)

    async def create_tables(self) -> None:
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
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY timestamp DESC
            LIMIT %s
            """
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """

        search_param = query if self._use_fts else f"%{query}%"
        params = (app_name, user_id, search_param, effective_limit)

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
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """
        async with self._config.provide_connection() as conn, conn.cursor() as cur:
            await cur.execute(sql.encode())
            await conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        memory_locality = _cockroach_table_locality_clause(adk_config, "memory_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)

        fts_index = ""
        if self._use_fts:
            fts_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(self._memory_table, self._memory_table)
        trigram_index = ""
        if adk_config.get("enable_memory_trigram_index", False):
            trigram_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE_2.format(self._memory_table, self._memory_table)

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE_3.format(
            self._memory_table,
            owner_id_line,
            memory_locality,
            self._memory_table,
            self._memory_table,
            hash_shard_clause,
            self._memory_table,
            self._memory_table,
            fts_index,
            trigram_index,
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]


class CockroachPsycopgSyncADKMemoryStore(BaseSyncADKMemoryStore["CockroachPsycopgSyncConfig"]):
    """CockroachDB ADK memory store using psycopg sync driver."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self._enabled:
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._memory_table_ddl())

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not query:
            return []

        effective_limit = limit if limit is not None else self._max_results

        if self._use_fts:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s
              AND to_tsvector('english', content_text) @@ plainto_tsquery('english', %s)
            ORDER BY timestamp DESC
            LIMIT %s
            """
        else:
            sql = f"""
            SELECT * FROM {self._memory_table}
            WHERE app_name = %s AND user_id = %s AND content_text ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
            """

        search_param = query if self._use_fts else f"%{query}%"
        params = (app_name, user_id, search_param, effective_limit)

        try:
            with self._config.provide_connection() as conn, conn.cursor() as cur:
                cur.execute(sql.encode(), params)
                rows = cur.fetchall()
                columns = [col[0] for col in cur.description or []]
        except errors.UndefinedTable:
            return []

        return [cast("MemoryRecord", dict(zip(columns, row, strict=False))) for row in rows]

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"DELETE FROM {self._memory_table} WHERE session_id = %s"
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode(), (session_id,))
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '{days} days'
        """
        with self._config.provide_connection() as conn, conn.cursor() as cur:
            cur.execute(sql.encode())
            conn.commit()
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def _memory_table_ddl(self) -> str:
        adk_config = _adk_config(self._config)
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = _ADK_SESSIONS_TABLE_DDL_TEMPLATE.format(self._owner_id_column_ddl)
        memory_locality = _cockroach_table_locality_clause(adk_config, "memory_table_locality")
        hash_shard_clause = _cockroach_hash_shard_clause(adk_config)

        fts_index = ""
        if self._use_fts:
            fts_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE.format(self._memory_table, self._memory_table)
        trigram_index = ""
        if adk_config.get("enable_memory_trigram_index", False):
            trigram_index = _ADK_MEMORY_TABLE_DDL_TEMPLATE_2.format(self._memory_table, self._memory_table)

        return _ADK_MEMORY_TABLE_DDL_TEMPLATE_3.format(
            self._memory_table,
            owner_id_line,
            memory_locality,
            self._memory_table,
            self._memory_table,
            hash_shard_clause,
            self._memory_table,
            self._memory_table,
            fts_index,
            trigram_index,
        )

    def _drop_memory_table_sql(self) -> "list[str]":
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]


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
    return f"\n        {clause}"


def _cockroach_hash_shard_clause(adk_config: CockroachPsycopgADKConfig) -> str:
    if not adk_config.get("enable_hash_sharded_indexes", False):
        return ""
    bucket_count = adk_config.get("hash_shard_bucket_count")
    if isinstance(bucket_count, int) and not isinstance(bucket_count, bool) and bucket_count > 0:
        return f" USING HASH WITH (bucket_count = {bucket_count})"
    return " USING HASH"


def _cockroach_storing_clause(adk_config: CockroachPsycopgADKConfig, columns: tuple[str, ...]) -> str:
    if not adk_config.get("enable_storing_indexes", False):
        return ""
    return f" STORING ({', '.join(columns)})"
