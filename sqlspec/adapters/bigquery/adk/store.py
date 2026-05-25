"""BigQuery ADK store — analytics-replica path for ADK sessions, events, and memory.

BigQuery is an analytical (OLAP) warehouse. Query latency is measured in seconds, not
milliseconds, and BigQuery DML does not provide cross-statement transactions. This store
is intended as the **analytics-replica path** for ADK telemetry — replay, search, and
historical analysis — not as a live OLTP session store for synchronous agent loops.

For live agent state, pair this store with Spanner, PostgreSQL, or one of the other
ADK adapters and stream into BigQuery for analytics.

Layout decisions:
    * sessions  — PARTITION BY DATE(create_time), CLUSTER BY app_name, user_id
    * events    — PARTITION BY DATE(timestamp),  CLUSTER BY session_id, app_name, user_id
    * app_state — CLUSTER BY app_name
    * user_state — CLUSTER BY app_name, user_id

When ``ADKConfig.bigquery.session_lookup_window_days`` is set, list reads constrain
``create_time`` so partitioned scans stay cheap. ``ADKConfig.retention.event_ttl_seconds``
maps to ``partition_expiration_days`` on the events table when ``require_partition_filter``
is enabled. JSON is stored using BigQuery's native ``JSON`` type.
"""

import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk._config_utils import _get_adk_config_from_extension
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ("BigQueryADKStore",)

_DEFAULT_LOOKUP_WINDOW_DAYS = 30


class BigQueryADKStore(BaseAsyncADKStore[BigQueryConfig]):
    """BigQuery ADK session/event/scoped-state store (analytics-replica path).

    Important: BigQuery query jobs are seconds-latency. Do not use this store for
    synchronous agent inner loops. Use it for analytics, replay, and audit workloads.
    Pair with an OLTP-grade ADK adapter (Spanner, PostgreSQL family) for live state.
    """

    connector_name: ClassVar[str] = "bigquery"
    __slots__ = ("_dataset_qualifier", "_lookup_window_days", "_partition_expiration_days", "_require_partition_filter")

    def __init__(self, config: BigQueryConfig) -> None:
        """Initialize BigQuery ADK store."""
        super().__init__(config)
        adk_config = _get_adk_config_from_extension(config)
        bigquery_config = adk_config.get("bigquery") or {}
        retention_config = adk_config.get("retention") or {}

        self._lookup_window_days: int = int(
            bigquery_config.get("session_lookup_window_days") or _DEFAULT_LOOKUP_WINDOW_DAYS
        )
        ttl_seconds = retention_config.get("event_ttl_seconds")
        self._partition_expiration_days: int | None = (
            max(1, math.ceil(int(ttl_seconds) / 86400)) if ttl_seconds else None
        )
        self._require_partition_filter: bool = bool(bigquery_config.get("require_partition_filter", True))

        dataset_id = config.connection_config.get("dataset_id")
        self._dataset_qualifier: str = f"{dataset_id}." if dataset_id else ""

    def _qualified(self, table: str) -> str:
        """Return the dataset-qualified table identifier when available."""
        return f"{self._dataset_qualifier}{table}"

    # ------------------------------------------------------------------
    # Session CRUD
    # ------------------------------------------------------------------

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        return await async_(self._get_session)(app_name, user_id, session_id, renew_for)

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        await async_(self._update_session_state)(app_name, user_id, session_id, state)

    async def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        return await async_(self._list_sessions)(app_name, user_id)

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        await async_(self._delete_session)(app_name, user_id, session_id)

    async def append_event(self, event_record: EventRecord) -> None:
        await async_(self._append_event)(event_record)

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
        return await async_(self._append_event_and_update_state)(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        return await async_(self._get_events)(app_name, user_id, session_id, after_timestamp, limit)

    async def delete_expired_events(self, before: datetime) -> int:
        return await async_(self._delete_expired_events)(before)

    async def delete_idle_sessions(self, updated_before: datetime) -> int:
        return await async_(self._delete_idle_sessions)(updated_before)

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        return await async_(self._get_app_state)(app_name)

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        return await async_(self._get_user_state)(app_name, user_id)

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        await async_(self._upsert_app_state)(app_name, state)

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        await async_(self._upsert_user_state)(app_name, user_id, state)

    async def get_metadata(self, key: str) -> "str | None":
        return await async_(self._get_metadata)(key)

    async def set_metadata(self, key: str, value: str) -> None:
        await async_(self._set_metadata)(key, value)

    async def create_tables(self) -> None:
        await async_(self._create_tables)()

    # ------------------------------------------------------------------
    # Sync implementations
    # ------------------------------------------------------------------

    def _run_query(self, sql: str, parameters: "Iterable[Any] | None" = None) -> "list[dict[str, Any]]":
        from google.cloud import bigquery

        client = self._config.create_connection()
        job_config = bigquery.QueryJobConfig(query_parameters=list(parameters)) if parameters is not None else None
        job = client.query(sql, job_config=job_config)
        return [dict(row) for row in job.result()]

    def _query_param(self, name: str, value: Any, *, bq_type: str = "STRING") -> Any:
        from google.cloud import bigquery

        return bigquery.ScalarQueryParameter(name, bq_type, value)

    def _json_param(self, name: str, value: "dict[str, Any] | None") -> Any:
        from google.cloud import bigquery

        return bigquery.ScalarQueryParameter(name, "JSON", to_json(value) if value is not None else None)

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        now = datetime.now(timezone.utc)
        sql = f"""
        INSERT INTO {self._qualified(self._session_table)}
            (id, app_name, user_id, state, create_time, update_time)
        VALUES (@id, @app_name, @user_id, @state, @create_time, @update_time)
        """
        params = [
            self._query_param("id", session_id),
            self._query_param("app_name", app_name),
            self._query_param("user_id", user_id),
            self._json_param("state", state),
            self._query_param("create_time", now, bq_type="TIMESTAMP"),
            self._query_param("update_time", now, bq_type="TIMESTAMP"),
        ]
        self._run_query(sql, params)
        return {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": state,
            "create_time": now,
            "update_time": now,
        }

    def _get_session(
        self, app_name: str, user_id: str, session_id: str, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            self._update_session_touch(app_name, user_id, session_id)

        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._qualified(self._session_table)}
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        LIMIT 1
        """
        rows = self._run_query(
            sql,
            [
                self._query_param("app_name", app_name),
                self._query_param("user_id", user_id),
                self._query_param("id", session_id),
            ],
        )
        if not rows:
            return None
        row = rows[0]
        record: SessionRecord = {
            "id": row["id"],
            "app_name": row["app_name"],
            "user_id": row["user_id"],
            "state": self._decode_json(row["state"]) or {},
            "create_time": row["create_time"],
            "update_time": row["update_time"],
        }
        return record

    def _update_session_touch(self, app_name: str, user_id: str, session_id: str) -> None:
        sql = f"""
        UPDATE {self._qualified(self._session_table)}
        SET update_time = CURRENT_TIMESTAMP()
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        self._run_query(
            sql,
            [
                self._query_param("app_name", app_name),
                self._query_param("user_id", user_id),
                self._query_param("id", session_id),
            ],
        )

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        UPDATE {self._qualified(self._session_table)}
        SET state = @state, update_time = CURRENT_TIMESTAMP()
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        self._run_query(
            sql,
            [
                self._json_param("state", state),
                self._query_param("app_name", app_name),
                self._query_param("user_id", user_id),
                self._query_param("id", session_id),
            ],
        )

    def _list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        window_start = datetime.now(timezone.utc) - timedelta(days=self._lookup_window_days)
        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._qualified(self._session_table)}
        WHERE app_name = @app_name
          AND create_time >= @window_start
        """
        params = [
            self._query_param("app_name", app_name),
            self._query_param("window_start", window_start, bq_type="TIMESTAMP"),
        ]
        if user_id is not None:
            sql += " AND user_id = @user_id"
            params.append(self._query_param("user_id", user_id))
        sql += " ORDER BY update_time DESC"
        rows = self._run_query(sql, params)
        records: list[SessionRecord] = []
        for row in rows:
            record: SessionRecord = {
                "id": row["id"],
                "app_name": row["app_name"],
                "user_id": row["user_id"],
                "state": self._decode_json(row["state"]) or {},
                "create_time": row["create_time"],
                "update_time": row["update_time"],
            }
            records.append(record)
        return records

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        events_sql = f"DELETE FROM {self._qualified(self._events_table)} WHERE session_id = @id"
        sessions_sql = f"DELETE FROM {self._qualified(self._session_table)} WHERE app_name = @app_name AND user_id = @user_id AND id = @id"
        self._run_query(events_sql, [self._query_param("id", session_id)])
        self._run_query(
            sessions_sql,
            [
                self._query_param("app_name", app_name),
                self._query_param("user_id", user_id),
                self._query_param("id", session_id),
            ],
        )

    def _append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._qualified(self._events_table)}
            (id, session_id, invocation_id, timestamp, event_data)
        VALUES (@id, @session_id, @invocation_id, @timestamp, @event_data)
        """
        params = [
            self._query_param("id", event_record["id"]),
            self._query_param("session_id", event_record["session_id"]),
            self._query_param("invocation_id", event_record["invocation_id"]),
            self._query_param("timestamp", event_record["timestamp"], bq_type="TIMESTAMP"),
            self._json_param("event_data", event_record["event_data"]),
        ]
        self._run_query(sql, params)

    def _append_event_and_update_state(
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
        self._append_event(event_record)
        self._update_session_state(app_name, user_id, session_id, state)
        if app_state:
            self._upsert_app_state(app_name, app_state)
        if user_state:
            self._upsert_user_state(app_name, user_id, user_state)

        record = self._get_session(app_name, user_id, session_id)
        if record is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return record

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._qualified(self._events_table)} e
        JOIN {self._qualified(self._session_table)} s ON e.session_id = s.id
        WHERE s.app_name = @app_name AND s.user_id = @user_id AND e.session_id = @session_id
        """
        params = [
            self._query_param("app_name", app_name),
            self._query_param("user_id", user_id),
            self._query_param("session_id", session_id),
        ]
        if after_timestamp is not None:
            sql += " AND e.timestamp > @after_timestamp"
            params.append(self._query_param("after_timestamp", after_timestamp, bq_type="TIMESTAMP"))
        sql += " ORDER BY e.timestamp ASC"
        if limit is not None:
            sql += " LIMIT @row_limit"
            params.append(self._query_param("row_limit", limit, bq_type="INT64"))
        rows = self._run_query(sql, params)
        return [
            {
                "id": row["id"],
                "session_id": row["session_id"],
                "invocation_id": row["invocation_id"],
                "timestamp": row["timestamp"],
                "event_data": self._decode_json(row["event_data"]) or {},
                "app_name": row["app_name"],
                "user_id": row["user_id"],
            }
            for row in rows
        ]

    def _delete_expired_events(self, before: datetime) -> int:
        sql = f"DELETE FROM {self._qualified(self._events_table)} WHERE timestamp < @before"
        # BigQuery jobs don't expose affected-rows reliably across all versions;
        # callers treat the count as best-effort and may consult job statistics if needed.
        self._run_query(sql, [self._query_param("before", before, bq_type="TIMESTAMP")])
        return 0

    def _delete_idle_sessions(self, updated_before: datetime) -> int:
        sql = f"DELETE FROM {self._qualified(self._session_table)} WHERE update_time < @before"
        self._run_query(sql, [self._query_param("before", updated_before, bq_type="TIMESTAMP")])
        return 0

    # ------------------------------------------------------------------
    # Scoped state CRUD
    # ------------------------------------------------------------------

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"""
        SELECT state FROM {self._qualified(self._app_state_table)} WHERE app_name = @app_name LIMIT 1
        """
        rows = self._run_query(sql, [self._query_param("app_name", app_name)])
        return self._decode_json(rows[0]["state"]) if rows else None

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"""
        SELECT state
        FROM {self._qualified(self._user_state_table)}
        WHERE app_name = @app_name AND user_id = @user_id LIMIT 1
        """
        rows = self._run_query(sql, [self._query_param("app_name", app_name), self._query_param("user_id", user_id)])
        return self._decode_json(rows[0]["state"]) if rows else None

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
        MERGE {self._qualified(self._app_state_table)} target
        USING (SELECT @app_name AS app_name) source
        ON target.app_name = source.app_name
        WHEN MATCHED THEN
            UPDATE SET state = @state, update_time = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (app_name, state, update_time)
            VALUES (source.app_name, @state, CURRENT_TIMESTAMP())
        """
        self._run_query(sql, [self._query_param("app_name", app_name), self._json_param("state", state)])

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
        MERGE {self._qualified(self._user_state_table)} target
        USING (SELECT @app_name AS app_name, @user_id AS user_id) source
        ON target.app_name = source.app_name AND target.user_id = source.user_id
        WHEN MATCHED THEN
            UPDATE SET state = @state, update_time = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (app_name, user_id, state, update_time)
            VALUES (source.app_name, source.user_id, @state, CURRENT_TIMESTAMP())
        """
        self._run_query(
            sql,
            [
                self._query_param("app_name", app_name),
                self._query_param("user_id", user_id),
                self._json_param("state", state),
            ],
        )

    def _get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._qualified(self._metadata_table)} WHERE key = @key LIMIT 1"
        rows = self._run_query(sql, [self._query_param("key", key)])
        return rows[0]["value"] if rows else None

    def _set_metadata(self, key: str, value: str) -> None:
        sql = f"""
        MERGE {self._qualified(self._metadata_table)} target
        USING (SELECT @key AS key) source
        ON target.key = source.key
        WHEN MATCHED THEN UPDATE SET value = @value
        WHEN NOT MATCHED THEN INSERT (key, value) VALUES (source.key, @value)
        """
        self._run_query(sql, [self._query_param("key", key), self._query_param("value", value)])

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _partition_options(self) -> str:
        parts: list[str] = []
        if self._require_partition_filter:
            parts.append("require_partition_filter = TRUE")
        if self._partition_expiration_days is not None:
            parts.append(f"partition_expiration_days = {self._partition_expiration_days}")
        return f"\nOPTIONS({', '.join(parts)})" if parts else ""

    async def _get_create_sessions_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._session_table)} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            state JSON,
            create_time TIMESTAMP NOT NULL,
            update_time TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(create_time)
        CLUSTER BY app_name, user_id, id{self._partition_options()}
        """

    async def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._events_table)} (
            id STRING NOT NULL,
            session_id STRING NOT NULL,
            invocation_id STRING,
            timestamp TIMESTAMP NOT NULL,
            event_data JSON
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY session_id, id{self._partition_options()}
        """

    async def _get_create_app_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._app_state_table)} (
            app_name STRING NOT NULL,
            state JSON,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY app_name
        """

    async def _get_create_user_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._user_state_table)} (
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            state JSON,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY app_name, user_id
        """

    async def _get_create_metadata_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._metadata_table)} (
            key STRING NOT NULL,
            value STRING NOT NULL
        )
        CLUSTER BY key
        """

    async def _get_seed_metadata_sql(self) -> str:
        return f"""
        MERGE {self._qualified(self._metadata_table)} target
        USING (SELECT 'schema_version' AS key, '1' AS value) source
        ON target.key = source.key
        WHEN NOT MATCHED THEN INSERT (key, value) VALUES (source.key, source.value)
        """

    def _get_drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._qualified(self._app_state_table)}"

    def _get_drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._qualified(self._user_state_table)}"

    def _get_drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._qualified(self._metadata_table)}"

    def _get_drop_tables_sql(self) -> "list[str]":
        return [
            f"DROP TABLE IF EXISTS {self._qualified(self._events_table)}",
            f"DROP TABLE IF EXISTS {self._qualified(self._user_state_table)}",
            f"DROP TABLE IF EXISTS {self._qualified(self._app_state_table)}",
            f"DROP TABLE IF EXISTS {self._qualified(self._session_table)}",
            f"DROP TABLE IF EXISTS {self._qualified(self._metadata_table)}",
        ]

    def _create_tables(self) -> None:
        # Run DDL synchronously; sync wrappers above will offload to a thread.
        import asyncio

        async def _ddl_text() -> "list[str]":
            return [
                await self._get_create_sessions_table_sql(),
                await self._get_create_events_table_sql(),
                await self._get_create_app_states_table_sql(),
                await self._get_create_user_states_table_sql(),
                await self._get_create_metadata_table_sql(),
                await self._get_seed_metadata_sql(),
            ]

        statements = asyncio.run(_ddl_text())
        for statement in statements:
            self._run_query(statement)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decode_json(value: Any) -> "dict[str, Any] | None":
        if value is None:
            return None
        if isinstance(value, dict):
            return cast("dict[str, Any]", value)
        if isinstance(value, (bytes, bytearray)):
            return cast("dict[str, Any]", from_json(bytes(value).decode("utf-8")))
        if isinstance(value, str):
            return cast("dict[str, Any]", from_json(value))
        msg = f"Unsupported JSON column representation from BigQuery: {type(value).__name__}"
        raise TypeError(msg)

    @staticmethod
    def _new_id() -> str:
        return str(uuid.uuid4())
