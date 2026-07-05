"""BigQuery ADK store as an analytics-replica path.

BigQuery is an analytical warehouse. Query latency is measured in seconds, not
milliseconds, and BigQuery DML does not provide cross-statement transactions.
This store is intended for ADK telemetry replay, search, and historical
analysis, not for low-latency live agent state.

For live state, pair an OLTP ADK adapter such as Spanner or PostgreSQL with an
analytics stream into BigQuery.
"""

import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk._config_utils import _adk_config_from_extension
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ("BigQueryADKConfig", "BigQueryADKRetentionConfig", "BigQueryADKStore")

_DEFAULT_LOOKUP_WINDOW_DAYS = 30


class BigQueryADKRetentionConfig(TypedDict):
    """BigQuery-specific ADK retention settings."""

    event_ttl_seconds: NotRequired[int]
    """Event partition retention in seconds."""


class BigQueryADKConfig(ADKConfig):
    """BigQuery-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the BigQuery ADK
    store. Base table and service settings are inherited from ``ADKConfig``.
    """

    session_lookup_window_days: NotRequired[int]
    """Days of session partitions scanned by list_sessions()."""

    require_partition_filter: NotRequired[bool]
    """Opt into BigQuery required partition filters on partitioned ADK tables."""

    retention: NotRequired[BigQueryADKRetentionConfig]
    """BigQuery event partition retention settings."""


class BigQueryADKStore(BaseSyncADKStore[BigQueryConfig]):
    """BigQuery ADK session/event/scoped-state store.

    The store exposes the synchronous ADK store contract because BigQuery is a
    synchronous SQLSpec adapter. ``SQLSpecSessionService`` provides async
    bridging when Google ADK calls it from async service methods.
    """

    connector_name: ClassVar[str] = "bigquery"
    __slots__ = ("_dataset_qualifier", "_lookup_window_days", "_partition_expiration_days", "_require_partition_filter")

    def __init__(self, config: BigQueryConfig) -> None:
        """Initialize the BigQuery ADK store.

        Args:
            config: BigQuery config with ``extension_config["adk"]`` settings.
        """
        super().__init__(config)
        adk_config = _adk_config_from_extension(config)
        retention_config = cast("dict[str, Any]", adk_config.get("retention") or {})

        self._lookup_window_days: int = int(adk_config.get("session_lookup_window_days") or _DEFAULT_LOOKUP_WINDOW_DAYS)
        ttl_seconds = retention_config.get("event_ttl_seconds")
        self._partition_expiration_days: int | None = (
            max(1, math.ceil(int(ttl_seconds) / 86400)) if ttl_seconds else None
        )
        self._require_partition_filter: bool = bool(adk_config.get("require_partition_filter", False))

        dataset_id = config.connection_config.get("dataset_id")
        self._dataset_qualifier: str = f"{dataset_id}." if dataset_id else ""

    def create_tables(self) -> None:
        """Create the BigQuery ADK tables if they do not exist."""
        self._create_tables()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create or refresh a session row for the analytics replica."""
        return self._create_session(session_id, app_name, user_id, state, owner_id)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get a session by app, user, and session identifier."""
        return self._get_session(app_name, user_id, session_id, renew_for=renew_for)

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Replace the durable session state snapshot."""
        self._update_session_state(app_name, user_id, session_id, state)

    def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user."""
        return self._list_sessions(app_name, user_id)

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and its replicated events."""
        self._delete_session(app_name, user_id, session_id)

    def append_event(self, event_record: EventRecord) -> None:
        """Append an ADK event blob."""
        self._append_event(event_record)

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
        """Append an event and then update analytics-replica state.

        BigQuery has no cross-statement transaction for this path. The method
        preserves the shared ADK store API while keeping BigQuery positioned as
        a replay/analytics replica rather than live state storage.
        """
        return self._append_event_and_update_state(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
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
        return self._get_events(app_name, user_id, session_id, after_timestamp, limit)

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than a timestamp."""
        return self._delete_expired_events(before)

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete idle sessions older than a timestamp."""
        return self._delete_idle_sessions(updated_before)

    def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        return self._get_app_state(app_name)

    def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        return self._get_user_state(app_name, user_id)

    def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        self._upsert_app_state(app_name, state)

    def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        self._upsert_user_state(app_name, user_id, state)

    def get_metadata(self, key: str) -> "str | None":
        """Return a metadata value."""
        return self._get_metadata(key)

    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata value."""
        self._set_metadata(key, value)

    def _qualified(self, table: str) -> str:
        """Return the dataset-qualified table identifier when available."""
        return f"{self._dataset_qualifier}{table}"

    def _partition_filter(self, column: str, *, alias: str | None = None) -> str:
        """Return a broad partition predicate for opt-in required-filter mode."""
        if not self._require_partition_filter:
            return ""
        qualified_column = f"{alias}.{column}" if alias else column
        return f" AND {qualified_column} IS NOT NULL"

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
        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_select = ", @owner_id AS owner_id" if self._owner_id_column_name else ""
        owner_update = f", {self._owner_id_column_name} = source.owner_id" if self._owner_id_column_name else ""
        owner_insert = ", source.owner_id" if self._owner_id_column_name else ""
        sql = f"""
        MERGE {self._qualified(self._session_table)} target
        USING (
            SELECT @id AS id, @app_name AS app_name, @user_id AS user_id,
                   @state AS state, @create_time AS create_time, @update_time AS update_time{owner_select}
        ) source
        ON target.app_name = source.app_name AND target.user_id = source.user_id AND target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET state = source.state, update_time = source.update_time{owner_update}
        WHEN NOT MATCHED THEN
            INSERT (id, app_name, user_id{owner_column}, state, create_time, update_time)
            VALUES (source.id, source.app_name, source.user_id{owner_insert}, source.state, source.create_time, source.update_time)
        """
        params = [
            self._query_param("id", session_id),
            self._query_param("app_name", app_name),
            self._query_param("user_id", user_id),
            self._json_param("state", state),
            self._query_param("create_time", now, bq_type="TIMESTAMP"),
            self._query_param("update_time", now, bq_type="TIMESTAMP"),
        ]
        if self._owner_id_column_name:
            params.append(self._query_param("owner_id", owner_id))
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
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            self._update_session_touch(app_name, user_id, session_id)

        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._qualified(self._session_table)}
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
          {self._partition_filter("create_time").strip()}
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
        return _session_record_from_row(rows[0])

    def _update_session_touch(self, app_name: str, user_id: str, session_id: str) -> None:
        sql = f"""
        UPDATE {self._qualified(self._session_table)}
        SET update_time = CURRENT_TIMESTAMP()
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
          {self._partition_filter("create_time").strip()}
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
          {self._partition_filter("create_time").strip()}
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
        return [_session_record_from_row(row) for row in rows]

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        events_sql = (
            f"DELETE FROM {self._qualified(self._events_table)} "
            "WHERE app_name = @app_name AND user_id = @user_id AND session_id = @id"
            f"{self._partition_filter('timestamp')}"
        )
        sessions_sql = (
            f"DELETE FROM {self._qualified(self._session_table)} "
            "WHERE app_name = @app_name AND user_id = @user_id AND id = @id"
            f"{self._partition_filter('create_time')}"
        )
        params = [
            self._query_param("app_name", app_name),
            self._query_param("user_id", user_id),
            self._query_param("id", session_id),
        ]
        self._run_query(events_sql, params)
        self._run_query(sessions_sql, params)

    def _append_event(self, event_record: EventRecord) -> None:
        sql = f"""
        INSERT INTO {self._qualified(self._events_table)}
            (id, app_name, user_id, session_id, invocation_id, timestamp, event_data)
        VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, @event_data)
        """
        params = [
            self._query_param("id", event_record["id"]),
            self._query_param("app_name", event_record["app_name"]),
            self._query_param("user_id", event_record["user_id"]),
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
        if app_state is not None:
            self._upsert_app_state(app_name, app_state)
        if user_state is not None:
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
        SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data
        FROM {self._qualified(self._events_table)} e
        WHERE e.app_name = @app_name AND e.user_id = @user_id AND e.session_id = @session_id
          {self._partition_filter("timestamp", alias="e").strip()}
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
        return [_event_record_from_row(row) for row in rows]

    def _delete_expired_events(self, before: datetime) -> int:
        sql = (
            f"DELETE FROM {self._qualified(self._events_table)} "
            f"WHERE timestamp < @before{self._partition_filter('timestamp')}"
        )
        self._run_query(sql, [self._query_param("before", before, bq_type="TIMESTAMP")])
        return 0

    def _delete_idle_sessions(self, updated_before: datetime) -> int:
        sql = (
            f"DELETE FROM {self._qualified(self._session_table)} "
            f"WHERE update_time < @before{self._partition_filter('create_time')}"
        )
        self._run_query(sql, [self._query_param("before", updated_before, bq_type="TIMESTAMP")])
        return 0

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

    def _partition_options(self, *, include_expiration: bool = False) -> str:
        parts: list[str] = []
        if self._require_partition_filter:
            parts.append("require_partition_filter = TRUE")
        if include_expiration and self._partition_expiration_days is not None:
            parts.append(f"partition_expiration_days = {self._partition_expiration_days}")
        return f"\nOPTIONS({', '.join(parts)})" if parts else ""

    def _get_create_sessions_table_sql(self) -> str:
        owner_column = f",\n            {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._session_table)} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL{owner_column},
            state JSON,
            create_time TIMESTAMP NOT NULL,
            update_time TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(create_time)
        CLUSTER BY app_name, user_id, id{self._partition_options()}
        """

    def _get_create_events_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._events_table)} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            session_id STRING NOT NULL,
            invocation_id STRING,
            timestamp TIMESTAMP NOT NULL,
            event_data JSON
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY app_name, user_id, session_id{self._partition_options(include_expiration=True)}
        """

    def _get_create_app_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._app_state_table)} (
            app_name STRING NOT NULL,
            state JSON,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY app_name
        """

    def _get_create_user_states_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._user_state_table)} (
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            state JSON,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY app_name, user_id
        """

    def _get_create_metadata_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._qualified(self._metadata_table)} (
            key STRING NOT NULL,
            value STRING NOT NULL
        )
        CLUSTER BY key
        """

    def _get_seed_metadata_sql(self) -> str:
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
        for statement in (
            self._get_create_sessions_table_sql(),
            self._get_create_events_table_sql(),
            self._get_create_app_states_table_sql(),
            self._get_create_user_states_table_sql(),
            self._get_create_metadata_table_sql(),
            self._get_seed_metadata_sql(),
        ):
            self._run_query(statement)

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


def _session_record_from_row(row: "dict[str, Any]") -> SessionRecord:
    return {
        "id": row["id"],
        "app_name": row["app_name"],
        "user_id": row["user_id"],
        "state": BigQueryADKStore._decode_json(row["state"]) or {},
        "create_time": row["create_time"],
        "update_time": row["update_time"],
    }


def _event_record_from_row(row: "dict[str, Any]") -> EventRecord:
    return {
        "id": row["id"],
        "app_name": row["app_name"],
        "user_id": row["user_id"],
        "session_id": row["session_id"],
        "invocation_id": row["invocation_id"],
        "timestamp": row["timestamp"],
        "event_data": BigQueryADKStore._decode_json(row["event_data"]) or {},
    }
