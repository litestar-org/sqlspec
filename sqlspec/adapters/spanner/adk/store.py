"""Spanner ADK store."""

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Final, Protocol, cast

from google.api_core.exceptions import NotFound
from google.cloud.spanner_v1 import param_types
from typing_extensions import NotRequired, TypedDict

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.config import ADKConfig
from sqlspec.exceptions import OperationalError
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.protocols import SpannerParamTypesProtocol
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from google.cloud.spanner_v1.transaction import Transaction

    from sqlspec.extensions.adk import MemoryRecord

__all__ = ("SpannerADKConfig", "SpannerADKRetentionConfig", "SpannerSyncADKMemoryStore", "SpannerSyncADKStore")

SPANNER_PARAM_TYPES: SpannerParamTypesProtocol = cast("SpannerParamTypesProtocol", param_types)
MIN_DROP_TABLE_TOKENS: Final = 3
MIN_DROP_SEARCH_INDEX_TOKENS: Final = 4


class SpannerADKRetentionConfig(TypedDict):
    """Spanner-specific ADK row-deletion policy settings."""

    session_ttl_seconds: NotRequired[int]
    """Session row retention in seconds."""

    event_ttl_seconds: NotRequired[int]
    """Event row retention in seconds."""

    memory_ttl_seconds: NotRequired[int]
    """Memory row retention in seconds."""


class SpannerADKConfig(ADKConfig):
    """Spanner-specific ADK extension settings."""

    shard_count: NotRequired[int]
    """Generated shard count for hot key mitigation."""

    session_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK session table."""

    events_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK events table."""

    memory_table_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for the ADK memory table."""

    expires_index_options: NotRequired[str]
    """Raw Spanner OPTIONS clause content for expiration indexes."""

    retention: NotRequired[SpannerADKRetentionConfig]
    """Spanner row-deletion retention policy settings."""


class SpannerSyncADKStore(BaseSyncADKStore[SpannerSyncConfig]):
    """Spanner ADK store backed by synchronous Spanner client."""

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        adk_config = _adk_config(config)
        self._shard_count: int = int(adk_config.get("shard_count", 0)) if adk_config.get("shard_count") else 0
        self._session_table_options: str | None = adk_config.get("session_table_options")
        self._events_table_options: str | None = adk_config.get("events_table_options")
        self._expires_index_options: str | None = adk_config.get("expires_index_options")
        self._session_row_deletion_policy = _spanner_row_deletion_policy(
            adk_config, "session_ttl_seconds", "create_time"
        )
        self._events_row_deletion_policy = _spanner_row_deletion_policy(adk_config, "event_ttl_seconds", "timestamp")

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        self._create_tables()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session."""
        return self._create_session(session_id, app_name, user_id, state, owner_id)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        """Get session by ID."""
        try:
            return self._get_session(app_name, user_id, session_id, renew_for=renew_for)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._session_table):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        self._update_session_state(app_name, user_id, session_id, state)

    def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app."""
        try:
            return self._list_sessions(app_name, user_id)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._session_table):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        self._delete_session(app_name, user_id, session_id)

    def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session."""
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
        """Atomically append an event and update the session's durable state."""
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
        try:
            return self._get_events(app_name, user_id, session_id, after_timestamp, limit)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._events_table, self._session_table):
                return []
            raise

    def delete_expired_events(self, before: datetime) -> int:
        """Delete events older than a timestamp."""
        return self._delete_expired_events(before)

    def delete_idle_sessions(self, updated_before: datetime) -> int:
        """Delete sessions older than a timestamp."""
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

    def _database(self) -> "Database":
        return self._config.get_database()

    def _get_reset_drop_tables_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(super()._get_reset_drop_tables_sql(), self._existing_tables())

    def _existing_tables(self) -> "set[str]":
        database = self._database()
        return {table.table_id for table in database.list_tables()}  # type: ignore[no-untyped-call]

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            result_set = cast("Any", snapshot).execute_sql(sql, params=params, param_types=types)
            return list(result_set)

    def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._database().run_in_transaction(_SpannerWriteJob(statements))  # type: ignore[no-untyped-call]

    def _session_param_types(self, include_owner: bool) -> "dict[str, Any]":
        json_type = _json_param_type()
        types: dict[str, Any] = {
            "id": SPANNER_PARAM_TYPES.STRING,
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "state": json_type,
        }
        if include_owner and self._owner_id_column_name:
            types["owner_id"] = SPANNER_PARAM_TYPES.STRING
        return types

    def _event_param_types(self) -> "dict[str, Any]":
        json_type = _json_param_type()
        return {
            "id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "invocation_id": SPANNER_PARAM_TYPES.STRING,
            "timestamp": SPANNER_PARAM_TYPES.TIMESTAMP,
            "event_data": json_type,
        }

    def _app_state_param_types(self) -> "dict[str, Any]":
        return {"app_name": SPANNER_PARAM_TYPES.STRING, "state": _json_param_type()}

    def _user_state_param_types(self) -> "dict[str, Any]":
        return {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "state": _json_param_type(),
        }

    def _metadata_param_types(self) -> "dict[str, Any]":
        return {"key": SPANNER_PARAM_TYPES.STRING, "value": SPANNER_PARAM_TYPES.STRING}

    def _decode_state(self, raw: Any) -> Any:
        if isinstance(raw, str):
            return from_json(raw)
        return raw

    def _decode_json(self, raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, str):
            return from_json(raw)
        return raw

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        state_json = to_json(state)
        params: dict[str, Any] = {"id": session_id, "app_name": app_name, "user_id": user_id, "state": state_json}
        columns = "id, app_name, user_id, state, create_time, update_time"
        values = "@id, @app_name, @user_id, @state, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()"
        if self._owner_id_column_name:
            params["owner_id"] = owner_id
            columns = f"id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time"
            values = (
                "@id, @app_name, @user_id, @owner_id, @state, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()"
            )

        sql = f"""
            INSERT INTO {self._session_table} ({columns})
            VALUES ({values})
        """
        self._run_write([(sql, params, self._session_param_types(self._owner_id_column_name is not None))])

        return {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": state,
            "create_time": datetime.now(timezone.utc),
            "update_time": datetime.now(timezone.utc),
        }

    def _get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "SessionRecord | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            update_sql = f"""
            UPDATE {self._session_table}
            SET update_time = PENDING_COMMIT_TIMESTAMP()
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
            """
            if self._shard_count > 1:
                update_sql = f"{update_sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
            self._run_write([
                (
                    update_sql,
                    {"app_name": app_name, "user_id": user_id, "id": session_id},
                    {
                        "app_name": SPANNER_PARAM_TYPES.STRING,
                        "user_id": SPANNER_PARAM_TYPES.STRING,
                        "id": SPANNER_PARAM_TYPES.STRING,
                    },
                )
            ])

        sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time{", " + self._owner_id_column_name if self._owner_id_column_name else ""}
            FROM {self._session_table}
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        sql = f"{sql} LIMIT 1"
        params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        rows = self._run_read(
            sql,
            params,
            {
                "app_name": SPANNER_PARAM_TYPES.STRING,
                "user_id": SPANNER_PARAM_TYPES.STRING,
                "id": SPANNER_PARAM_TYPES.STRING,
            },
        )
        if not rows:
            return None

        row = rows[0]
        state_value = self._decode_state(row[3])
        record: SessionRecord = {
            "id": row[0],
            "app_name": row[1],
            "user_id": row[2],
            "state": state_value,
            "create_time": row[4],
            "update_time": row[5],
        }
        return record

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        params = {"app_name": app_name, "user_id": user_id, "id": session_id, "state": to_json(state)}
        json_type = _json_param_type()
        sql = f"""
            UPDATE {self._session_table}
            SET state = @state, update_time = PENDING_COMMIT_TIMESTAMP()
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        self._run_write([
            (
                sql,
                params,
                {
                    "app_name": SPANNER_PARAM_TYPES.STRING,
                    "user_id": SPANNER_PARAM_TYPES.STRING,
                    "id": SPANNER_PARAM_TYPES.STRING,
                    "state": json_type,
                },
            )
        ])

    def _list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
        sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time{", " + self._owner_id_column_name if self._owner_id_column_name else ""}
            FROM {self._session_table}
            WHERE app_name = @app_name
        """
        params: dict[str, Any] = {"app_name": app_name}
        types: dict[str, Any] = {"app_name": SPANNER_PARAM_TYPES.STRING}
        if user_id is not None:
            sql = f"{sql} AND user_id = @user_id"
            params["user_id"] = user_id
            types["user_id"] = SPANNER_PARAM_TYPES.STRING
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(id), {self._shard_count})"
        sql = f"{sql} ORDER BY update_time DESC"

        rows = self._run_read(sql, params, types)
        records: list[SessionRecord] = []
        for row in rows:
            state_value = self._decode_state(row[3])
            record: SessionRecord = {
                "id": row[0],
                "app_name": row[1],
                "user_id": row[2],
                "state": state_value,
                "create_time": row[4],
                "update_time": row[5],
            }
            records.append(record)
        return records

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        shard_clause = (
            f" AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})" if self._shard_count > 1 else ""
        )
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = @session_id{shard_clause}"
        delete_session_sql = (
            f"DELETE FROM {self._session_table} "
            f"WHERE app_name = @app_name AND user_id = @user_id AND id = @session_id{shard_clause}"
        )
        params = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        types = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
        }
        self._run_write([(delete_events_sql, params, types), (delete_session_sql, params, types)])

    def _append_event_and_update_state(
        self,
        event_record: "EventRecord",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> SessionRecord:
        """Atomically insert an event and update session state in one transaction.

        Both the event INSERT and the session state UPDATE execute within a single
        Spanner transaction so they succeed or fail together. A follow-up
        single-use read returns the SessionRecord; we can't capture update_time
        inside the write txn because PENDING_COMMIT_TIMESTAMP() only materialises
        on commit.

        Args:
            event_record: Event record to store.
            app_name: Application name.
            user_id: User identifier.
            session_id: Session whose state should be updated.
            state: Post-append durable state snapshot.
            app_state: Optional app-scoped state snapshot.
            user_state: Optional user-scoped state snapshot.
        """
        event_params: dict[str, Any] = {
            "id": event_record["id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": event_record["timestamp"],
            "event_data": to_json(event_record["event_data"]),
        }
        insert_sql = f"""
            INSERT INTO {self._events_table} (id, session_id, invocation_id, timestamp, event_data)
            VALUES (@id, @session_id, @invocation_id, @timestamp, @event_data)
        """

        json_type = _json_param_type()
        state_params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "id": session_id,
            "state": to_json(state),
        }
        update_sql = f"""
            UPDATE {self._session_table}
            SET state = @state, update_time = PENDING_COMMIT_TIMESTAMP()
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            update_sql = f"{update_sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"

        statements: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
            (insert_sql, event_params, self._event_param_types()),
            (
                update_sql,
                state_params,
                {
                    "app_name": SPANNER_PARAM_TYPES.STRING,
                    "user_id": SPANNER_PARAM_TYPES.STRING,
                    "id": SPANNER_PARAM_TYPES.STRING,
                    "state": json_type,
                },
            ),
        ]
        if app_state is not None:
            statements.append((
                f"""
                INSERT OR UPDATE {self._app_state_table} (app_name, state, update_time)
                VALUES (@app_name, @state, PENDING_COMMIT_TIMESTAMP())
                """,
                {"app_name": app_name, "state": to_json(app_state)},
                self._app_state_param_types(),
            ))
        if user_state is not None:
            statements.append((
                f"""
                INSERT OR UPDATE {self._user_state_table} (app_name, user_id, state, update_time)
                VALUES (@app_name, @user_id, @state, PENDING_COMMIT_TIMESTAMP())
                """,
                {"app_name": app_name, "user_id": user_id, "state": to_json(user_state)},
                self._user_state_param_types(),
            ))

        self._run_write(statements)

        record = self._get_session(app_name, user_id, session_id)
        if record is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return record

    def _insert_event(self, event_record: "EventRecord") -> None:
        event_params: dict[str, Any] = {
            "id": event_record["id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": event_record["timestamp"],
            "event_data": to_json(event_record["event_data"]),
        }
        insert_sql = f"""
            INSERT INTO {self._events_table} (id, session_id, invocation_id, timestamp, event_data)
            VALUES (@id, @session_id, @invocation_id, @timestamp, @event_data)
        """
        self._run_write([(insert_sql, event_params, self._event_param_types())])

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[EventRecord]":
        if limit == 0:
            return []

        sql = f"""
            SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
            FROM {self._events_table} e
            JOIN {self._session_table} s ON e.session_id = s.id
            WHERE s.app_name = @app_name AND s.user_id = @user_id AND e.session_id = @session_id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND e.shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        types: dict[str, Any] = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
        }
        if after_timestamp is not None:
            sql = f"{sql} AND e.timestamp > @after_timestamp"
            params["after_timestamp"] = after_timestamp
            types["after_timestamp"] = SPANNER_PARAM_TYPES.TIMESTAMP
        sql = f"{sql} ORDER BY e.timestamp ASC"
        if limit is not None:
            sql = f"{sql} LIMIT @limit"
            params["limit"] = limit
            types["limit"] = SPANNER_PARAM_TYPES.INT64
        rows = self._run_read(sql, params, types)
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "invocation_id": row[2] or "",
                "timestamp": row[3],
                "event_data": self._decode_json(row[4]) or {},
                "app_name": row[5],
                "user_id": row[6],
            }
            for row in rows
        ]

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)

    def _delete_expired_events(self, before: datetime) -> int:
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < @before"
        return int(
            cast("Any", self._database()).run_in_transaction(
                _SpannerUpdateJob(sql, {"before": before}, {"before": SPANNER_PARAM_TYPES.TIMESTAMP})
            )
        )

    def _delete_idle_sessions(self, updated_before: datetime) -> int:
        sql = f"DELETE FROM {self._session_table} WHERE update_time < @updated_before"
        return int(
            cast("Any", self._database()).run_in_transaction(
                _SpannerUpdateJob(
                    sql, {"updated_before": updated_before}, {"updated_before": SPANNER_PARAM_TYPES.TIMESTAMP}
                )
            )
        )

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = @app_name LIMIT 1"
        rows = self._run_read(sql, {"app_name": app_name}, {"app_name": SPANNER_PARAM_TYPES.STRING})
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"""
            SELECT state
            FROM {self._user_state_table}
            WHERE app_name = @app_name AND user_id = @user_id
            LIMIT 1
        """
        rows = self._run_read(
            sql,
            {"app_name": app_name, "user_id": user_id},
            {"app_name": SPANNER_PARAM_TYPES.STRING, "user_id": SPANNER_PARAM_TYPES.STRING},
        )
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        sql = f"""
            INSERT OR UPDATE {self._app_state_table} (app_name, state, update_time)
            VALUES (@app_name, @state, PENDING_COMMIT_TIMESTAMP())
        """
        self._run_write([(sql, {"app_name": app_name, "state": to_json(state)}, self._app_state_param_types())])

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        sql = f"""
            INSERT OR UPDATE {self._user_state_table} (app_name, user_id, state, update_time)
            VALUES (@app_name, @user_id, @state, PENDING_COMMIT_TIMESTAMP())
        """
        self._run_write([
            (sql, {"app_name": app_name, "user_id": user_id, "state": to_json(state)}, self._user_state_param_types())
        ])

    def _get_metadata(self, key: str) -> "str | None":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = @key LIMIT 1"
        rows = self._run_read(sql, {"key": key}, {"key": SPANNER_PARAM_TYPES.STRING})
        if not rows:
            return None
        return str(rows[0][0])

    def _set_metadata(self, key: str, value: str) -> None:
        sql = f"""
            INSERT OR UPDATE {self._metadata_table} (key, value)
            VALUES (@key, @value)
        """
        self._run_write([(sql, {"key": key, "value": value}, self._metadata_param_types())])

    def _create_tables(self) -> None:
        database = self._database()
        existing_tables = {t.table_id for t in database.list_tables()}  # type: ignore[no-untyped-call]

        ddl_statements: list[str] = []
        if self._session_table not in existing_tables:
            ddl_statements.append(self._get_create_sessions_table_sql())
        if self._events_table not in existing_tables:
            ddl_statements.append(self._get_create_events_table_sql())
        if self._app_state_table not in existing_tables:
            ddl_statements.append(self._get_create_app_states_table_sql())
        if self._user_state_table not in existing_tables:
            ddl_statements.append(self._get_create_user_states_table_sql())
        if self._metadata_table not in existing_tables:
            ddl_statements.append(self._get_create_metadata_table_sql())
        ddl_statements.extend(self._get_create_expiration_index_sql())

        if ddl_statements:
            database.update_ddl(ddl_statements).result(300)  # type: ignore[no-untyped-call]
            if self._metadata_table not in existing_tables:
                self._run_write([(self._get_seed_metadata_sql(), {}, {})])

    def _get_create_sessions_table_sql(self) -> str:
        owner_line = ""
        if self._owner_id_column_ddl:
            owner_line = f",\n  {self._owner_id_column_ddl}"
        shard_column = ""
        pk = "PRIMARY KEY (id)"
        if self._shard_count > 1:
            shard_column = f",\n  shard_id INT64 AS (MOD(FARM_FINGERPRINT(id), {self._shard_count})) STORED"
            pk = "PRIMARY KEY (shard_id, id)"
        options = ""
        if self._session_table_options:
            options = f"\nOPTIONS ({self._session_table_options})"
        return f"""
CREATE TABLE {self._session_table} (
  id STRING(128) NOT NULL,
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL{owner_line},
  state JSON NOT NULL,
  create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true){shard_column}
) {pk}{options}{self._session_row_deletion_policy}
"""

    def _get_create_events_table_sql(self) -> str:
        shard_column = ""
        pk = "PRIMARY KEY (session_id, timestamp)"
        if self._shard_count > 1:
            shard_column = f",\n  shard_id INT64 AS (MOD(FARM_FINGERPRINT(session_id), {self._shard_count})) STORED"
            pk = "PRIMARY KEY (shard_id, session_id, timestamp)"
        options = ""
        if self._events_table_options:
            options = f"\nOPTIONS ({self._events_table_options})"
        return f"""
CREATE TABLE {self._events_table} (
  id STRING(128) NOT NULL,
  session_id STRING(128) NOT NULL,
  invocation_id STRING(256),
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  event_data JSON NOT NULL{shard_column}
) {pk}{options}{self._events_row_deletion_policy}
"""

    def _get_create_app_states_table_sql(self) -> str:
        return f"""
CREATE TABLE {self._app_state_table} (
  app_name STRING(128) NOT NULL,
  state JSON NOT NULL,
  update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (app_name)
"""

    def _get_create_user_states_table_sql(self) -> str:
        return f"""
CREATE TABLE {self._user_state_table} (
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL,
  state JSON NOT NULL,
  update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (app_name, user_id)
"""

    def _get_create_metadata_table_sql(self) -> str:
        return f"""
CREATE TABLE {self._metadata_table} (
  key STRING(128) NOT NULL,
  value STRING(512) NOT NULL
) PRIMARY KEY (key)
"""

    def _get_seed_metadata_sql(self) -> str:
        return f"""
INSERT INTO {self._metadata_table} (key, value)
VALUES ('schema_version', '1')
"""

    def _get_create_expiration_index_sql(self) -> "list[str]":
        options = f" OPTIONS ({self._expires_index_options})" if self._expires_index_options else ""
        return [
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time "
            f"ON {self._session_table}(update_time){options}",
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp "
            f"ON {self._events_table}(timestamp){options}",
        ]

    def _get_drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE {self._app_state_table}"

    def _get_drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE {self._user_state_table}"

    def _get_drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE {self._metadata_table}"

    def _get_drop_tables_sql(self) -> "list[str]":
        return [
            f"DROP INDEX idx_{self._events_table}_timestamp",
            f"DROP INDEX idx_{self._session_table}_update_time",
            self._get_drop_metadata_table_sql(),
            self._get_drop_user_states_table_sql(),
            self._get_drop_app_states_table_sql(),
            f"DROP TABLE {self._events_table}",
            f"DROP TABLE {self._session_table}",
        ]


class SpannerSyncADKMemoryStore(BaseSyncADKMemoryStore[SpannerSyncConfig]):
    """Spanner ADK memory store backed by synchronous Spanner client."""

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        adk_config = _adk_config(config)
        shard_count = adk_config.get("shard_count")
        self._shard_count = int(shard_count) if isinstance(shard_count, int) else 0
        self._memory_table_options: str | None = adk_config.get("memory_table_options")
        self._memory_row_deletion_policy = _spanner_row_deletion_policy(adk_config, "memory_ttl_seconds", "inserted_at")

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        self._create_tables()

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return self._insert_memory_entries(entries, owner_id)

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        return self._search_entries(query, app_name, user_id, limit)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return self._delete_entries_by_session(session_id)

    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return self._delete_entries_older_than(days)

    def _database(self) -> "Database":
        return self._config.get_database()

    def _get_reset_drop_memory_table_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(super()._get_reset_drop_memory_table_sql(), self._existing_tables())

    def _existing_tables(self) -> "set[str]":
        database = self._database()
        return {table.table_id for table in database.list_tables()}  # type: ignore[no-untyped-call]

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            reader = cast("_SpannerReadProtocol", snapshot)
            result_set = reader.execute_sql(sql, params=params, param_types=types)
            return list(result_set)

    def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._database().run_in_transaction(_SpannerMemoryWriteJob(statements))  # type: ignore[no-untyped-call]

    def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        return int(self._database().run_in_transaction(_SpannerMemoryUpdateJob(sql, params, types)))  # type: ignore[no-untyped-call]

    def _memory_param_types(self, include_owner: bool) -> "dict[str, Any]":
        types: dict[str, Any] = {
            "id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "event_id": SPANNER_PARAM_TYPES.STRING,
            "author": SPANNER_PARAM_TYPES.STRING,
            "timestamp": SPANNER_PARAM_TYPES.TIMESTAMP,
            "content_json": _json_param_type(),
            "content_text": SPANNER_PARAM_TYPES.STRING,
            "metadata_json": _json_param_type(),
            "inserted_at": SPANNER_PARAM_TYPES.TIMESTAMP,
        }
        if include_owner and self._owner_id_column_name:
            types["owner_id"] = SPANNER_PARAM_TYPES.STRING
        return types

    def _decode_json(self, raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, str):
            return from_json(raw)
        return raw

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        database = self._database()
        existing_tables = {t.table_id for t in database.list_tables()}  # type: ignore[no-untyped-call]

        ddl_statements: list[str] = []
        if self._memory_table not in existing_tables:
            ddl_statements.extend(self._get_create_memory_table_sql())

        if ddl_statements:
            database.update_ddl(ddl_statements).result(300)  # type: ignore[no-untyped-call]

    def _get_create_memory_table_sql(self) -> "list[str]":
        owner_line = ""
        if self._owner_id_column_ddl:
            owner_line = f",\n  {self._owner_id_column_ddl}"

        fts_column_line = ""
        fts_index = ""
        if self._use_fts:
            fts_column_line = "\n  content_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(content_text)) HIDDEN"
            fts_index = f"CREATE SEARCH INDEX idx_{self._memory_table}_fts ON {self._memory_table}(content_tokens)"

        shard_column = ""
        pk = "PRIMARY KEY (id)"
        if self._shard_count > 1:
            shard_column = f",\n  shard_id INT64 AS (MOD(FARM_FINGERPRINT(id), {self._shard_count})) STORED"
            pk = "PRIMARY KEY (shard_id, id)"
        options = ""
        if self._memory_table_options:
            options = f"\nOPTIONS ({self._memory_table_options})"

        table_sql = f"""
CREATE TABLE {self._memory_table} (
  id STRING(128) NOT NULL,
  session_id STRING(128) NOT NULL,
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL,
  event_id STRING(128) NOT NULL,
  author STRING(256){owner_line},
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  content_json JSON NOT NULL,
  content_text STRING(MAX) NOT NULL,
  metadata_json JSON,
  inserted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true){fts_column_line}{shard_column}
) {pk}{options}{self._memory_row_deletion_policy}
"""

        app_user_idx = (
            f"CREATE INDEX idx_{self._memory_table}_app_user_time "
            f"ON {self._memory_table}(app_name, user_id, timestamp DESC)"
        )
        session_idx = f"CREATE INDEX idx_{self._memory_table}_session ON {self._memory_table}(session_id)"

        statements = [table_sql, app_user_idx, session_idx]
        if fts_index:
            statements.append(fts_index)
        return statements

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get SQL to drop the memory table and its indexes.

        Returns:
            List of SQL statements to drop the memory table and associated indexes.
        """
        statements: list[str] = []
        if self._use_fts:
            statements.append(f"DROP SEARCH INDEX idx_{self._memory_table}_fts")
        statements.extend([
            f"DROP INDEX idx_{self._memory_table}_session",
            f"DROP INDEX idx_{self._memory_table}_app_user_time",
            f"DROP TABLE {self._memory_table}",
        ])
        return statements

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        inserted_count = 0
        statements: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", @owner_id" if self._owner_id_column_name else ""

        insert_sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, event_id, author{owner_column},
            timestamp, content_json, content_text, metadata_json, inserted_at
        ) VALUES (
            @id, @session_id, @app_name, @user_id, @event_id, @author{owner_param},
            @timestamp, @content_json, @content_text, @metadata_json, @inserted_at
        )
        """

        for entry in entries:
            if self._event_exists(entry["event_id"]):
                continue
            params = {
                "id": entry["id"],
                "session_id": entry["session_id"],
                "app_name": entry["app_name"],
                "user_id": entry["user_id"],
                "event_id": entry["event_id"],
                "author": entry["author"],
                "timestamp": entry["timestamp"],
                "content_json": to_json(entry["content_json"]),
                "content_text": entry["content_text"],
                "metadata_json": to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
                "inserted_at": entry["inserted_at"],
            }
            if self._owner_id_column_name:
                params["owner_id"] = str(owner_id) if owner_id is not None else None
            statements.append((insert_sql, params, self._memory_param_types(self._owner_id_column_name is not None)))
            inserted_count += 1

        if statements:
            self._run_write(statements)
        return inserted_count

    def _event_exists(self, event_id: str) -> bool:
        sql = f"SELECT event_id FROM {self._memory_table} WHERE event_id = @event_id LIMIT 1"
        rows = self._run_read(sql, {"event_id": event_id}, {"event_id": SPANNER_PARAM_TYPES.STRING})
        return bool(rows)

    def _search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        if self._use_fts:
            return self._search_entries_fts(query, app_name, user_id, effective_limit)
        return self._search_entries_simple(query, app_name, user_id, effective_limit)

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = @app_name
          AND user_id = @user_id
          AND SEARCH(content_tokens, @query)
        ORDER BY timestamp DESC
        LIMIT @limit
        """
        params = {"app_name": app_name, "user_id": user_id, "query": query, "limit": limit}
        types = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "query": SPANNER_PARAM_TYPES.STRING,
            "limit": SPANNER_PARAM_TYPES.INT64,
        }
        rows = self._run_read(sql, params, types)
        return self._rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM {self._memory_table}
        WHERE app_name = @app_name
          AND user_id = @user_id
          AND LOWER(content_text) LIKE @pattern
        ORDER BY timestamp DESC
        LIMIT @limit
        """
        pattern = f"%{query.lower()}%"
        params = {"app_name": app_name, "user_id": user_id, "pattern": pattern, "limit": limit}
        types = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "pattern": SPANNER_PARAM_TYPES.STRING,
            "limit": SPANNER_PARAM_TYPES.INT64,
        }
        rows = self._run_read(sql, params, types)
        return self._rows_to_records(rows)

    def _delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = @session_id"
        params = {"session_id": session_id}
        types = {"session_id": SPANNER_PARAM_TYPES.STRING}
        return self._execute_update(sql, params, types)

    def _delete_entries_older_than(self, days: int) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        sql = f"DELETE FROM {self._memory_table} WHERE inserted_at < @cutoff"
        params = {"cutoff": cutoff}
        types = {"cutoff": SPANNER_PARAM_TYPES.TIMESTAMP}
        return self._execute_update(sql, params, types)

    def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": row[6],
                "content_json": self._decode_json(row[7]),
                "content_text": row[8],
                "metadata_json": self._decode_json(row[9]),
                "inserted_at": row[10],
            }
            for row in rows
        ]


def _json_param_type() -> Any:
    try:
        return SPANNER_PARAM_TYPES.JSON
    except AttributeError:
        return SPANNER_PARAM_TYPES.STRING


def _spanner_ttl_days(ttl_seconds: Any) -> int:
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        return 0
    return max(1, (ttl_seconds + 86_399) // 86_400)


def _spanner_row_deletion_policy(adk_config: Mapping[str, Any], ttl_key: str, column: str) -> str:
    retention = adk_config.get("retention")
    if not isinstance(retention, dict):
        return ""
    ttl_days = _spanner_ttl_days(retention.get(ttl_key))
    if ttl_days == 0:
        return ""
    return f"\nROW DELETION POLICY (OLDER_THAN({column}, INTERVAL {ttl_days} DAY))"


def _adk_config(config: Any) -> SpannerADKConfig:
    """Return Spanner ADK extension settings from ``extension_config["adk"]``."""
    extension_config = getattr(config, "extension_config", {})
    if not isinstance(extension_config, dict):
        return {}
    adk_config = extension_config.get("adk", {})
    if not isinstance(adk_config, dict):
        return {}
    return cast("SpannerADKConfig", adk_config)


def _is_spanner_table_missing(exc: NotFound, *table_names: str) -> bool:
    message = str(exc).lower()
    return "not found" in message and any(table_name.lower() in message for table_name in table_names)


def _filter_existing_spanner_drops(statements: "list[str]", existing_tables: "set[str]") -> "list[str]":
    return [statement for statement in statements if _spanner_drop_statement_table(statement, existing_tables)]


def _spanner_drop_statement_table(statement: str, existing_tables: "set[str]") -> "str | None":
    tokens = statement.strip().split()
    if len(tokens) >= MIN_DROP_TABLE_TOKENS and tokens[0].upper() == "DROP" and tokens[1].upper() == "TABLE":
        table_name = tokens[2]
        return table_name if table_name in existing_tables else None

    index_name: str | None = None
    if len(tokens) >= MIN_DROP_TABLE_TOKENS and tokens[0].upper() == "DROP" and tokens[1].upper() == "INDEX":
        index_name = tokens[2]
    if (
        len(tokens) >= MIN_DROP_SEARCH_INDEX_TOKENS
        and tokens[0].upper() == "DROP"
        and tokens[1].upper() == "SEARCH"
        and tokens[2].upper() == "INDEX"
    ):
        index_name = tokens[3]
    if index_name is None:
        return None

    for table_name in existing_tables:
        if index_name.startswith(f"idx_{table_name}_"):
            return table_name
    return None


class _SpannerWriteJob:
    __slots__ = ("_statements",)

    def __init__(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._statements = statements

    def __call__(self, transaction: "Transaction") -> None:
        if len(self._statements) > 1:
            status, _row_counts = transaction.batch_update(self._statements)  # type: ignore[no-untyped-call]
            if status.code != 0:
                msg = f"Spanner batch update failed (code {status.code}): {status.message}"
                raise OperationalError(msg)
            return
        for sql, params, types in self._statements:
            transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]


class _SpannerMemoryWriteJob:
    __slots__ = ("_statements",)

    def __init__(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._statements = statements

    def __call__(self, transaction: "Transaction") -> None:
        if len(self._statements) > 1:
            status, _row_counts = transaction.batch_update(self._statements)  # type: ignore[no-untyped-call]
            if status.code != 0:
                msg = f"Spanner batch update failed (code {status.code}): {status.message}"
                raise OperationalError(msg)
            return
        for sql, params, types in self._statements:
            transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]


class _SpannerUpdateJob:
    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    def __call__(self, transaction: "Transaction") -> int:
        return int(transaction.execute_update(self._sql, params=self._params, param_types=self._types))  # type: ignore[no-untyped-call]


class _SpannerMemoryUpdateJob:
    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    def __call__(self, transaction: "Transaction") -> int:
        return int(transaction.execute_update(self._sql, params=self._params, param_types=self._types))  # type: ignore[no-untyped-call]


class _SpannerReadProtocol(Protocol):
    def execute_sql(
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> Iterable[Any]: ...
