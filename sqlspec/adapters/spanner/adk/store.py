"""Spanner ADK store."""

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast

import sqlglot
from sqlglot import exp
from typing_extensions import NotRequired, TypedDict

import sqlspec.dialects.spanner  # noqa: F401
from sqlspec.adapters.spanner._typing import SpannerNotFound as NotFound
from sqlspec.adapters.spanner._typing import spanner_param_types as param_types
from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.core import (
    execute_ddl_async,
    execute_ddl_sync,
    list_existing_table_names_async,
    list_existing_table_names_sync,
)
from sqlspec.config import ADKConfig
from sqlspec.exceptions import OperationalError
from sqlspec.extensions.adk import (
    BaseAsyncADKStore,
    BaseSyncADKStore,
    StoredEvent,
    StoredSession,
    normalize_session_list_options,
)
from sqlspec.extensions.adk._table_utils import ensure_table_name
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.protocols import SpannerParamTypesProtocol
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.spanner._typing import (
        SpannerAsyncDatabase,
        SpannerAsyncTransaction,
        SpannerDatabase,
        SpannerTransaction,
    )
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory

__all__ = (
    "SpannerADKConfig",
    "SpannerADKRetentionConfig",
    "SpannerAsyncADKMemoryStore",
    "SpannerAsyncADKStore",
    "SpannerSyncADKMemoryStore",
    "SpannerSyncADKStore",
)

SPANNER_PARAM_TYPES: SpannerParamTypesProtocol = cast("SpannerParamTypesProtocol", param_types)
_DDL_TIMEOUT_SECONDS = 300
_EMBEDDING_ROW_INDEX = 12
_SPANNER_DISTANCE_FUNCTIONS: dict[str, str] = {
    "COSINE": "COSINE_DISTANCE",
    "EUCLIDEAN": "EUCLIDEAN_DISTANCE",
    "DOT_PRODUCT": "DOT_PRODUCT",
}


class SpannerADKRetentionConfig(TypedDict):
    """Spanner-specific ADK row-deletion policy settings."""

    session_ttl_seconds: NotRequired[int]
    """Session row retention in seconds."""

    event_ttl_seconds: NotRequired[int]
    """Event row retention in seconds."""

    memory_ttl_seconds: NotRequired[int]
    """Memory row retention in seconds."""

    artifact_ttl_seconds: NotRequired[int]
    """Artifact row retention in seconds."""


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

    vector_dimensions: NotRequired[int | None]
    """Optional fixed dimensionality for the memory embedding column."""

    vector_distance_type: NotRequired[str]
    """Distance metric for the memory vector index ('COSINE', 'EUCLIDEAN', 'DOT_PRODUCT')."""

    vector_index_enabled: NotRequired[bool]
    """Whether to emit CREATE VECTOR INDEX for the memory table."""

    scann_tree_depth: NotRequired[int]
    """ScaNN tree depth for the memory vector index (2 or 3)."""

    scann_num_leaves: NotRequired[int]
    """ScaNN leaf count for the memory vector index."""

    enable_hybrid_search: NotRequired[bool]
    """Enable Reciprocal Rank Fusion when both embedding and FTS query are supplied."""

    enable_memory_graph: NotRequired[bool]
    """Emit CREATE OR REPLACE PROPERTY GRAPH over the ADK memory table."""

    memory_graph_name: NotRequired[str]
    """Name of the Spanner Property Graph created over the ADK memory table."""


class _SpannerADKStoreMixin:
    """Shared SQL, DDL, parameter type, and decoding helpers for Spanner ADK stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _session_table: str
        _events_table: str
        _app_state_table: str
        _user_state_table: str
        _metadata_table: str
        _owner_id_column_ddl: str | None
        _owner_id_column_name: str | None
        _shard_count: int
        _session_table_options: str | None
        _events_table_options: str | None
        _expires_index_options: str | None
        _session_row_deletion_policy: str
        _events_row_deletion_policy: str

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
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
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

    def _build_create_session_statement(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
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
        return (sql, params, self._session_param_types(self._owner_id_column_name is not None))

    def _build_renew_session_statement(
        self, app_name: str, user_id: str, session_id: str
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        update_sql = f"""
        UPDATE {self._session_table}
        SET update_time = PENDING_COMMIT_TIMESTAMP()
        WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            update_sql = f"{update_sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        return (
            update_sql,
            {"app_name": app_name, "user_id": user_id, "id": session_id},
            {
                "app_name": SPANNER_PARAM_TYPES.STRING,
                "user_id": SPANNER_PARAM_TYPES.STRING,
                "id": SPANNER_PARAM_TYPES.STRING,
            },
        )

    def _build_get_session_query(
        self, app_name: str, user_id: str, session_id: str
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time{", " + self._owner_id_column_name if self._owner_id_column_name else ""}
            FROM {self._session_table}
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        sql = f"{sql} LIMIT 1"
        params = {"app_name": app_name, "user_id": user_id, "id": session_id}
        types = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "id": SPANNER_PARAM_TYPES.STRING,
        }
        return (sql, params, types)

    def _decode_session_row(self, row: Any) -> StoredSession:
        state_value = self._decode_state(row[3])
        return {
            "id": row[0],
            "app_name": row[1],
            "user_id": row[2],
            "state": state_value,
            "create_time": row[4],
            "update_time": row[5],
        }

    def _build_update_session_state_statement(
        self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        params = {"app_name": app_name, "user_id": user_id, "id": session_id, "state": to_json(state)}
        json_type = _json_param_type()
        sql = f"""
            UPDATE {self._session_table}
            SET state = @state, update_time = PENDING_COMMIT_TIMESTAMP()
            WHERE app_name = @app_name AND user_id = @user_id AND id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        return (
            sql,
            params,
            {
                "app_name": SPANNER_PARAM_TYPES.STRING,
                "user_id": SPANNER_PARAM_TYPES.STRING,
                "id": SPANNER_PARAM_TYPES.STRING,
                "state": json_type,
            },
        )

    def _build_list_sessions_query(
        self,
        app_name: str,
        user_id: "str | None" = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: "int | None" = None,
        offset: "int | None" = None,
    ) -> "tuple[str, dict[str, Any], dict[str, Any]] | None":
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return None

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
        sql = f"{sql} ORDER BY {column} {direction}, id {direction}"
        if page_limit is not None:
            sql = f"{sql} LIMIT @limit OFFSET @offset"
            params["limit"] = page_limit
            params["offset"] = page_offset
            types["limit"] = SPANNER_PARAM_TYPES.INT64
            types["offset"] = SPANNER_PARAM_TYPES.INT64
        return (sql, params, types)

    def _build_delete_session_statements(
        self, app_name: str, user_id: str, session_id: str
    ) -> "list[tuple[str, dict[str, Any], dict[str, Any]]]":
        shard_clause = (
            f" AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})" if self._shard_count > 1 else ""
        )
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = @session_id{shard_clause}"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE app_name = @app_name AND user_id = @user_id AND id = @session_id{shard_clause}"
        params = {"app_name": app_name, "user_id": user_id, "session_id": session_id}
        types = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
        }
        return [(delete_events_sql, params, types), (delete_session_sql, params, types)]

    def _build_insert_event_statement(
        self, event_record: "StoredEvent"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        event_params: dict[str, Any] = {
            "id": event_record["id"],
            "app_name": event_record["app_name"],
            "user_id": event_record["user_id"],
            "session_id": event_record["session_id"],
            "invocation_id": event_record["invocation_id"],
            "timestamp": event_record["timestamp"],
            "event_data": to_json(event_record["event_data"]),
        }
        insert_sql = f"""
            INSERT INTO {self._events_table} (id, app_name, user_id, session_id, invocation_id, timestamp, event_data)
            VALUES (@id, @app_name, @user_id, @session_id, @invocation_id, @timestamp, @event_data)
        """
        return (insert_sql, event_params, self._event_param_types())

    def _build_append_event_and_update_state_statements(
        self,
        event_record: "StoredEvent",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> "list[tuple[str, dict[str, Any], dict[str, Any]]]":
        statements: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
            self._build_insert_event_statement(event_record),
            self._build_update_session_state_statement(app_name, user_id, session_id, state),
        ]
        if app_state is not None:
            statements.append(self._build_upsert_app_state_statement(app_name, app_state))
        if user_state is not None:
            statements.append(self._build_upsert_user_state_statement(app_name, user_id, user_state))
        return statements

    def _build_get_events_query(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "tuple[str, dict[str, Any], dict[str, Any]] | None":
        if limit == 0:
            return None

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
        return (sql, params, types)

    def _decode_event_rows(self, rows: "Iterable[Any]") -> "list[StoredEvent]":
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

    def _build_delete_expired_events_statement(
        self, before: datetime, app_name: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"DELETE FROM {self._events_table} WHERE timestamp < @before"
        params: dict[str, Any] = {"before": before}
        types: dict[str, Any] = {"before": SPANNER_PARAM_TYPES.TIMESTAMP}
        if app_name is not None:
            sql += " AND app_name = @app_name"
            params["app_name"] = app_name
            types["app_name"] = SPANNER_PARAM_TYPES.STRING
        return (sql, params, types)

    def _build_delete_idle_sessions_statement(
        self, updated_before: datetime, app_name: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"DELETE FROM {self._session_table} WHERE update_time < @updated_before"
        params: dict[str, Any] = {"updated_before": updated_before}
        types: dict[str, Any] = {"updated_before": SPANNER_PARAM_TYPES.TIMESTAMP}
        if app_name is not None:
            sql += " AND app_name = @app_name"
            params["app_name"] = app_name
            types["app_name"] = SPANNER_PARAM_TYPES.STRING
        return (sql, params, types)

    def _build_delete_idle_user_states_statement(
        self, updated_before: datetime, app_name: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"DELETE FROM {self._user_state_table} WHERE update_time < @updated_before"
        params: dict[str, Any] = {"updated_before": updated_before}
        types: dict[str, Any] = {"updated_before": SPANNER_PARAM_TYPES.TIMESTAMP}
        if app_name is not None:
            sql += " AND app_name = @app_name"
            params["app_name"] = app_name
            types["app_name"] = SPANNER_PARAM_TYPES.STRING
        return (sql, params, types)

    def _build_get_app_state_query(self, app_name: str) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = @app_name LIMIT 1"
        return (sql, {"app_name": app_name}, {"app_name": SPANNER_PARAM_TYPES.STRING})

    def _build_get_user_state_query(self, app_name: str, user_id: str) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"""
            SELECT state
            FROM {self._user_state_table}
            WHERE app_name = @app_name AND user_id = @user_id
            LIMIT 1
        """
        return (
            sql,
            {"app_name": app_name, "user_id": user_id},
            {"app_name": SPANNER_PARAM_TYPES.STRING, "user_id": SPANNER_PARAM_TYPES.STRING},
        )

    def _build_upsert_app_state_statement(
        self, app_name: str, state: "dict[str, Any]"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"""
            INSERT OR UPDATE {self._app_state_table} (app_name, state, update_time)
            VALUES (@app_name, @state, PENDING_COMMIT_TIMESTAMP())
        """
        return (sql, {"app_name": app_name, "state": to_json(state)}, self._app_state_param_types())

    def _build_upsert_user_state_statement(
        self, app_name: str, user_id: str, state: "dict[str, Any]"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"""
            INSERT OR UPDATE {self._user_state_table} (app_name, user_id, state, update_time)
            VALUES (@app_name, @user_id, @state, PENDING_COMMIT_TIMESTAMP())
        """
        return (
            sql,
            {"app_name": app_name, "user_id": user_id, "state": to_json(state)},
            self._user_state_param_types(),
        )

    def _build_get_metadata_query(self, key: str) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"SELECT value FROM {self._metadata_table} WHERE key = @key LIMIT 1"
        return (sql, {"key": key}, {"key": SPANNER_PARAM_TYPES.STRING})

    def _build_set_metadata_statement(self, key: str, value: str) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"""
            INSERT OR UPDATE {self._metadata_table} (key, value)
            VALUES (@key, @value)
        """
        return (sql, {"key": key, "value": value}, self._metadata_param_types())

    def _build_sessions_table_ddl(self) -> str:
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

    def _build_events_table_ddl(self) -> str:
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
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL,
  session_id STRING(128) NOT NULL,
  invocation_id STRING(256),
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  event_data JSON NOT NULL{shard_column}
) {pk}{options}{self._events_row_deletion_policy}
"""

    def _build_app_states_table_ddl(self) -> str:
        return f"""
CREATE TABLE {self._app_state_table} (
  app_name STRING(128) NOT NULL,
  state JSON NOT NULL,
  update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (app_name)
"""

    def _build_user_states_table_ddl(self) -> str:
        return f"""
CREATE TABLE {self._user_state_table} (
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL,
  state JSON NOT NULL,
  update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (app_name, user_id)
"""

    def _build_metadata_table_ddl(self) -> str:
        return f"""
CREATE TABLE {self._metadata_table} (
  key STRING(128) NOT NULL,
  value STRING(512) NOT NULL
) PRIMARY KEY (key)
"""

    def _expiration_index_ddl(self) -> "list[str]":
        options = f" OPTIONS ({self._expires_index_options})" if self._expires_index_options else ""
        return [
            f"CREATE INDEX IF NOT EXISTS idx_{self._session_table}_update_time ON {self._session_table}(update_time){options}",
            f"CREATE INDEX IF NOT EXISTS idx_{self._events_table}_timestamp ON {self._events_table}(timestamp){options}",
        ]

    def _missing_table_ddl_statements(self, existing_tables: "set[str]") -> "list[str]":
        ddl_statements: list[str] = []
        if self._session_table not in existing_tables:
            ddl_statements.append(self._build_sessions_table_ddl())
        if self._events_table not in existing_tables:
            ddl_statements.append(self._build_events_table_ddl())
        if self._app_state_table not in existing_tables:
            ddl_statements.append(self._build_app_states_table_ddl())
        if self._user_state_table not in existing_tables:
            ddl_statements.append(self._build_user_states_table_ddl())
        if self._metadata_table not in existing_tables:
            ddl_statements.append(self._build_metadata_table_ddl())
        ddl_statements.extend(self._expiration_index_ddl())
        return ddl_statements

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE {self._metadata_table}"

    def _drop_tables_sql(self) -> "list[str]":
        return [
            f"DROP INDEX idx_{self._events_table}_timestamp",
            f"DROP INDEX idx_{self._session_table}_update_time",
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE {self._events_table}",
            f"DROP TABLE {self._session_table}",
        ]


class SpannerSyncADKStore(_SpannerADKStoreMixin, BaseSyncADKStore[SpannerSyncConfig]):
    """Spanner ADK store backed by synchronous Spanner client."""

    __slots__ = (
        "_events_row_deletion_policy",
        "_events_table_options",
        "_expires_index_options",
        "_session_row_deletion_policy",
        "_session_table_options",
        "_shard_count",
    )

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        (
            self._shard_count,
            self._session_table_options,
            self._events_table_options,
            self._expires_index_options,
            self._session_row_deletion_policy,
            self._events_row_deletion_policy,
        ) = _extract_spanner_adk_options(config)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new session."""
        return self._create_session(session_id, app_name, user_id, state, owner_id)

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
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
        try:
            return self._list_sessions(
                app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
            )
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._session_table):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        self._delete_session(app_name, user_id, session_id)

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        self._append_event(event_record)

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> StoredSession:
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
    ) -> "list[StoredEvent]":
        """Get events for a session."""
        try:
            return self._get_events(app_name, user_id, session_id, after_timestamp, limit)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._events_table, self._session_table):
                return []
            raise

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than a timestamp, optionally scoped to one application."""
        return self._delete_expired_events(before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete sessions older than a timestamp, optionally scoped to one application."""
        return self._delete_idle_sessions(updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user-scoped state rows older than a timestamp, optionally scoped to one application."""
        return self._delete_idle_user_states(updated_before, app_name)

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

    def _database(self) -> "SpannerDatabase":
        return self._config.get_database()

    def _reset_drop_tables_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(self._reset_drop_statements(), self._existing_tables())

    def _existing_tables(self) -> "set[str]":
        return list_existing_table_names_sync(self._database())

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            reader = cast("_SpannerReadProtocol", snapshot)
            return list(reader.execute_sql(sql, params=params, param_types=types))

    def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        cast("Any", self._database()).run_in_transaction(_SpannerSyncWriteJob(statements))

    def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        return int(cast("Any", self._database()).run_in_transaction(_SpannerSyncUpdateJob(sql, params, types)))

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        self._run_write([self._build_create_session_statement(session_id, app_name, user_id, state, owner_id)])
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
    ) -> "StoredSession | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            self._run_write([self._build_renew_session_statement(app_name, user_id, session_id)])

        sql, params, types = self._build_get_session_query(app_name, user_id, session_id)
        rows = self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_session_row(rows[0])

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        self._run_write([self._build_update_session_state_statement(app_name, user_id, session_id, state)])

    def _list_sessions(
        self,
        app_name: str,
        user_id: "str | None" = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: "int | None" = None,
        offset: "int | None" = None,
    ) -> "list[StoredSession]":
        query = self._build_list_sessions_query(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if query is None:
            return []
        sql, params, types = query
        rows = self._run_read(sql, params, types)
        return [self._decode_session_row(row) for row in rows]

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        self._run_write(self._build_delete_session_statements(app_name, user_id, session_id))

    def _append_event_and_update_state(
        self,
        event_record: "StoredEvent",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Atomically insert an event and update session state in one transaction."""
        statements = self._build_append_event_and_update_state_statements(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )
        self._run_write(statements)

        record = self._get_session(app_name, user_id, session_id)
        if record is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return record

    def _insert_event(self, event_record: "StoredEvent") -> None:
        self._run_write([self._build_insert_event_statement(event_record)])

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        query = self._build_get_events_query(app_name, user_id, session_id, after_timestamp, limit)
        if query is None:
            return []
        sql, params, types = query
        rows = self._run_read(sql, params, types)
        return self._decode_event_rows(rows)

    def _append_event(self, event_record: StoredEvent) -> None:
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)

    def _delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_expired_events_statement(before, app_name)
        return self._execute_update(sql, params, types)

    def _delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_idle_sessions_statement(updated_before, app_name)
        return self._execute_update(sql, params, types)

    def _delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_idle_user_states_statement(updated_before, app_name)
        return self._execute_update(sql, params, types)

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql, params, types = self._build_get_app_state_query(app_name)
        rows = self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql, params, types = self._build_get_user_state_query(app_name, user_id)
        rows = self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        self._run_write([self._build_upsert_app_state_statement(app_name, state)])

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        self._run_write([self._build_upsert_user_state_statement(app_name, user_id, state)])

    def _get_metadata(self, key: str) -> "str | None":
        sql, params, types = self._build_get_metadata_query(key)
        rows = self._run_read(sql, params, types)
        if not rows:
            return None
        return str(rows[0][0])

    def _set_metadata(self, key: str, value: str) -> None:
        self._run_write([self._build_set_metadata_statement(key, value)])

    def _create_tables(self) -> None:
        ddl_statements = self._missing_table_ddl_statements(self._existing_tables())
        execute_ddl_sync(self._database(), ddl_statements, timeout=_DDL_TIMEOUT_SECONDS)

    def _sessions_table_ddl(self) -> str:
        return self._build_sessions_table_ddl()

    def _events_table_ddl(self) -> str:
        return self._build_events_table_ddl()

    def _app_states_table_ddl(self) -> str:
        return self._build_app_states_table_ddl()

    def _user_states_table_ddl(self) -> str:
        return self._build_user_states_table_ddl()

    def _metadata_table_ddl(self) -> str:
        return self._build_metadata_table_ddl()


class SpannerAsyncADKStore(_SpannerADKStoreMixin, BaseAsyncADKStore[SpannerAsyncConfig]):
    """Spanner ADK store backed by asynchronous Spanner client."""

    __slots__ = (
        "_events_row_deletion_policy",
        "_events_table_options",
        "_expires_index_options",
        "_session_row_deletion_policy",
        "_session_table_options",
        "_shard_count",
    )

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerAsyncConfig) -> None:
        super().__init__(config)
        (
            self._shard_count,
            self._session_table_options,
            self._events_table_options,
            self._expires_index_options,
            self._session_row_deletion_policy,
            self._events_row_deletion_policy,
        ) = _extract_spanner_adk_options(config)

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        await self._create_tables()

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new session."""
        return await self._create_session(session_id, app_name, user_id, state, owner_id)

    async def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Get session by ID."""
        try:
            return await self._get_session(app_name, user_id, session_id, renew_for=renew_for)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._session_table):
                return None
            raise

    async def update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state."""
        await self._update_session_state(app_name, user_id, session_id, state)

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
        """List sessions for an app."""
        try:
            return await self._list_sessions(
                app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
            )
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._session_table):
                return []
            raise

    async def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and associated events."""
        await self._delete_session(app_name, user_id, session_id)

    async def append_event(self, event_record: StoredEvent) -> None:
        """Append an event to a session."""
        await self._append_event(event_record)

    async def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Atomically append an event and update the session's durable state."""
        return await self._append_event_and_update_state(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )

    async def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        """Get events for a session."""
        try:
            return await self._get_events(app_name, user_id, session_id, after_timestamp, limit)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._events_table, self._session_table):
                return []
            raise

    async def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than a timestamp, optionally scoped to one application."""
        return await self._delete_expired_events(before, app_name)

    async def delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete sessions older than a timestamp, optionally scoped to one application."""
        return await self._delete_idle_sessions(updated_before, app_name)

    async def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user-scoped state rows older than a timestamp, optionally scoped to one application."""
        return await self._delete_idle_user_states(updated_before, app_name)

    async def get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        """Return app-scoped state."""
        return await self._get_app_state(app_name)

    async def get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        """Return user-scoped state."""
        return await self._get_user_state(app_name, user_id)

    async def upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        """Insert or replace app-scoped state."""
        await self._upsert_app_state(app_name, state)

    async def upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        """Insert or replace user-scoped state."""
        await self._upsert_user_state(app_name, user_id, state)

    async def get_metadata(self, key: str) -> "str | None":
        """Return a metadata value."""
        return await self._get_metadata(key)

    async def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata value."""
        await self._set_metadata(key, value)

    async def _database(self) -> "SpannerAsyncDatabase":
        return await self._config.get_database()

    async def _reset_drop_tables_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(self._reset_drop_statements(), await self._existing_tables())

    async def _existing_tables(self) -> "set[str]":
        return await list_existing_table_names_async(await self._database())

    async def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        return await _run_read_async(await self._database(), sql, params, types)

    async def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        database = await self._database()
        await database.run_in_transaction(_SpannerAsyncWriteJob(statements))

    async def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        database = await self._database()
        return int(await database.run_in_transaction(_SpannerAsyncUpdateJob(sql, params, types)))

    async def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        await self._run_write([self._build_create_session_statement(session_id, app_name, user_id, state, owner_id)])
        return {
            "id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "state": state,
            "create_time": datetime.now(timezone.utc),
            "update_time": datetime.now(timezone.utc),
        }

    async def _get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            await self._run_write([self._build_renew_session_statement(app_name, user_id, session_id)])

        sql, params, types = self._build_get_session_query(app_name, user_id, session_id)
        rows = await self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_session_row(rows[0])

    async def _update_session_state(
        self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]"
    ) -> None:
        await self._run_write([self._build_update_session_state_statement(app_name, user_id, session_id, state)])

    async def _list_sessions(
        self,
        app_name: str,
        user_id: "str | None" = None,
        *,
        order_by: "SessionOrderBy" = "update_time",
        descending: bool = True,
        limit: "int | None" = None,
        offset: "int | None" = None,
    ) -> "list[StoredSession]":
        query = self._build_list_sessions_query(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if query is None:
            return []
        sql, params, types = query
        rows = await self._run_read(sql, params, types)
        return [self._decode_session_row(row) for row in rows]

    async def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        await self._run_write(self._build_delete_session_statements(app_name, user_id, session_id))

    async def _append_event_and_update_state(
        self,
        event_record: "StoredEvent",
        app_name: str,
        user_id: str,
        session_id: str,
        state: "dict[str, Any]",
        *,
        app_state: "dict[str, Any] | None" = None,
        user_state: "dict[str, Any] | None" = None,
    ) -> StoredSession:
        """Atomically insert an event and update session state in one transaction."""
        statements = self._build_append_event_and_update_state_statements(
            event_record, app_name, user_id, session_id, state, app_state=app_state, user_state=user_state
        )
        await self._run_write(statements)

        record = await self._get_session(app_name, user_id, session_id)
        if record is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)
        return record

    async def _insert_event(self, event_record: "StoredEvent") -> None:
        await self._run_write([self._build_insert_event_statement(event_record)])

    async def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        query = self._build_get_events_query(app_name, user_id, session_id, after_timestamp, limit)
        if query is None:
            return []
        sql, params, types = query
        rows = await self._run_read(sql, params, types)
        return self._decode_event_rows(rows)

    async def _append_event(self, event_record: StoredEvent) -> None:
        """Asynchronous implementation of append_event."""
        await self._insert_event(event_record)

    async def _delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_expired_events_statement(before, app_name)
        return await self._execute_update(sql, params, types)

    async def _delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_idle_sessions_statement(updated_before, app_name)
        return await self._execute_update(sql, params, types)

    async def _delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        sql, params, types = self._build_delete_idle_user_states_statement(updated_before, app_name)
        return await self._execute_update(sql, params, types)

    async def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql, params, types = self._build_get_app_state_query(app_name)
        rows = await self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    async def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql, params, types = self._build_get_user_state_query(app_name, user_id)
        rows = await self._run_read(sql, params, types)
        if not rows:
            return None
        return self._decode_json(rows[0][0]) or {}

    async def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        await self._run_write([self._build_upsert_app_state_statement(app_name, state)])

    async def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        await self._run_write([self._build_upsert_user_state_statement(app_name, user_id, state)])

    async def _get_metadata(self, key: str) -> "str | None":
        sql, params, types = self._build_get_metadata_query(key)
        rows = await self._run_read(sql, params, types)
        if not rows:
            return None
        return str(rows[0][0])

    async def _set_metadata(self, key: str, value: str) -> None:
        await self._run_write([self._build_set_metadata_statement(key, value)])

    async def _create_tables(self) -> None:
        ddl_statements = self._missing_table_ddl_statements(await self._existing_tables())
        await execute_ddl_async(await self._database(), ddl_statements, timeout=_DDL_TIMEOUT_SECONDS)

    async def _sessions_table_ddl(self) -> str:
        return self._build_sessions_table_ddl()

    async def _events_table_ddl(self) -> str:
        return self._build_events_table_ddl()

    async def _app_states_table_ddl(self) -> str:
        return self._build_app_states_table_ddl()

    async def _user_states_table_ddl(self) -> str:
        return self._build_user_states_table_ddl()

    async def _metadata_table_ddl(self) -> str:
        return self._build_metadata_table_ddl()


class _SpannerADKMemoryStoreMixin:
    """Shared SQL, DDL, parameter type, and decoding helpers for Spanner ADK memory stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _memory_table: str
        _enabled: bool
        _max_results: int
        _use_fts: bool
        _owner_id_column_ddl: str | None
        _owner_id_column_name: str | None
        _shard_count: int
        _memory_table_options: str | None
        _memory_row_deletion_policy: str
        _vector_dimensions: int
        _has_explicit_vector_dimensions: bool
        _vector_distance_type: str
        _vector_index_enabled: bool
        _scann_tree_depth: int
        _scann_num_leaves: int
        _enable_hybrid_search: bool
        _enable_memory_graph: bool
        _memory_graph_name: str

    def _memory_param_types(self, include_owner: bool) -> "dict[str, Any]":
        types: dict[str, Any] = {
            "id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "scope": SPANNER_PARAM_TYPES.STRING,
            "event_id": SPANNER_PARAM_TYPES.STRING,
            "author": SPANNER_PARAM_TYPES.STRING,
            "timestamp": SPANNER_PARAM_TYPES.TIMESTAMP,
            "content_json": _json_param_type(),
            "content_text": SPANNER_PARAM_TYPES.STRING,
            "embedding": SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.FLOAT32),
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

    def _deduplicate_entries(self, entries: "list[StoredMemory]") -> "list[StoredMemory]":
        seen: set[str] = set()
        unique_entries: list[StoredMemory] = []
        for entry in entries:
            event_id = entry["event_id"]
            if event_id in seen:
                continue
            seen.add(event_id)
            unique_entries.append(entry)
        return unique_entries

    def _build_memory_table_ddl(self) -> "list[str]":
        owner_line = ""
        if self._owner_id_column_ddl:
            owner_line = f",\n  {self._owner_id_column_ddl}"

        fts_column_line = ""
        fts_index = ""
        if self._use_fts:
            fts_column_line = ",\n  content_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(content_text)) HIDDEN"
            fts_index = f"CREATE SEARCH INDEX idx_{self._memory_table}_fts ON {self._memory_table}(content_tokens)"

        embedding_type = (
            f"ARRAY<FLOAT32>(vector_length=>{self._vector_dimensions})"
            if self._has_explicit_vector_dimensions
            else "ARRAY<FLOAT32>"
        )

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
  scope STRING(16) NOT NULL,
  event_id STRING(128) NOT NULL,
  author STRING(256){owner_line},
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  content_json JSON NOT NULL,
  content_text STRING(MAX) NOT NULL,
  embedding {embedding_type},
  metadata_json JSON,
  inserted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true){fts_column_line}{shard_column}
) {pk}{options}{self._memory_row_deletion_policy}
"""

        app_scope_user_idx = f"CREATE INDEX idx_{self._memory_table}_app_scope_user_time ON {self._memory_table}(app_name, scope, user_id, timestamp DESC)"
        scope_idx = f"CREATE INDEX idx_{self._memory_table}_scope ON {self._memory_table}(app_name, scope)"
        session_idx = f"CREATE INDEX idx_{self._memory_table}_session ON {self._memory_table}(session_id)"
        event_id_idx = f"CREATE INDEX idx_{self._memory_table}_event_id ON {self._memory_table}(event_id)"

        statements = [table_sql, app_scope_user_idx, scope_idx, session_idx, event_id_idx]
        if fts_index:
            statements.append(fts_index)
        if self._vector_index_enabled:
            statements.append(
                f"CREATE VECTOR INDEX idx_{self._memory_table}_embedding ON {self._memory_table}(embedding) "
                f"STORING (app_name, scope, user_id, timestamp) "
                f"WHERE embedding IS NOT NULL "
                f"OPTIONS (distance_type = '{self._vector_distance_type}', tree_depth = {self._scann_tree_depth}, num_leaves = {self._scann_num_leaves})"
            )
        if self._enable_memory_graph:
            statements.append(
                f"CREATE OR REPLACE PROPERTY GRAPH {self._memory_graph_name} "
                f"NODE TABLES ("
                f"{self._memory_table} AS MemoryNode KEY (id) LABEL Memory "
                f"PROPERTIES (id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_text)"
                f")"
            )
        return statements

    def _drop_memory_table_sql(self) -> "list[str]":
        """Get SQL to drop the memory table and its indexes.

        Returns:
            List of SQL statements to drop the memory table and associated indexes.
        """
        statements: list[str] = []
        if self._enable_memory_graph:
            statements.append(f"DROP PROPERTY GRAPH {self._memory_graph_name}")
        if self._vector_index_enabled:
            statements.append(f"DROP VECTOR INDEX idx_{self._memory_table}_embedding")
        if self._use_fts:
            statements.append(f"DROP SEARCH INDEX idx_{self._memory_table}_fts")
        statements.extend([
            f"DROP INDEX idx_{self._memory_table}_event_id",
            f"DROP INDEX idx_{self._memory_table}_session",
            f"DROP INDEX idx_{self._memory_table}_app_scope_user_time",
            f"DROP INDEX idx_{self._memory_table}_scope",
            f"DROP TABLE {self._memory_table}",
        ])
        return statements

    def _build_insert_memory_entry_statement(
        self, entry: "StoredMemory", owner_id: "object | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", @owner_id" if self._owner_id_column_name else ""
        insert_sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, scope, event_id, author{owner_column},
            timestamp, content_json, content_text, embedding, metadata_json, inserted_at
        ) VALUES (
            @id, @session_id, @app_name, @user_id, @scope, @event_id, @author{owner_param},
            @timestamp, @content_json, @content_text, @embedding, @metadata_json, @inserted_at
        )
        """
        raw_embedding = entry.get("embedding")
        embedding_param = [float(x) for x in raw_embedding] if raw_embedding is not None else None
        params: dict[str, Any] = {
            "id": entry["id"],
            "session_id": entry["session_id"],
            "app_name": entry["app_name"],
            "user_id": entry["user_id"],
            "scope": entry.get("scope", "user"),
            "event_id": entry["event_id"],
            "author": entry["author"],
            "timestamp": entry["timestamp"],
            "content_json": to_json(entry["content_json"]),
            "content_text": entry["content_text"],
            "embedding": embedding_param,
            "metadata_json": to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None,
            "inserted_at": entry["inserted_at"],
        }
        if self._owner_id_column_name:
            params["owner_id"] = str(owner_id) if owner_id is not None else None
        return (insert_sql, params, self._memory_param_types(self._owner_id_column_name is not None))

    def _build_existing_event_ids_query(self, event_ids: "list[str]") -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"SELECT event_id FROM {self._memory_table} WHERE event_id IN UNNEST(@event_ids)"
        return (sql, {"event_ids": event_ids}, {"event_ids": SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.STRING)})

    def _build_search_entries_query(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        if embedding is not None and self._use_fts and bool(query.strip()) and self._enable_hybrid_search:
            return self._build_search_entries_hybrid_rrf_query(query, app_name, user_id, limit, embedding, scope_filter)
        if embedding is not None:
            return self._build_search_entries_vector_query(app_name, user_id, limit, embedding, scope_filter)
        if self._use_fts:
            return self._build_search_entries_fts_query(query, app_name, user_id, limit, scope_filter)
        return self._build_search_entries_simple_query(query, app_name, user_id, limit, scope_filter)

    def _build_search_entries_hybrid_rrf_query(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int,
        embedding: "Sequence[float]",
        scope_filter: Literal["all", "user", "app"] = "all",
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        where_scope, scope_params, scope_types = _build_spanner_scope_where(app_name, user_id, scope_filter)
        candidate_limit = max(limit * 2, 50)
        distance_fn = _SPANNER_DISTANCE_FUNCTIONS.get(self._vector_distance_type, "COSINE_DISTANCE")
        sql = f"""
        WITH vector_matches AS (
            SELECT id, RANK() OVER (ORDER BY {distance_fn}(embedding, @embedding) ASC) AS rank_vec
            FROM {self._memory_table}
            WHERE {where_scope}
              AND embedding IS NOT NULL
            LIMIT @candidate_limit
        ),
        text_matches AS (
            SELECT id, RANK() OVER (ORDER BY SCORE(content_tokens, @query) DESC) AS rank_txt
            FROM {self._memory_table}
            WHERE {where_scope}
              AND SEARCH(content_tokens, @query)
            LIMIT @candidate_limit
        )
        SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author,
               m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at, m.embedding,
               (COALESCE(1.0 / (60 + v.rank_vec), 0.0) + COALESCE(1.0 / (60 + t.rank_txt), 0.0)) AS rrf_score
        FROM {self._memory_table} m
        LEFT JOIN vector_matches v ON m.id = v.id
        LEFT JOIN text_matches t ON m.id = t.id
        WHERE v.id IS NOT NULL OR t.id IS NOT NULL
        ORDER BY rrf_score DESC, m.timestamp DESC
        LIMIT @limit
        """
        params = {
            **scope_params,
            "embedding": [float(x) for x in embedding],
            "query": query,
            "candidate_limit": candidate_limit,
            "limit": limit,
        }
        types = {
            **scope_types,
            "embedding": SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.FLOAT32),
            "query": SPANNER_PARAM_TYPES.STRING,
            "candidate_limit": SPANNER_PARAM_TYPES.INT64,
            "limit": SPANNER_PARAM_TYPES.INT64,
        }
        return (sql, params, types)

    def _build_search_entries_vector_query(
        self,
        app_name: str,
        user_id: str,
        limit: int,
        embedding: "Sequence[float]",
        scope_filter: Literal["all", "user", "app"] = "all",
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        where_scope, scope_params, scope_types = _build_spanner_scope_where(app_name, user_id, scope_filter)
        distance_fn = _SPANNER_DISTANCE_FUNCTIONS.get(self._vector_distance_type, "COSINE_DISTANCE")
        sql = f"""
        SELECT id, session_id, app_name, user_id, scope, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at, embedding
        FROM {self._memory_table}
        WHERE {where_scope}
          AND embedding IS NOT NULL
        ORDER BY {distance_fn}(embedding, @embedding) ASC, timestamp DESC
        LIMIT @limit
        """
        params = {**scope_params, "embedding": [float(x) for x in embedding], "limit": limit}
        types = {
            **scope_types,
            "embedding": SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.FLOAT32),
            "limit": SPANNER_PARAM_TYPES.INT64,
        }
        return (sql, params, types)

    def _build_search_entries_fts_query(
        self, query: str, app_name: str, user_id: str, limit: int, scope_filter: Literal["all", "user", "app"] = "all"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        where_scope, scope_params, scope_types = _build_spanner_scope_where(app_name, user_id, scope_filter)
        sql = f"""
        SELECT id, session_id, app_name, user_id, scope, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at, embedding
        FROM {self._memory_table}
        WHERE {where_scope}
          AND SEARCH(content_tokens, @query)
        ORDER BY SCORE(content_tokens, @query) DESC, timestamp DESC
        LIMIT @limit
        """
        params = {**scope_params, "query": query, "limit": limit}
        types = {**scope_types, "query": SPANNER_PARAM_TYPES.STRING, "limit": SPANNER_PARAM_TYPES.INT64}
        return (sql, params, types)

    def _build_search_entries_simple_query(
        self, query: str, app_name: str, user_id: str, limit: int, scope_filter: Literal["all", "user", "app"] = "all"
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        where_scope, scope_params, scope_types = _build_spanner_scope_where(app_name, user_id, scope_filter)
        sql = f"""
        SELECT id, session_id, app_name, user_id, scope, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at, embedding
        FROM {self._memory_table}
        WHERE {where_scope}
          AND LOWER(content_text) LIKE @pattern
        ORDER BY timestamp DESC
        LIMIT @limit
        """
        pattern = f"%{query.lower()}%"
        params = {**scope_params, "pattern": pattern, "limit": limit}
        types = {**scope_types, "pattern": SPANNER_PARAM_TYPES.STRING, "limit": SPANNER_PARAM_TYPES.INT64}
        return (sql, params, types)

    def _build_delete_entries_by_session_statement(
        self, session_id: str
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = @session_id"
        params = {"session_id": session_id}
        types = {"session_id": SPANNER_PARAM_TYPES.STRING}
        return (sql, params, types)

    def _build_delete_entries_older_than_statement(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        clauses = ["inserted_at < @cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}
        types: dict[str, Any] = {"cutoff": SPANNER_PARAM_TYPES.TIMESTAMP}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
            types["app_name"] = SPANNER_PARAM_TYPES.STRING
        if scope is not None:
            clauses.append("scope = @scope")
            params["scope"] = scope
            types["scope"] = SPANNER_PARAM_TYPES.STRING
        where_sql = " AND ".join(clauses)
        sql = f"DELETE FROM {self._memory_table} WHERE {where_sql}"
        return (sql, params, types)

    def _rows_to_records(self, rows: "list[Any]") -> "list[StoredMemory]":
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "scope": row[4],
                "event_id": row[5],
                "author": row[6],
                "timestamp": row[7],
                "content_json": self._decode_json(row[8]),
                "content_text": row[9],
                "metadata_json": self._decode_json(row[10]),
                "inserted_at": row[11],
                "embedding": (
                    [float(x) for x in row[_EMBEDDING_ROW_INDEX]]
                    if len(row) > _EMBEDDING_ROW_INDEX and row[_EMBEDDING_ROW_INDEX] is not None
                    else None
                ),
            }
            for row in rows
        ]


class SpannerSyncADKMemoryStore(_SpannerADKMemoryStoreMixin, BaseSyncADKMemoryStore[SpannerSyncConfig]):
    """Spanner ADK memory store backed by synchronous Spanner client."""

    __slots__ = (
        "_enable_hybrid_search",
        "_enable_memory_graph",
        "_has_explicit_vector_dimensions",
        "_memory_graph_name",
        "_memory_row_deletion_policy",
        "_memory_table_options",
        "_scann_tree_depth",
        "_shard_count",
        "_vector_distance_type",
        "_vector_index_enabled",
    )

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        (
            self._shard_count,
            self._memory_table_options,
            self._memory_row_deletion_policy,
            self._has_explicit_vector_dimensions,
            self._vector_distance_type,
            self._vector_index_enabled,
            self._scann_tree_depth,
            self._scann_num_leaves,
            self._enable_hybrid_search,
            self._enable_memory_graph,
            self._memory_graph_name,
        ) = _extract_spanner_memory_options(config, self._memory_table)

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return

        self._create_tables()

    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return self._insert_memory_entries(entries, owner_id)

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        """Search memory entries by text query or vector embedding."""
        return self._search_entries(query, app_name, user_id, limit, scope_filter, embedding=embedding)

    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return self._delete_entries_by_session(session_id)

    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than specified days."""
        return self._delete_entries_older_than(days, app_name, scope)

    def _database(self) -> "SpannerDatabase":
        return self._config.get_database()

    def _reset_drop_memory_table_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(self._reset_drop_memory_statements(), self._existing_tables())

    def _existing_tables(self) -> "set[str]":
        return list_existing_table_names_sync(self._database())

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            reader = cast("_SpannerReadProtocol", snapshot)
            return list(reader.execute_sql(sql, params=params, param_types=types))

    def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        return int(cast("Any", self._database()).run_in_transaction(_SpannerSyncUpdateJob(sql, params, types)))

    def _create_tables(self) -> None:
        if not self._enabled or self._memory_table in self._existing_tables():
            return
        execute_ddl_sync(self._database(), self._memory_table_ddl(), timeout=_DDL_TIMEOUT_SECONDS)

    def _memory_table_ddl(self) -> "list[str]":
        return self._build_memory_table_ddl()

    def _insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        unique_entries = self._deduplicate_entries(entries)
        if not unique_entries:
            return 0

        event_ids = [entry["event_id"] for entry in unique_entries]
        check_sql, check_params, check_types = self._build_existing_event_ids_query(event_ids)
        candidates = [
            (entry["event_id"], self._build_insert_memory_entry_statement(entry, owner_id)) for entry in unique_entries
        ]
        job = _SpannerSyncMemoryInsertJob(check_sql, check_params, check_types, candidates)
        return int(cast("Any", self._database()).run_in_transaction(job))

    def _search_entries(
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

        if not query.strip() and embedding is None:
            return []

        effective_limit = limit if limit is not None else self._max_results
        sql, params, types = self._build_search_entries_query(
            query, app_name, user_id, effective_limit, scope_filter, embedding=embedding
        )
        rows = self._run_read(sql, params, types)
        return self._rows_to_records(rows)

    def _delete_entries_by_session(self, session_id: str) -> int:
        sql, params, types = self._build_delete_entries_by_session_statement(session_id)
        return self._execute_update(sql, params, types)

    def _delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        sql, params, types = self._build_delete_entries_older_than_statement(days, app_name, scope)
        return self._execute_update(sql, params, types)


class SpannerAsyncADKMemoryStore(_SpannerADKMemoryStoreMixin, BaseAsyncADKMemoryStore[SpannerAsyncConfig]):
    """Spanner ADK memory store backed by asynchronous Spanner client."""

    __slots__ = (
        "_enable_hybrid_search",
        "_enable_memory_graph",
        "_has_explicit_vector_dimensions",
        "_memory_graph_name",
        "_memory_row_deletion_policy",
        "_memory_table_options",
        "_scann_tree_depth",
        "_shard_count",
        "_vector_distance_type",
        "_vector_index_enabled",
    )

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerAsyncConfig) -> None:
        super().__init__(config)
        (
            self._shard_count,
            self._memory_table_options,
            self._memory_row_deletion_policy,
            self._has_explicit_vector_dimensions,
            self._vector_distance_type,
            self._vector_index_enabled,
            self._scann_tree_depth,
            self._scann_num_leaves,
            self._enable_hybrid_search,
            self._enable_memory_graph,
            self._memory_graph_name,
        ) = _extract_spanner_memory_options(config, self._memory_table)

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return

        await self._create_tables()

    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        return await self._insert_memory_entries(entries, owner_id)

    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        """Search memory entries by text query or vector embedding."""
        return await self._search_entries(query, app_name, user_id, limit, scope_filter, embedding=embedding)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await self._delete_entries_by_session(session_id)

    async def delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
        """Delete memory entries older than specified days."""
        return await self._delete_entries_older_than(days, app_name, scope)

    async def _database(self) -> "SpannerAsyncDatabase":
        return await self._config.get_database()

    async def _reset_drop_memory_table_sql(self) -> "list[str]":
        return _filter_existing_spanner_drops(self._reset_drop_memory_statements(), await self._existing_tables())

    async def _existing_tables(self) -> "set[str]":
        return await list_existing_table_names_async(await self._database())

    async def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        return await _run_read_async(await self._database(), sql, params, types)

    async def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        database = await self._database()
        return int(await database.run_in_transaction(_SpannerAsyncUpdateJob(sql, params, types)))

    async def _create_tables(self) -> None:
        if not self._enabled or self._memory_table in await self._existing_tables():
            return
        await execute_ddl_async(await self._database(), await self._memory_table_ddl(), timeout=_DDL_TIMEOUT_SECONDS)

    async def _memory_table_ddl(self) -> "list[str]":
        return self._build_memory_table_ddl()

    async def _insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        unique_entries = self._deduplicate_entries(entries)
        if not unique_entries:
            return 0

        event_ids = [entry["event_id"] for entry in unique_entries]
        check_sql, check_params, check_types = self._build_existing_event_ids_query(event_ids)
        candidates = [
            (entry["event_id"], self._build_insert_memory_entry_statement(entry, owner_id)) for entry in unique_entries
        ]
        database = await self._database()
        job = _SpannerAsyncMemoryInsertJob(check_sql, check_params, check_types, candidates)
        return int(await database.run_in_transaction(job))

    async def _search_entries(
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

        if not query.strip() and embedding is None:
            return []

        effective_limit = limit if limit is not None else self._max_results
        sql, params, types = self._build_search_entries_query(
            query, app_name, user_id, effective_limit, scope_filter, embedding=embedding
        )
        rows = await self._run_read(sql, params, types)
        return self._rows_to_records(rows)

    async def _delete_entries_by_session(self, session_id: str) -> int:
        sql, params, types = self._build_delete_entries_by_session_statement(session_id)
        return await self._execute_update(sql, params, types)

    async def _delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
        sql, params, types = self._build_delete_entries_older_than_statement(days, app_name, scope)
        return await self._execute_update(sql, params, types)


async def _run_read_async(
    database: "SpannerAsyncDatabase",
    sql: str,
    params: "dict[str, Any] | None" = None,
    types: "dict[str, Any] | None" = None,
) -> "list[Any]":
    async with cast("Any", database).snapshot() as snapshot:
        result_set = await snapshot.execute_sql(sql, params=params, param_types=types)
        return [row async for row in result_set]


def _json_param_type() -> Any:
    try:
        return SPANNER_PARAM_TYPES.JSON
    except AttributeError:
        return SPANNER_PARAM_TYPES.STRING


def _extract_spanner_adk_options(config: Any) -> "tuple[int, str | None, str | None, str | None, str, str]":
    """Return shard count, table options, and row deletion policies for the ADK session tables."""
    adk_config = _adk_config(config)
    return (
        _spanner_shard_count(adk_config),
        adk_config.get("session_table_options"),
        adk_config.get("events_table_options"),
        adk_config.get("expires_index_options"),
        _spanner_row_deletion_policy(adk_config, "session_ttl_seconds", "create_time"),
        _spanner_row_deletion_policy(adk_config, "event_ttl_seconds", "timestamp"),
    )


def _extract_spanner_memory_options(
    config: Any, memory_table: str
) -> "tuple[int, str | None, str, bool, str, bool, int, int, bool, bool, str]":
    """Return Spanner-specific DDL, vector index, hybrid search, and property graph options for ADK memory."""
    adk_config = _adk_config(config)
    has_explicit_vector_dims = adk_config.get("vector_dimensions") is not None
    raw_distance = str(adk_config.get("vector_distance_type") or "COSINE").upper()
    if raw_distance not in _SPANNER_DISTANCE_FUNCTIONS:
        msg = f"Unsupported Spanner vector_distance_type: {raw_distance!r}"
        raise ValueError(msg)
    vector_index_enabled = bool(adk_config.get("vector_index_enabled", False))
    raw_tree_depth = adk_config.get("scann_tree_depth", 2)
    scann_tree_depth = int(raw_tree_depth) if isinstance(raw_tree_depth, int) else 2
    if scann_tree_depth not in {2, 3}:
        msg = f"scann_tree_depth must be 2 or 3, got {scann_tree_depth!r}"
        raise ValueError(msg)
    raw_num_leaves = adk_config.get("scann_num_leaves")
    scann_num_leaves = int(raw_num_leaves) if isinstance(raw_num_leaves, int) and raw_num_leaves > 0 else 1000
    enable_hybrid_search = bool(adk_config.get("enable_hybrid_search", True))
    enable_memory_graph = bool(adk_config.get("enable_memory_graph", False))
    raw_graph_name = adk_config.get("memory_graph_name")
    memory_graph_name = str(raw_graph_name) if raw_graph_name else f"{memory_table}_graph"
    ensure_table_name(memory_graph_name)
    return (
        _spanner_shard_count(adk_config),
        adk_config.get("memory_table_options"),
        _spanner_row_deletion_policy(adk_config, "memory_ttl_seconds", "inserted_at"),
        has_explicit_vector_dims,
        raw_distance,
        vector_index_enabled,
        scann_tree_depth,
        scann_num_leaves,
        enable_hybrid_search,
        enable_memory_graph,
        memory_graph_name,
    )


def _spanner_shard_count(adk_config: "SpannerADKConfig") -> int:
    shard_count = adk_config.get("shard_count")
    return int(shard_count) if shard_count else 0


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
    try:
        parsed = sqlglot.parse_one(statement, read="spanner")
    except Exception:
        return None

    if not isinstance(parsed, exp.Drop):
        return None

    target = parsed.this if isinstance(parsed.this, exp.Table) else parsed.find(exp.Table)
    if target is None or not target.name:
        return None

    kind = str(parsed.args.get("kind") or "").upper()
    if kind == "TABLE":
        return target.name if target.name in existing_tables else None
    if kind in {"INDEX", "SEARCH INDEX", "VECTOR INDEX"}:
        for table_name in existing_tables:
            if target.name.startswith(f"idx_{table_name}_"):
                return table_name
    if kind == "PROPERTY GRAPH" and target.name.endswith("_graph"):
        base_table = target.name[:-6]
        if base_table in existing_tables:
            return base_table
    return None


class _SpannerSyncWriteJob:
    """Callable transaction work item for Spanner sync ADK write batches."""

    __slots__ = ("_statements",)

    def __init__(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._statements = statements

    def __call__(self, transaction: "SpannerTransaction") -> None:
        if len(self._statements) > 1:
            status, _row_counts = transaction.batch_update(self._statements)  # type: ignore[no-untyped-call]
            if status.code != 0:
                msg = f"Spanner batch update failed (code {status.code}): {status.message}"
                raise OperationalError(msg)
            return
        for sql, params, types in self._statements:
            transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]


class _SpannerAsyncWriteJob:
    """Callable async transaction work item for Spanner async ADK write batches."""

    __slots__ = ("_statements",)

    def __init__(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        self._statements = statements

    async def __call__(self, transaction: "SpannerAsyncTransaction") -> None:
        if len(self._statements) > 1:
            status, _row_counts = await transaction.batch_update(self._statements)
            if status.code != 0:
                msg = f"Spanner batch update failed (code {status.code}): {status.message}"
                raise OperationalError(msg)
            return
        for sql, params, types in self._statements:
            await transaction.execute_update(sql, params=params, param_types=types)


class _SpannerSyncMemoryInsertJob:
    """Callable transaction work item for Spanner sync ADK memory batch deduplication and insert."""

    __slots__ = ("_candidates", "_check_params", "_check_sql", "_check_types")

    def __init__(
        self,
        check_sql: str,
        check_params: "dict[str, Any]",
        check_types: "dict[str, Any]",
        candidates: "list[tuple[str, tuple[str, dict[str, Any], dict[str, Any]]]]",
    ) -> None:
        self._check_sql = check_sql
        self._check_params = check_params
        self._check_types = check_types
        self._candidates = candidates

    def __call__(self, transaction: "SpannerTransaction") -> int:
        rows = cast("_SpannerReadProtocol", transaction).execute_sql(
            self._check_sql, params=self._check_params, param_types=self._check_types
        )
        existing = {str(row[0]) for row in rows}
        statements = [stmt for event_id, stmt in self._candidates if event_id not in existing]
        if not statements:
            return 0
        _SpannerSyncWriteJob(statements)(transaction)
        return len(statements)


class _SpannerAsyncMemoryInsertJob:
    """Callable async transaction work item for Spanner async ADK memory batch deduplication and insert."""

    __slots__ = ("_candidates", "_check_params", "_check_sql", "_check_types")

    def __init__(
        self,
        check_sql: str,
        check_params: "dict[str, Any]",
        check_types: "dict[str, Any]",
        candidates: "list[tuple[str, tuple[str, dict[str, Any], dict[str, Any]]]]",
    ) -> None:
        self._check_sql = check_sql
        self._check_params = check_params
        self._check_types = check_types
        self._candidates = candidates

    async def __call__(self, transaction: "SpannerAsyncTransaction") -> int:
        result_set = await cast("Any", transaction).execute_sql(
            self._check_sql, params=self._check_params, param_types=self._check_types
        )
        existing = {str(row[0]) async for row in result_set}
        statements = [stmt for event_id, stmt in self._candidates if event_id not in existing]
        if not statements:
            return 0
        await _SpannerAsyncWriteJob(statements)(transaction)
        return len(statements)


class _SpannerSyncUpdateJob:
    """Callable transaction work item for Spanner sync ADK single DML updates."""

    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    def __call__(self, transaction: "SpannerTransaction") -> int:
        return int(transaction.execute_update(self._sql, params=self._params, param_types=self._types))  # type: ignore[no-untyped-call]


class _SpannerAsyncUpdateJob:
    """Callable async transaction work item for Spanner async ADK single DML updates."""

    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    async def __call__(self, transaction: "SpannerAsyncTransaction") -> int:
        return int(await transaction.execute_update(self._sql, params=self._params, param_types=self._types))


class _SpannerReadProtocol(Protocol):
    def execute_sql(
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> Iterable[Any]: ...


def _build_spanner_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    if scope_filter == "all":
        where = "app_name = @app_name AND ((scope = 'user' AND user_id = @user_id) OR scope = 'app')"
        params = {"app_name": app_name, "user_id": user_id}
        types = {"app_name": SPANNER_PARAM_TYPES.STRING, "user_id": SPANNER_PARAM_TYPES.STRING}
        return where, params, types
    if scope_filter == "user":
        where = "app_name = @app_name AND scope = 'user' AND user_id = @user_id"
        params = {"app_name": app_name, "user_id": user_id}
        types = {"app_name": SPANNER_PARAM_TYPES.STRING, "user_id": SPANNER_PARAM_TYPES.STRING}
        return where, params, types
    where = "app_name = @app_name AND scope = 'app'"
    params = {"app_name": app_name}
    types = {"app_name": SPANNER_PARAM_TYPES.STRING}
    return where, params, types
