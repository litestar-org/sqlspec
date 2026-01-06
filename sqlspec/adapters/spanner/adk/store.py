"""Spanner ADK store."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.spanner.type_converter import bytes_to_spanner, spanner_to_bytes
from sqlspec.extensions.adk import BaseSyncADKStore, EventRecord, SessionRecord
from sqlspec.protocols import SpannerParamTypesProtocol
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from google.cloud.spanner_v1.transaction import Transaction

SPANNER_PARAM_TYPES: SpannerParamTypesProtocol = cast("SpannerParamTypesProtocol", param_types)

__all__ = ("SpannerSyncADKStore",)


def _json_param_type() -> Any:
    try:
        return SPANNER_PARAM_TYPES.JSON
    except AttributeError:
        return SPANNER_PARAM_TYPES.STRING


class SpannerSyncADKStore(BaseSyncADKStore[SpannerSyncConfig]):
    """Spanner ADK store backed by synchronous Spanner client."""

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        adk_config = cast("dict[str, Any]", config.extension_config.get("adk", {}))
        self._shard_count: int = int(adk_config.get("shard_count", 0)) if adk_config.get("shard_count") else 0
        self._session_table_options: str | None = adk_config.get("session_table_options")
        self._events_table_options: str | None = adk_config.get("events_table_options")
        self._expires_index_options: str | None = adk_config.get("expires_index_options")

    def _database(self) -> "Database":
        return self._config.get_database()

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            result_set = cast("Any", snapshot).execute_sql(sql, params=params, param_types=types)
            return list(result_set)

    def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        def _txn_job(transaction: "Transaction") -> None:
            for sql, params, types in statements:
                transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]

        self._database().run_in_transaction(_txn_job)  # type: ignore[no-untyped-call]

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

    def _event_param_types(self, has_branch: bool) -> "dict[str, Any]":
        json_type = _json_param_type()
        types: dict[str, Any] = {
            "id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "author": SPANNER_PARAM_TYPES.STRING,
            "actions": SPANNER_PARAM_TYPES.BYTES,
            "long_running_tool_ids_json": json_type,
            "invocation_id": SPANNER_PARAM_TYPES.STRING,
            "timestamp": SPANNER_PARAM_TYPES.TIMESTAMP,
            "content": json_type,
            "grounding_metadata": json_type,
            "custom_metadata": json_type,
            "partial": SPANNER_PARAM_TYPES.BOOL,
            "turn_complete": SPANNER_PARAM_TYPES.BOOL,
            "interrupted": SPANNER_PARAM_TYPES.BOOL,
            "error_code": SPANNER_PARAM_TYPES.STRING,
            "error_message": SPANNER_PARAM_TYPES.STRING,
        }
        if has_branch:
            types["branch"] = SPANNER_PARAM_TYPES.STRING
        return types

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

    def create_session(
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

    def get_session(self, session_id: str) -> "SessionRecord | None":
        sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time{", " + self._owner_id_column_name if self._owner_id_column_name else ""}
            FROM {self._session_table}
            WHERE id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        sql = f"{sql} LIMIT 1"
        params = {"id": session_id}
        rows = self._run_read(sql, params, {"id": SPANNER_PARAM_TYPES.STRING})
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

    def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        params = {"id": session_id, "state": to_json(state)}
        json_type = _json_param_type()
        sql = f"""
            UPDATE {self._session_table}
            SET state = @state, update_time = PENDING_COMMIT_TIMESTAMP()
            WHERE id = @id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@id), {self._shard_count})"
        self._run_write([(sql, params, {"id": SPANNER_PARAM_TYPES.STRING, "state": json_type})])

    def list_sessions(self, app_name: str, user_id: "str | None" = None) -> "list[SessionRecord]":
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

    def delete_session(self, session_id: str) -> None:
        shard_clause = (
            f" AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})" if self._shard_count > 1 else ""
        )
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = @session_id{shard_clause}"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE id = @session_id{shard_clause}"
        params = {"session_id": session_id}
        types = {"session_id": SPANNER_PARAM_TYPES.STRING}
        self._run_write([(delete_events_sql, params, types), (delete_session_sql, params, types)])

    def create_event(
        self,
        event_id: str,
        session_id: str,
        app_name: str,
        user_id: str,
        author: "str | None" = None,
        actions: "bytes | None" = None,
        content: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> EventRecord:
        branch = kwargs.get("branch")
        long_running_serialized = (
            to_json(kwargs.get("long_running_tool_ids_json"))
            if kwargs.get("long_running_tool_ids_json") is not None
            else None
        )
        content_serialized = to_json(content) if content is not None else None
        grounding_serialized = (
            to_json(kwargs.get("grounding_metadata")) if kwargs.get("grounding_metadata") is not None else None
        )
        custom_serialized = (
            to_json(kwargs.get("custom_metadata")) if kwargs.get("custom_metadata") is not None else None
        )
        params: dict[str, Any] = {
            "id": event_id,
            "session_id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "author": author,
            "actions": bytes_to_spanner(actions),
            "long_running_tool_ids_json": long_running_serialized,
            "timestamp": datetime.now(timezone.utc),
            "content": content_serialized,
            "grounding_metadata": grounding_serialized,
            "custom_metadata": custom_serialized,
            "invocation_id": kwargs.get("invocation_id"),
            "partial": kwargs.get("partial"),
            "turn_complete": kwargs.get("turn_complete"),
            "interrupted": kwargs.get("interrupted"),
            "error_code": kwargs.get("error_code"),
            "error_message": kwargs.get("error_message"),
        }
        branch = kwargs.get("branch")
        columns = [
            "id",
            "session_id",
            "app_name",
            "user_id",
            "author",
            "actions",
            "long_running_tool_ids_json",
            "timestamp",
            "content",
            "grounding_metadata",
            "custom_metadata",
            "invocation_id",
            "partial",
            "turn_complete",
            "interrupted",
            "error_code",
            "error_message",
        ]
        values = [
            "@id",
            "@session_id",
            "@app_name",
            "@user_id",
            "@author",
            "@actions",
            "@long_running_tool_ids_json",
            "PENDING_COMMIT_TIMESTAMP()",
            "@content",
            "@grounding_metadata",
            "@custom_metadata",
            "@invocation_id",
            "@partial",
            "@turn_complete",
            "@interrupted",
            "@error_code",
            "@error_message",
        ]
        has_branch = branch is not None
        if has_branch:
            params["branch"] = branch
            columns.append("branch")
            values.append("@branch")

        sql = f"""
            INSERT INTO {self._events_table} ({", ".join(columns)})
            VALUES ({", ".join(values)})
        """
        self._run_write([(sql, params, self._event_param_types(has_branch))])

        record: EventRecord = {
            "id": event_id,
            "session_id": session_id,
            "app_name": app_name,
            "user_id": user_id,
            "author": author or "",
            "actions": actions or b"",
            "long_running_tool_ids_json": long_running_serialized,
            "branch": branch,
            "timestamp": params["timestamp"],
            "content": from_json(content_serialized) if content_serialized else None,
            "grounding_metadata": from_json(grounding_serialized) if grounding_serialized else None,
            "custom_metadata": from_json(custom_serialized) if custom_serialized else None,
            "invocation_id": kwargs.get("invocation_id", ""),
            "partial": kwargs.get("partial"),
            "turn_complete": kwargs.get("turn_complete"),
            "interrupted": kwargs.get("interrupted"),
            "error_code": kwargs.get("error_code"),
            "error_message": kwargs.get("error_message"),
        }
        return record

    def list_events(self, session_id: str) -> "list[EventRecord]":
        sql = f"""
            SELECT id, session_id, app_name, user_id, author, actions, long_running_tool_ids_json, branch,
                   timestamp, content, grounding_metadata, custom_metadata, invocation_id, partial,
                   turn_complete, interrupted, error_code, error_message
            FROM {self._events_table}
            WHERE session_id = @session_id
        """
        if self._shard_count > 1:
            sql = f"{sql} AND shard_id = MOD(FARM_FINGERPRINT(@session_id), {self._shard_count})"
        sql = f"{sql} ORDER BY timestamp ASC"
        params = {"session_id": session_id}
        types = {"session_id": SPANNER_PARAM_TYPES.STRING}
        rows = self._run_read(sql, params, types)
        return [
            {
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "invocation_id": row[12] or "",
                "author": row[4] or "",
                "actions": spanner_to_bytes(row[5]) or b"",
                "long_running_tool_ids_json": row[6],
                "branch": row[7],
                "timestamp": row[8],
                "content": self._decode_json(row[9]),
                "grounding_metadata": self._decode_json(row[10]),
                "custom_metadata": self._decode_json(row[11]),
                "partial": row[13],
                "turn_complete": row[14],
                "interrupted": row[15],
                "error_code": row[16],
                "error_message": row[17],
            }
            for row in rows
        ]

    def create_tables(self) -> None:
        database = self._database()
        existing_tables = {t.table_id for t in database.list_tables()}  # type: ignore[no-untyped-call]

        ddl_statements: list[str] = []
        if self._session_table not in existing_tables:
            ddl_statements.append(self._get_create_sessions_table_sql())
        if self._events_table not in existing_tables:
            ddl_statements.append(self._get_create_events_table_sql())

        if ddl_statements:
            database.update_ddl(ddl_statements).result(300)  # type: ignore[no-untyped-call]

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
) {pk}{options}
"""

    def _get_create_events_table_sql(self) -> str:
        shard_column = ""
        pk = "PRIMARY KEY (session_id, timestamp, id)"
        if self._shard_count > 1:
            shard_column = f",\n  shard_id INT64 AS (MOD(FARM_FINGERPRINT(session_id), {self._shard_count})) STORED"
            pk = "PRIMARY KEY (shard_id, session_id, timestamp, id)"
        options = ""
        if self._events_table_options:
            options = f"\nOPTIONS ({self._events_table_options})"
        return f"""
CREATE TABLE {self._events_table} (
  id STRING(128) NOT NULL,
  session_id STRING(128) NOT NULL,
  app_name STRING(128) NOT NULL,
  user_id STRING(128) NOT NULL,
  invocation_id STRING(128),
  author STRING(64),
  actions BYTES(MAX),
  long_running_tool_ids_json JSON,
  branch STRING(64),
  timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  content JSON,
  grounding_metadata JSON,
  custom_metadata JSON,
  partial BOOL,
  turn_complete BOOL,
  interrupted BOOL,
  error_code STRING(64),
  error_message STRING(255){shard_column}
) {pk}{options}
"""

    def _get_drop_tables_sql(self) -> "list[str]":
        return [f"DROP TABLE {self._events_table}", f"DROP TABLE {self._session_table}"]
