"""Spanner ADK memory store."""

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast

from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.protocols import SpannerParamTypesProtocol
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from google.cloud.spanner_v1.transaction import Transaction

    from sqlspec.config import ADKConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

SPANNER_PARAM_TYPES: SpannerParamTypesProtocol = cast("SpannerParamTypesProtocol", param_types)

__all__ = ("SpannerSyncADKMemoryStore",)


def _json_param_type() -> Any:
    try:
        return SPANNER_PARAM_TYPES.JSON
    except AttributeError:
        return SPANNER_PARAM_TYPES.STRING


class _SpannerReadProtocol(Protocol):
    def execute_sql(
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> Iterable[Any]: ...


class SpannerSyncADKMemoryStore(BaseSyncADKMemoryStore[SpannerSyncConfig]):
    """Spanner ADK memory store backed by synchronous Spanner client."""

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        adk_config = cast("ADKConfig", config.extension_config.get("adk", {}))
        shard_count = adk_config.get("shard_count")
        self._shard_count = int(shard_count) if isinstance(shard_count, int) else 0

    def _database(self) -> "Database":
        return self._config.get_database()

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            reader = cast("_SpannerReadProtocol", snapshot)
            result_set = reader.execute_sql(sql, params=params, param_types=types)
            return list(result_set)

    def _run_write(self, statements: "list[tuple[str, dict[str, Any], dict[str, Any]]]") -> None:
        def _txn_job(transaction: "Transaction") -> None:
            for sql, params, types in statements:
                transaction.execute_update(sql, params=params, param_types=types)  # type: ignore[no-untyped-call]

        self._database().run_in_transaction(_txn_job)  # type: ignore[no-untyped-call]

    def _execute_update(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> int:
        def _txn_job(transaction: "Transaction") -> int:
            return int(transaction.execute_update(sql, params=params, param_types=types))  # type: ignore[no-untyped-call]

        return int(self._database().run_in_transaction(_txn_job))  # type: ignore[no-untyped-call]

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

    def create_tables(self) -> None:
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
) {pk}
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

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
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

    def search_entries(
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

    def delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = @session_id"
        params = {"session_id": session_id}
        types = {"session_id": SPANNER_PARAM_TYPES.STRING}
        return self._execute_update(sql, params, types)

    def delete_entries_older_than(self, days: int) -> int:
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
