"""BigQuery ADK memory store for Google Agent Development Kit memory storage."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.config import BigQueryConfig
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.bigquery.adk.memory_store")

__all__ = ("BigQueryADKMemoryStore",)


class BigQueryADKMemoryStore(BaseAsyncADKMemoryStore["BigQueryConfig"]):
    """BigQuery ADK memory store using synchronous BigQuery client with async wrapper."""

    __slots__ = ("_dataset_id",)

    def __init__(self, config: "BigQueryConfig") -> None:
        """Initialize BigQuery ADK memory store."""
        super().__init__(config)
        self._dataset_id = config.connection_config.get("dataset_id")

    def _get_full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name for BigQuery."""
        if self._dataset_id:
            return f"`{self._dataset_id}.{table_name}`"
        return f"`{table_name}`"

    async def _get_create_memory_table_sql(self) -> str:
        """Get BigQuery CREATE TABLE SQL for memory entries."""
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        table_name = self._get_full_table_name(self._memory_table)
        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        CREATE SEARCH INDEX idx_{self._memory_table}_fts
        ON {table_name}(content_text)
        """

        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING NOT NULL,
            session_id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            event_id STRING NOT NULL,
            author STRING{owner_id_line},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text STRING NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY app_name, user_id;
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get BigQuery DROP TABLE SQL statements."""
        table_name = self._get_full_table_name(self._memory_table)
        return [f"DROP TABLE IF EXISTS {table_name}"]

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_memory_table_sql)())
        logger.debug("Created BigQuery ADK memory table: %s", self._memory_table)

    async def create_tables(self) -> None:
        """Create the memory table if it doesn't exist."""
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return
        await async_(self._create_tables)()

    def _insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Synchronous implementation of insert_memory_entries."""
        table_name = self._get_full_table_name(self._memory_table)
        inserted_count = 0

        with self._config.provide_connection() as conn:
            for entry in entries:
                content_json = to_json(entry["content_json"])
                metadata_json = to_json(entry["metadata_json"]) if entry["metadata_json"] is not None else None
                metadata_expr = "JSON(@metadata_json)" if metadata_json is not None else "NULL"

                owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
                owner_value = ", @owner_id" if self._owner_id_column_name else ""

                sql = f"""
                MERGE {table_name} T
                USING (SELECT @event_id AS event_id) S
                ON T.event_id = S.event_id
                WHEN NOT MATCHED THEN
                  INSERT (id, session_id, app_name, user_id, event_id, author{owner_column},
                          timestamp, content_json, content_text, metadata_json, inserted_at)
                  VALUES (@id, @session_id, @app_name, @user_id, @event_id, @author{owner_value},
                          @timestamp, JSON(@content_json), @content_text, {metadata_expr}, @inserted_at)
                """

                params = [
                    ScalarQueryParameter("id", "STRING", entry["id"]),
                    ScalarQueryParameter("session_id", "STRING", entry["session_id"]),
                    ScalarQueryParameter("app_name", "STRING", entry["app_name"]),
                    ScalarQueryParameter("user_id", "STRING", entry["user_id"]),
                    ScalarQueryParameter("event_id", "STRING", entry["event_id"]),
                    ScalarQueryParameter("author", "STRING", entry["author"]),
                    ScalarQueryParameter("timestamp", "TIMESTAMP", entry["timestamp"]),
                    ScalarQueryParameter("content_json", "STRING", content_json),
                    ScalarQueryParameter("content_text", "STRING", entry["content_text"]),
                    ScalarQueryParameter("inserted_at", "TIMESTAMP", entry["inserted_at"]),
                ]

                if self._owner_id_column_name:
                    params.append(ScalarQueryParameter("owner_id", "STRING", str(owner_id) if owner_id else None))
                if metadata_json is not None:
                    params.append(ScalarQueryParameter("metadata_json", "STRING", metadata_json))

                job_config = QueryJobConfig(query_parameters=params)
                job = conn.query(sql, job_config=job_config)
                job.result()
                if job.num_dml_affected_rows:
                    inserted_count += int(job.num_dml_affected_rows)

        return inserted_count

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        return await async_(self._insert_memory_entries)(entries, owner_id)

    def _search_entries(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        """Synchronous implementation of search_entries."""
        table_name = self._get_full_table_name(self._memory_table)
        base_params = [
            ScalarQueryParameter("app_name", "STRING", app_name),
            ScalarQueryParameter("user_id", "STRING", user_id),
            ScalarQueryParameter("limit", "INT64", limit),
        ]

        if self._use_fts:
            sql = f"""
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {table_name}
            WHERE app_name = @app_name
              AND user_id = @user_id
              AND SEARCH(content_text, @query)
            ORDER BY timestamp DESC
            LIMIT @limit
            """
            params = [*base_params, ScalarQueryParameter("query", "STRING", query)]
        else:
            sql = f"""
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {table_name}
            WHERE app_name = @app_name
              AND user_id = @user_id
              AND LOWER(content_text) LIKE LOWER(@pattern)
            ORDER BY timestamp DESC
            LIMIT @limit
            """
            pattern = f"%{query}%"
            params = [*base_params, ScalarQueryParameter("pattern", "STRING", pattern)]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            rows = conn.query(sql, job_config=job_config).result()
            return _rows_to_records(rows)

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            return await async_(self._search_entries)(query, app_name, user_id, effective_limit)
        except NotFound:
            return []

    def _delete_entries_by_session(self, session_id: str) -> int:
        table_name = self._get_full_table_name(self._memory_table)
        sql = f"DELETE FROM {table_name} WHERE session_id = @session_id"
        params = [ScalarQueryParameter("session_id", "STRING", session_id)]
        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            job = conn.query(sql, job_config=job_config)
            job.result()
            return int(job.num_dml_affected_rows or 0)

    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        return await async_(self._delete_entries_by_session)(session_id)

    def _delete_entries_older_than(self, days: int) -> int:
        table_name = self._get_full_table_name(self._memory_table)
        sql = f"""
        DELETE FROM {table_name}
        WHERE inserted_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        """
        with self._config.provide_connection() as conn:
            job = conn.query(sql)
            job.result()
            return int(job.num_dml_affected_rows or 0)

    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days."""
        return await async_(self._delete_entries_older_than)(days)


def _decode_json_field(value: Any) -> "dict[str, Any] | None":
    if value is None:
        return None
    if isinstance(value, str):
        return cast("dict[str, Any]", from_json(value))
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _rows_to_records(rows: Any) -> "list[MemoryRecord]":
    return [
        {
            "id": row["id"],
            "session_id": row["session_id"],
            "app_name": row["app_name"],
            "user_id": row["user_id"],
            "event_id": row["event_id"],
            "author": row["author"],
            "timestamp": row["timestamp"],
            "content_json": _decode_json_field(row["content_json"]) or {},
            "content_text": row["content_text"],
            "metadata_json": _decode_json_field(row["metadata_json"]),
            "inserted_at": row["inserted_at"],
        }
        for row in rows
    ]
