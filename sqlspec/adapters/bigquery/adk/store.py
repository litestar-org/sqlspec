"""BigQuery ADK store for Google Agent Development Kit session/event storage."""

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.config import BigQueryConfig
    from sqlspec.extensions.adk import MemoryRecord


__all__ = ("BigQueryADKMemoryStore", "BigQueryADKStore")


class BigQueryADKStore(BaseAsyncADKStore["BigQueryConfig"]):
    """BigQuery ADK store using synchronous BigQuery client with async wrapper.

    Implements session and event storage for Google Agent Development Kit
    using Google Cloud BigQuery. Uses BigQuery's native JSON type for state/metadata
    storage and async_() wrapper to provide async interface.

    Provides:
    - Serverless, scalable session state management with JSON storage
    - Event history tracking optimized for analytics
    - Microsecond-precision timestamps with TIMESTAMP type
    - Cost-optimized queries with partitioning and clustering
    - Efficient JSON handling with BigQuery's JSON type
    - Manual cascade delete pattern (no foreign key support)

    Args:
        config: BigQueryConfig with extension_config["adk"] settings.

    Example:
        from sqlspec.adapters.bigquery import BigQueryConfig
        from sqlspec.adapters.bigquery.adk import BigQueryADKStore

        config = BigQueryConfig(
            connection_config={
                "project": "my-project",
                "dataset_id": "my_dataset",
            },
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "owner_id_column": "tenant_id INT64 NOT NULL"
                }
            }
        )
        store = BigQueryADKStore(config)
        await store.ensure_tables()

    Notes:
        - JSON type for state, content, and metadata (native BigQuery JSON)
        - BYTES for pre-serialized actions from Google ADK
        - TIMESTAMP for timezone-aware microsecond precision
        - Partitioned by DATE(create_time) for cost optimization
        - Clustered by app_name, user_id for query performance
        - Uses to_json/from_json for serialization to JSON columns
        - BigQuery has eventual consistency - handle appropriately
        - No true foreign keys but implements cascade delete pattern
        - Configuration is read from config.extension_config["adk"]
    """

    __slots__ = ("_dataset_id",)

    def __init__(self, config: "BigQueryConfig") -> None:
        """Initialize BigQuery ADK store.

        Args:
            config: BigQueryConfig instance.

        Notes:
            Configuration is read from config.extension_config["adk"]:
            - session_table: Sessions table name (default: "adk_sessions")
            - events_table: Events table name (default: "adk_events")
            - owner_id_column: Optional owner FK column DDL (default: None)
        """
        super().__init__(config)
        self._dataset_id = config.connection_config.get("dataset_id")

    def _get_full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name for BigQuery.

        Args:
            table_name: Base table name.

        Returns:
            Fully qualified table name with backticks.

        Notes:
            BigQuery requires backtick-quoted identifiers for table names.
            Format: `project.dataset.table` or `dataset.table`
        """
        if self._dataset_id:
            return f"`{self._dataset_id}.{table_name}`"
        return f"`{table_name}`"

    async def _get_create_sessions_table_sql(self) -> str:
        """Get BigQuery CREATE TABLE SQL for sessions.

        Returns:
            SQL statement to create adk_sessions table.

        Notes:
            - STRING for IDs and names
            - JSON type for state storage (native BigQuery JSON)
            - TIMESTAMP for timezone-aware microsecond precision
            - Partitioned by DATE(create_time) for cost optimization
            - Clustered by app_name, user_id for query performance
            - No indexes needed (BigQuery auto-optimizes)
            - Optional owner ID column for multi-tenant scenarios
            - Note: BigQuery doesn't enforce FK constraints
        """
        owner_id_line = ""
        if self._owner_id_column_ddl:
            owner_id_line = f",\n            {self._owner_id_column_ddl}"

        table_name = self._get_full_table_name(self._session_table)
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL{owner_id_line},
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL,
            update_time TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(create_time)
        CLUSTER BY app_name, user_id
        """

    async def _get_create_events_table_sql(self) -> str:
        """Get BigQuery CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table.

        Notes:
            - STRING for IDs and text fields
            - BYTES for pickled actions
            - JSON for content, grounding_metadata, custom_metadata, long_running_tool_ids_json
            - BOOL for boolean flags
            - TIMESTAMP for timezone-aware timestamps
            - Partitioned by DATE(timestamp) for cost optimization
            - Clustered by session_id, timestamp for ordered retrieval
        """
        table_name = self._get_full_table_name(self._events_table)
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING NOT NULL,
            session_id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            invocation_id STRING,
            author STRING,
            actions BYTES,
            long_running_tool_ids_json JSON,
            branch STRING,
            timestamp TIMESTAMP NOT NULL,
            content JSON,
            grounding_metadata JSON,
            custom_metadata JSON,
            partial BOOL,
            turn_complete BOOL,
            interrupted BOOL,
            error_code STRING,
            error_message STRING
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY session_id, timestamp
        """

    def _get_drop_tables_sql(self) -> "list[str]":
        """Get BigQuery DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables.

        Notes:
            Order matters: drop events table before sessions table.
            BigQuery uses IF EXISTS for idempotent drops.
        """
        events_table = self._get_full_table_name(self._events_table)
        sessions_table = self._get_full_table_name(self._session_table)
        return [f"DROP TABLE IF EXISTS {events_table}", f"DROP TABLE IF EXISTS {sessions_table}"]

    def _create_tables(self) -> None:
        """Synchronous implementation of create_tables."""
        with self._config.provide_session() as driver:
            driver.execute_script(run_(self._get_create_sessions_table_sql)())
            driver.execute_script(run_(self._get_create_events_table_sql)())

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        await async_(self._create_tables)()

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Synchronous implementation of create_session."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state) if state else "{}"

        table_name = self._get_full_table_name(self._session_table)

        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {table_name} (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (@id, @app_name, @user_id, @owner_id, JSON(@state), @create_time, @update_time)
            """

            params = [
                ScalarQueryParameter("id", "STRING", session_id),
                ScalarQueryParameter("app_name", "STRING", app_name),
                ScalarQueryParameter("user_id", "STRING", user_id),
                ScalarQueryParameter("owner_id", "STRING", str(owner_id) if owner_id is not None else None),
                ScalarQueryParameter("state", "STRING", state_json),
                ScalarQueryParameter("create_time", "TIMESTAMP", now),
                ScalarQueryParameter("update_time", "TIMESTAMP", now),
            ]
        else:
            sql = f"""
            INSERT INTO {table_name} (id, app_name, user_id, state, create_time, update_time)
            VALUES (@id, @app_name, @user_id, JSON(@state), @create_time, @update_time)
            """

            params = [
                ScalarQueryParameter("id", "STRING", session_id),
                ScalarQueryParameter("app_name", "STRING", app_name),
                ScalarQueryParameter("user_id", "STRING", user_id),
                ScalarQueryParameter("state", "STRING", state_json),
                ScalarQueryParameter("create_time", "TIMESTAMP", now),
                ScalarQueryParameter("update_time", "TIMESTAMP", now),
            ]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            conn.query(sql, job_config=job_config).result()

        return SessionRecord(
            id=session_id, app_name=app_name, user_id=user_id, state=state, create_time=now, update_time=now
        )

    async def create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Created session record.

        Notes:
            Uses CURRENT_TIMESTAMP() for timestamps.
            State is JSON-serialized then stored in JSON column.
            If owner_id_column is configured, owner_id value must be provided.
            BigQuery doesn't enforce FK constraints, but column is useful for JOINs.
        """
        return await async_(self._create_session)(session_id, app_name, user_id, state, owner_id)

    def _get_session(self, session_id: str) -> "SessionRecord | None":
        """Synchronous implementation of get_session."""
        table_name = self._get_full_table_name(self._session_table)
        sql = f"""
        SELECT id, app_name, user_id, JSON_VALUE(state) as state, create_time, update_time
        FROM {table_name}
        WHERE id = @session_id
        """

        params = [ScalarQueryParameter("session_id", "STRING", session_id)]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            query_job = conn.query(sql, job_config=job_config)
            results = list(query_job.result())

            if not results:
                return None

            row = results[0]
            return SessionRecord(
                id=row.id,
                app_name=row.app_name,
                user_id=row.user_id,
                state=from_json(row.state) if row.state else {},
                create_time=row.create_time,
                update_time=row.update_time,
            )

    async def get_session(self, session_id: str) -> "SessionRecord | None":
        """Get session by ID.

        Args:
            session_id: Session identifier.

        Returns:
            Session record or None if not found.

        Notes:
            BigQuery returns datetime objects for TIMESTAMP columns.
            JSON_VALUE extracts string representation for parsing.
        """
        return await async_(self._get_session)(session_id)

    def _update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Synchronous implementation of update_session_state."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state) if state else "{}"

        table_name = self._get_full_table_name(self._session_table)
        sql = f"""
        UPDATE {table_name}
        SET state = JSON(@state), update_time = @update_time
        WHERE id = @session_id
        """

        params = [
            ScalarQueryParameter("state", "STRING", state_json),
            ScalarQueryParameter("update_time", "TIMESTAMP", now),
            ScalarQueryParameter("session_id", "STRING", session_id),
        ]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            conn.query(sql, job_config=job_config).result()

    async def update_session_state(self, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).

        Notes:
            Replaces entire state dictionary.
            Updates update_time to CURRENT_TIMESTAMP().
        """
        await async_(self._update_session_state)(session_id, state)

    def _list_sessions(self, app_name: str, user_id: "str | None") -> "list[SessionRecord]":
        """Synchronous implementation of list_sessions."""
        table_name = self._get_full_table_name(self._session_table)

        if user_id is None:
            sql = f"""
            SELECT id, app_name, user_id, JSON_VALUE(state) as state, create_time, update_time
            FROM {table_name}
            WHERE app_name = @app_name
            ORDER BY update_time DESC
            """
            params = [ScalarQueryParameter("app_name", "STRING", app_name)]
        else:
            sql = f"""
            SELECT id, app_name, user_id, JSON_VALUE(state) as state, create_time, update_time
            FROM {table_name}
            WHERE app_name = @app_name AND user_id = @user_id
            ORDER BY update_time DESC
            """
            params = [
                ScalarQueryParameter("app_name", "STRING", app_name),
                ScalarQueryParameter("user_id", "STRING", user_id),
            ]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            query_job = conn.query(sql, job_config=job_config)
            results = list(query_job.result())

            return [
                SessionRecord(
                    id=row.id,
                    app_name=row.app_name,
                    user_id=row.user_id,
                    state=from_json(row.state) if row.state else {},
                    create_time=row.create_time,
                    update_time=row.update_time,
                )
                for row in results
            ]

    async def list_sessions(self, app_name: str, user_id: str | None = None) -> "list[SessionRecord]":
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses clustering on (app_name, user_id) when user_id is provided for efficiency.
        """
        return await async_(self._list_sessions)(app_name, user_id)

    def _delete_session(self, session_id: str) -> None:
        """Synchronous implementation of delete_session."""
        events_table = self._get_full_table_name(self._events_table)
        sessions_table = self._get_full_table_name(self._session_table)

        params = [ScalarQueryParameter("session_id", "STRING", session_id)]

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            conn.query(f"DELETE FROM {events_table} WHERE session_id = @session_id", job_config=job_config).result()
            conn.query(f"DELETE FROM {sessions_table} WHERE id = @session_id", job_config=job_config).result()

    async def delete_session(self, session_id: str) -> None:
        """Delete session and all associated events.

        Args:
            session_id: Session identifier.

        Notes:
            BigQuery doesn't support foreign keys, so we manually delete events first.
            Uses two separate DELETE statements in sequence.
        """
        await async_(self._delete_session)(session_id)

    def _append_event(self, event_record: EventRecord) -> None:
        """Synchronous implementation of append_event."""
        table_name = self._get_full_table_name(self._events_table)

        content_json = to_json(event_record.get("content")) if event_record.get("content") else None
        grounding_metadata_json = (
            to_json(event_record.get("grounding_metadata")) if event_record.get("grounding_metadata") else None
        )
        custom_metadata_json = (
            to_json(event_record.get("custom_metadata")) if event_record.get("custom_metadata") else None
        )

        sql = f"""
        INSERT INTO {table_name} (
            id, session_id, app_name, user_id, invocation_id, author, actions,
            long_running_tool_ids_json, branch, timestamp, content,
            grounding_metadata, custom_metadata, partial, turn_complete,
            interrupted, error_code, error_message
        ) VALUES (
            @id, @session_id, @app_name, @user_id, @invocation_id, @author, @actions,
            @long_running_tool_ids_json, @branch, @timestamp,
            {"JSON(@content)" if content_json else "NULL"},
            {"JSON(@grounding_metadata)" if grounding_metadata_json else "NULL"},
            {"JSON(@custom_metadata)" if custom_metadata_json else "NULL"},
            @partial, @turn_complete, @interrupted, @error_code, @error_message
        )
        """

        actions_value = event_record.get("actions")
        params = [
            ScalarQueryParameter("id", "STRING", event_record["id"]),
            ScalarQueryParameter("session_id", "STRING", event_record["session_id"]),
            ScalarQueryParameter("app_name", "STRING", event_record["app_name"]),
            ScalarQueryParameter("user_id", "STRING", event_record["user_id"]),
            ScalarQueryParameter("invocation_id", "STRING", event_record.get("invocation_id")),
            ScalarQueryParameter("author", "STRING", event_record.get("author")),
            ScalarQueryParameter(
                "actions",
                "BYTES",
                actions_value.decode("latin1") if isinstance(actions_value, bytes) else actions_value,
            ),
            ScalarQueryParameter(
                "long_running_tool_ids_json", "STRING", event_record.get("long_running_tool_ids_json")
            ),
            ScalarQueryParameter("branch", "STRING", event_record.get("branch")),
            ScalarQueryParameter("timestamp", "TIMESTAMP", event_record["timestamp"]),
            ScalarQueryParameter("partial", "BOOL", event_record.get("partial")),
            ScalarQueryParameter("turn_complete", "BOOL", event_record.get("turn_complete")),
            ScalarQueryParameter("interrupted", "BOOL", event_record.get("interrupted")),
            ScalarQueryParameter("error_code", "STRING", event_record.get("error_code")),
            ScalarQueryParameter("error_message", "STRING", event_record.get("error_message")),
        ]

        if content_json:
            params.append(ScalarQueryParameter("content", "STRING", content_json))
        if grounding_metadata_json:
            params.append(ScalarQueryParameter("grounding_metadata", "STRING", grounding_metadata_json))
        if custom_metadata_json:
            params.append(ScalarQueryParameter("custom_metadata", "STRING", custom_metadata_json))

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            conn.query(sql, job_config=job_config).result()

    async def append_event(self, event_record: EventRecord) -> None:
        """Append an event to a session.

        Args:
            event_record: Event record to store.

        Notes:
            Uses BigQuery TIMESTAMP for timezone-aware timestamps.
            JSON fields are serialized to STRING then cast to JSON.
            Boolean fields stored natively as BOOL.
        """
        await async_(self._append_event)(event_record)

    def _get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Synchronous implementation of get_events."""
        table_name = self._get_full_table_name(self._events_table)

        where_clauses = ["session_id = @session_id"]
        params: list[ScalarQueryParameter] = [ScalarQueryParameter("session_id", "STRING", session_id)]

        if after_timestamp is not None:
            where_clauses.append("timestamp > @after_timestamp")
            params.append(ScalarQueryParameter("after_timestamp", "TIMESTAMP", after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = f" LIMIT {limit}" if limit else ""

        sql = f"""
        SELECT id, session_id, app_name, user_id, invocation_id, author, actions,
               long_running_tool_ids_json, branch, timestamp,
               JSON_VALUE(content) as content,
               JSON_VALUE(grounding_metadata) as grounding_metadata,
               JSON_VALUE(custom_metadata) as custom_metadata,
               partial, turn_complete, interrupted, error_code, error_message
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY timestamp ASC{limit_clause}
        """

        with self._config.provide_connection() as conn:
            job_config = QueryJobConfig(query_parameters=params)
            query_job = conn.query(sql, job_config=job_config)
            results = list(query_job.result())

            return [
                EventRecord(
                    id=row.id,
                    session_id=row.session_id,
                    app_name=row.app_name,
                    user_id=row.user_id,
                    invocation_id=row.invocation_id,
                    author=row.author,
                    actions=bytes(row.actions) if row.actions else b"",
                    long_running_tool_ids_json=row.long_running_tool_ids_json,
                    branch=row.branch,
                    timestamp=row.timestamp,
                    content=from_json(row.content) if row.content else None,
                    grounding_metadata=from_json(row.grounding_metadata) if row.grounding_metadata else None,
                    custom_metadata=from_json(row.custom_metadata) if row.custom_metadata else None,
                    partial=row.partial,
                    turn_complete=row.turn_complete,
                    interrupted=row.interrupted,
                    error_code=row.error_code,
                    error_message=row.error_message,
                )
                for row in results
            ]

    async def get_events(
        self, session_id: str, after_timestamp: "datetime | None" = None, limit: "int | None" = None
    ) -> "list[EventRecord]":
        """Get events for a session.

        Args:
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.

        Notes:
            Uses clustering on (session_id, timestamp) for efficient retrieval.
            Parses JSON fields and converts BYTES actions to bytes.
        """
        return await async_(self._get_events)(session_id, after_timestamp, limit)


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

    async def create_tables(self) -> None:
        """Create the memory table if it doesn't exist."""
        if not self._enabled:
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
