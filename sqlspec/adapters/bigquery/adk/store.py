"""BigQuery ADK store for Google Agent Development Kit session/event storage."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from sqlspec.extensions.adk import BaseAsyncADKStore, EventRecord, SessionRecord
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.config import BigQueryConfig

logger = get_logger("adapters.bigquery.adk.store")

__all__ = ("BigQueryADKStore",)


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
        config: BigQueryConfig instance.
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".
        dataset_id: Optional dataset ID. If not provided, uses config's dataset_id.
        user_fk_column: Optional FK column DDL. Defaults to None.

    Example:
        from sqlspec.adapters.bigquery import BigQueryConfig
        from sqlspec.adapters.bigquery.adk import BigQueryADKStore

        config = BigQueryConfig(
            connection_config={
                "project": "my-project",
                "dataset_id": "my_dataset",
            }
        )
        store = BigQueryADKStore(config)
        await store.create_tables()

        store_with_fk = BigQueryADKStore(
            config,
            user_fk_column="tenant_id INT64 NOT NULL"
        )
        await store_with_fk.create_tables()

    Notes:
        - JSON type for state, content, and metadata (native BigQuery JSON)
        - BYTES for pre-serialized actions from Google ADK
        - TIMESTAMP for timezone-aware microsecond precision
        - Partitioned by DATE(create_time) for cost optimization
        - Clustered by app_name, user_id for query performance
        - Uses to_json/from_json for serialization to JSON columns
        - BigQuery has eventual consistency - handle appropriately
        - No true foreign keys but implements cascade delete pattern
    """

    __slots__ = ("_dataset_id",)

    def __init__(
        self,
        config: "BigQueryConfig",
        session_table: str = "adk_sessions",
        events_table: str = "adk_events",
        dataset_id: "str | None" = None,
        user_fk_column: "str | None" = None,
    ) -> None:
        """Initialize BigQuery ADK store.

        Args:
            config: BigQueryConfig instance.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
            dataset_id: Optional dataset ID override.
            user_fk_column: Optional FK column DDL (e.g., "tenant_id INT64 NOT NULL").
        """
        super().__init__(config, session_table, events_table, user_fk_column)
        self._dataset_id = dataset_id or config.connection_config.get("dataset_id")

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

    def _get_create_sessions_table_sql(self) -> str:
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
            - Optional user FK column for multi-tenant scenarios
            - Note: BigQuery doesn't enforce FK constraints
        """
        user_fk_line = ""
        if self._user_fk_column_ddl:
            user_fk_line = f",\n            {self._user_fk_column_ddl}"

        table_name = self._get_full_table_name(self._session_table)
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL{user_fk_line},
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL,
            update_time TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(create_time)
        CLUSTER BY app_name, user_id
        """

    def _get_create_events_table_sql(self) -> str:
        """Get BigQuery CREATE TABLE SQL for events.

        Returns:
            SQL statement to create adk_events table.

        Notes:
            - STRING for IDs and text fields
            - BYTES for pickled actions
            - JSON for content, grounding_metadata, custom_metadata
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
            long_running_tool_ids_json STRING,
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
        with self._config.provide_connection() as conn:
            conn.query(self._get_create_sessions_table_sql()).result()
            conn.query(self._get_create_events_table_sql()).result()
        logger.debug("Created BigQuery ADK tables: %s, %s", self._session_table, self._events_table)

    async def create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        await async_(self._create_tables)()

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", user_fk: "Any | None" = None
    ) -> SessionRecord:
        """Synchronous implementation of create_session."""
        now = datetime.now(timezone.utc)
        state_json = to_json(state) if state else "{}"

        table_name = self._get_full_table_name(self._session_table)

        if self._user_fk_column_name:
            sql = f"""
            INSERT INTO {table_name} (id, app_name, user_id, {self._user_fk_column_name}, state, create_time, update_time)
            VALUES (@id, @app_name, @user_id, @user_fk, JSON(@state), @create_time, @update_time)
            """

            params = [
                ScalarQueryParameter("id", "STRING", session_id),
                ScalarQueryParameter("app_name", "STRING", app_name),
                ScalarQueryParameter("user_id", "STRING", user_id),
                ScalarQueryParameter("user_fk", "STRING", str(user_fk) if user_fk is not None else None),
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
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", user_fk: "Any | None" = None
    ) -> SessionRecord:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            user_fk: Optional FK value for user_fk_column (if configured).

        Returns:
            Created session record.

        Notes:
            Uses CURRENT_TIMESTAMP() for timestamps.
            State is JSON-serialized then stored in JSON column.
            If user_fk_column is configured, user_fk value must be provided.
            BigQuery doesn't enforce FK constraints, but column is useful for JOINs.
        """
        return await async_(self._create_session)(session_id, app_name, user_id, state, user_fk)

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

    def _list_sessions(self, app_name: str, user_id: str) -> "list[SessionRecord]":
        """Synchronous implementation of list_sessions."""
        table_name = self._get_full_table_name(self._session_table)
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

    async def list_sessions(self, app_name: str, user_id: str) -> "list[SessionRecord]":
        """List all sessions for a user in an app.

        Args:
            app_name: Application name.
            user_id: User identifier.

        Returns:
            List of session records ordered by update_time DESC.

        Notes:
            Uses clustering on (app_name, user_id) for efficiency.
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

        params = [
            ScalarQueryParameter("id", "STRING", event_record["id"]),
            ScalarQueryParameter("session_id", "STRING", event_record["session_id"]),
            ScalarQueryParameter("app_name", "STRING", event_record["app_name"]),
            ScalarQueryParameter("user_id", "STRING", event_record["user_id"]),
            ScalarQueryParameter("invocation_id", "STRING", event_record.get("invocation_id")),
            ScalarQueryParameter("author", "STRING", event_record.get("author")),
            ScalarQueryParameter("actions", "BYTES", event_record.get("actions")),
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
