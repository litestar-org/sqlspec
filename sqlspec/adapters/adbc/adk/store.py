"""ADBC ADK store for Google Agent Development Kit session/event storage."""

import contextlib
import struct
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Literal, NotRequired, cast

from sqlspec.adapters.adbc.core import (
    _DB2_INDEX_EXISTS_SQL,
    _DB2_TABLE_EXISTS_SQL,
    driver_kind_from_driver_name,
    is_postgres_dialect,
    is_shared_object_driver,
    resolve_dialect_from_config,
)
from sqlspec.adapters.adbc.core import split_db2_table_name as _split_db2_table_name
from sqlspec.config import ADKConfig
from sqlspec.core.parameters._validator import PARAMETER_REGEX
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent, StoredSession, normalize_session_list_options
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.adbc.config import AdbcConfig
    from sqlspec.extensions.adk import SessionOrderBy, StoredMemory

__all__ = ("AdbcADKConfig", "AdbcADKMemoryStore", "AdbcADKStore", "decode_adbc_pgvector", "format_pgvector_literal")

logger = get_logger("sqlspec.adapters.adbc.adk.store")


class AdbcADKConfig(ADKConfig):
    """ADBC-specific ADK extension settings.

    Use these keys inside ``extension_config["adk"]`` with the ADBC ADK stores.
    """

    enable_bm25: NotRequired[bool]
    """Enable pg_textsearch BM25 indexing for hybrid vector search on PostgreSQL. Defaults to False."""

    vector_dimensions: NotRequired[int]
    """Dimensions for the vector embedding column on PostgreSQL. Defaults to 768."""

    vector_index_type: NotRequired[Literal["hnsw", "ivfflat", "scann", "none"]]
    """Vector index type for PostgreSQL. Defaults to 'hnsw'."""

    scann_num_leaves: NotRequired[int]
    """Number of leaves for ScaNN index on PostgreSQL. Defaults to 100."""

    scann_quantizer: NotRequired[Literal["SQ8", "FP32"]]
    """Quantizer for ScaNN index on PostgreSQL. Defaults to 'SQ8'."""


DIALECT_POSTGRESQL: Final = "postgresql"
DIALECT_SQLITE: Final = "sqlite"
DIALECT_DUCKDB: Final = "duckdb"
DIALECT_BIGQUERY: Final = "bigquery"
DIALECT_SNOWFLAKE: Final = "snowflake"
DIALECT_ORACLE: Final = "oracle"
DIALECT_DB2: Final = "db2"
DIALECT_MSSQL: Final = "mssql"
DIALECT_GENERIC: Final = "generic"

ADBC_TABLE_NOT_FOUND_PATTERNS: Final = (
    "no such table",
    "table or view does not exist",
    "relation does not exist",
    "does not exist",
    "table with name",
    "invalid object name",
    "undefined name",
    "ora-00942",
    "sql0204n",
)


class AdbcADKStore(BaseSyncADKStore["AdbcConfig"]):
    """ADBC synchronous ADK store for Arrow Database Connectivity.

    Implements session and event storage for Google Agent Development Kit
    using ADBC. ADBC provides a vendor-neutral API with Arrow-native data
    transfer across multiple databases (PostgreSQL, SQLite, DuckDB, etc.).

    Events use the clean-break contract: id, session_id, invocation_id,
    timestamp, and event_data. The full ADK Event payload is stored as a
    single JSON blob in event_data using a dialect-appropriate column type
    (JSONB for PostgreSQL, JSON for DuckDB and BigQuery, VARIANT for Snowflake,
    TEXT for SQLite and generic fallback).

    Provides:
        - Session state management with JSON serialization
        - Event history tracking via single event_data blob
        - Atomic event insert + session state update
        - Timezone-aware timestamps
        - Foreign key constraints with cascade delete
        - Database-agnostic SQL (supports multiple backends)

    Args:
        config: AdbcConfig with extension_config["adk"] settings.
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC ADK store.

        Args:
            config: AdbcConfig instance (any ADBC driver).
        """
        super().__init__(config)
        self._dialect = self._detect_dialect()

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
        return self._get_session(app_name, user_id, session_id, renew_for=renew_for)

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
        return self._list_sessions(
            app_name, user_id, order_by=order_by, descending=descending, limit=limit, offset=offset
        )

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
        return self._get_events(app_name, user_id, session_id, after_timestamp, limit)

    def delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        """Delete events older than a timestamp."""
        return self._delete_expired_events(before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete sessions older than a timestamp."""
        return self._delete_idle_sessions(updated_before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        """Delete user state rows older than a timestamp."""
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

    @property
    def dialect(self) -> str:
        """Return the detected database dialect."""
        return self._dialect

    def _detect_dialect(self) -> str:
        """Detect ADBC driver dialect from connection config.

        Returns:
            Dialect identifier for DDL generation.
        """
        driver_name = self._config.connection_config.get("driver_name", "")
        if (
            isinstance(driver_name, str)
            and driver_name
            and driver_kind_from_driver_name(driver_name) is None
            and not is_shared_object_driver(driver_name)
        ):
            logger.warning(
                "Unknown ADBC driver: %s. Using generic SQL dialect. "
                "Consider using a direct adapter for better performance.",
                driver_name,
            )
            return DIALECT_GENERIC

        resolved = resolve_dialect_from_config(self._config.connection_config)
        stmt_dialect = self._config.statement_config.dialect
        if (isinstance(stmt_dialect, str) and stmt_dialect.lower() in {"oracle", "db2", "mssql", "tsql"}) or (
            resolved is None
            and isinstance(stmt_dialect, str)
            and stmt_dialect.lower() in {"sqlite", "duckdb", "bigquery", "snowflake"}
        ):
            resolved = stmt_dialect.lower()
        if is_postgres_dialect(resolved):
            return DIALECT_POSTGRESQL
        if resolved == "sqlite":
            return DIALECT_SQLITE
        if resolved == "duckdb":
            return DIALECT_DUCKDB
        if resolved == "bigquery":
            return DIALECT_BIGQUERY
        if resolved == "snowflake":
            return DIALECT_SNOWFLAKE
        if resolved == "oracle":
            return DIALECT_ORACLE
        if resolved == "db2":
            return DIALECT_DB2
        if resolved in {"mssql", "tsql"}:
            return DIALECT_MSSQL

        logger.warning(
            "Unknown ADBC driver: %s. Using generic SQL dialect. "
            "Consider using a direct adapter for better performance.",
            driver_name,
        )
        return DIALECT_GENERIC

    def _encode_timestamp(self, value: datetime) -> Any:
        if self._dialect == DIALECT_SQLITE:
            return value.timestamp()
        return value

    def _decode_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    def _serialize_state(self, state: "dict[str, Any]") -> str:
        """Serialize state dictionary to JSON string.

        Args:
            state: State dictionary to serialize.

        Returns:
            JSON string.
        """
        return to_json(state)

    def _deserialize_state(self, data: Any) -> "dict[str, Any]":
        """Deserialize state data from JSON string.

        Args:
            data: JSON string from database.

        Returns:
            Deserialized state dictionary.
        """
        if data is None:
            return {}
        return cast("dict[str, Any]", from_json(str(data)))

    def _serialize_json_field(self, value: Any) -> "str | None":
        """Serialize optional JSON field for event storage.

        Args:
            value: Value to serialize (dict or None).

        Returns:
            Serialized JSON string or None.
        """
        if value is None:
            return None
        return to_json(value)

    def _json_storage_type(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return "JSONB"
        if self._dialect in {DIALECT_DUCKDB, DIALECT_BIGQUERY}:
            return "JSON"
        if self._dialect == DIALECT_SNOWFLAKE:
            return "VARIANT"
        if self._dialect == DIALECT_ORACLE:
            return "CLOB"
        if self._dialect == DIALECT_DB2:
            return "CLOB(1M)"
        if self._dialect == DIALECT_MSSQL:
            return "NVARCHAR(MAX)"
        return "TEXT"

    def _timestamp_storage_type(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return "TIMESTAMPTZ"
        if self._dialect == DIALECT_SNOWFLAKE:
            return "TIMESTAMP_TZ"
        if self._dialect == DIALECT_SQLITE:
            return "REAL"
        if self._dialect == DIALECT_ORACLE:
            return "TIMESTAMP WITH TIME ZONE"
        if self._dialect == DIALECT_DB2:
            return "TIMESTAMP"
        if self._dialect == DIALECT_MSSQL:
            return "DATETIME2"
        return "TIMESTAMP"

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        """Deserialize optional JSON field from database.

        Args:
            data: JSON string from database or None.

        Returns:
            Deserialized dictionary or None.
        """
        if data is None:
            return None
        return cast("dict[str, Any]", from_json(str(data)))

    def _sessions_table_ddl(self) -> str:
        """Get CREATE TABLE SQL for sessions with dialect dispatch.

        Returns:
            SQL statement to create adk_sessions table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._sessions_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._sessions_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._sessions_ddl_duckdb()
        if self._dialect == DIALECT_BIGQUERY:
            return self._sessions_ddl_bigquery()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._sessions_ddl_snowflake()
        if self._dialect == DIALECT_ORACLE:
            return self._sessions_ddl_oracle()
        if self._dialect == DIALECT_DB2:
            return self._sessions_ddl_db2()
        if self._dialect == DIALECT_MSSQL:
            return self._sessions_ddl_mssql()
        return self._sessions_ddl_generic()

    def _sessions_ddl_postgresql(self) -> str:
        """PostgreSQL DDL with JSONB and TIMESTAMPTZ.

        Returns:
            SQL to create sessions table optimized for PostgreSQL.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _sessions_ddl_sqlite(self) -> str:
        """SQLite DDL with TEXT and REAL timestamps.

        Returns:
            SQL to create sessions table optimized for SQLite.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL{owner_id_ddl},
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time REAL NOT NULL,
            update_time REAL NOT NULL
        )
        """

    def _sessions_ddl_duckdb(self) -> str:
        """DuckDB DDL with native JSON type.

        Returns:
            SQL to create sessions table optimized for DuckDB.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _sessions_ddl_bigquery(self) -> str:
        """BigQuery DDL with JSON and TIMESTAMP.

        Returns:
            SQL to create sessions table optimized for BigQuery.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL{owner_id_ddl},
            state JSON NOT NULL,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _sessions_ddl_snowflake(self) -> str:
        """Snowflake DDL with VARIANT type.

        Returns:
            SQL to create sessions table optimized for Snowflake.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL{owner_id_ddl},
            state VARIANT NOT NULL,
            create_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            update_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _sessions_ddl_oracle(self) -> str:
        """Oracle DDL with CLOB and TIMESTAMP WITH TIME ZONE."""
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._session_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL{owner_id_ddl},
                state CLOB NOT NULL,
                create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _sessions_ddl_db2(self) -> str:
        """Db2 DDL with CLOB(1M) and TIMESTAMP."""
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE {self._session_table} (
            id VARCHAR(128) NOT NULL PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state CLOB(1M) NOT NULL,
            create_time TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
            update_time TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP
        )
        """

    def _sessions_ddl_mssql(self) -> str:
        """SQL Server DDL with NVARCHAR(MAX) and DATETIME2."""
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        IF OBJECT_ID(N'{self._session_table}', N'U') IS NULL
        CREATE TABLE {self._session_table} (
            id NVARCHAR(128) NOT NULL PRIMARY KEY,
            app_name NVARCHAR(128) NOT NULL,
            user_id NVARCHAR(128) NOT NULL{owner_id_ddl},
            state NVARCHAR(MAX) NOT NULL,
            create_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            update_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )
        """

    def _sessions_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL fallback.

        Returns:
            SQL to create sessions table using generic types.
        """
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._session_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL{owner_id_ddl},
            state TEXT NOT NULL DEFAULT '{{}}',
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _events_table_ddl(self) -> str:
        """Get CREATE TABLE SQL for events with dialect dispatch.

        Returns:
            SQL statement to create adk_events table.
        """
        if self._dialect == DIALECT_POSTGRESQL:
            return self._events_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._events_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._events_ddl_duckdb()
        if self._dialect == DIALECT_BIGQUERY:
            return self._events_ddl_bigquery()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._events_ddl_snowflake()
        if self._dialect == DIALECT_ORACLE:
            return self._events_ddl_oracle()
        if self._dialect == DIALECT_DB2:
            return self._events_ddl_db2()
        if self._dialect == DIALECT_MSSQL:
            return self._events_ddl_mssql()
        return self._events_ddl_generic()

    def _events_ddl_postgresql(self) -> str:
        """PostgreSQL DDL for events table.

        Returns:
            SQL to create events table optimized for PostgreSQL.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSONB NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_sqlite(self) -> str:
        """SQLite DDL for events table.

        Returns:
            SQL to create events table optimized for SQLite.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            invocation_id TEXT,
            timestamp REAL NOT NULL,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_duckdb(self) -> str:
        """DuckDB DDL for events table.

        Returns:
            SQL to create events table optimized for DuckDB.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _events_ddl_bigquery(self) -> str:
        """BigQuery DDL for events table.

        Returns:
            SQL to create events table optimized for BigQuery.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            session_id STRING NOT NULL,
            invocation_id STRING,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            event_data JSON NOT NULL
        )
        """

    def _events_ddl_snowflake(self) -> str:
        """Snowflake DDL for events table.

        Returns:
            SQL to create events table optimized for Snowflake.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            event_data VARIANT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id)
        )
        """

    def _events_ddl_oracle(self) -> str:
        """Oracle DDL for events table."""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._events_table} (
                id VARCHAR2(128) PRIMARY KEY,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                session_id VARCHAR2(128) NOT NULL,
                invocation_id VARCHAR2(256),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                event_data CLOB NOT NULL,
                FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _events_ddl_db2(self) -> str:
        """Db2 DDL for events table."""
        return f"""
        CREATE TABLE {self._events_table} (
            id VARCHAR(128) NOT NULL PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
            event_data CLOB(1M) NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_mssql(self) -> str:
        """SQL Server DDL for events table."""
        return f"""
        IF OBJECT_ID(N'{self._events_table}', N'U') IS NULL
        CREATE TABLE {self._events_table} (
            id NVARCHAR(128) NOT NULL PRIMARY KEY,
            app_name NVARCHAR(128) NOT NULL,
            user_id NVARCHAR(128) NOT NULL,
            session_id NVARCHAR(128) NOT NULL,
            invocation_id NVARCHAR(256),
            timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
            event_data NVARCHAR(MAX) NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _events_ddl_generic(self) -> str:
        """Generic SQL-92 compatible DDL for events table.

        Returns:
            SQL to create events table using generic types.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._events_table} (
            id VARCHAR(128) PRIMARY KEY,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            session_id VARCHAR(128) NOT NULL,
            invocation_id VARCHAR(256),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(id) ON DELETE CASCADE
        )
        """

    def _app_states_table_ddl(self) -> str:
        if self._dialect == DIALECT_BIGQUERY:
            return f"""
            CREATE TABLE IF NOT EXISTS {self._app_state_table} (
                app_name STRING NOT NULL,
                state JSON NOT NULL,
                update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
            )
            """
        if self._dialect == DIALECT_ORACLE:
            return f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self._app_state_table} (
                    app_name VARCHAR2(128) PRIMARY KEY,
                    state CLOB NOT NULL,
                    update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        if self._dialect == DIALECT_DB2:
            return f"""
            CREATE TABLE {self._app_state_table} (
                app_name VARCHAR(128) NOT NULL PRIMARY KEY,
                state CLOB(1M) NOT NULL,
                update_time TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP
            )
            """
        if self._dialect == DIALECT_MSSQL:
            return f"""
            IF OBJECT_ID(N'{self._app_state_table}', N'U') IS NULL
            CREATE TABLE {self._app_state_table} (
                app_name NVARCHAR(128) NOT NULL PRIMARY KEY,
                state NVARCHAR(MAX) NOT NULL,
                update_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
            """
        json_type = self._json_storage_type()
        timestamp_type = self._timestamp_storage_type()
        if self._dialect == DIALECT_SQLITE:
            default = ""
        elif self._dialect == DIALECT_SNOWFLAKE:
            default = " DEFAULT CURRENT_TIMESTAMP()"
        else:
            default = " DEFAULT CURRENT_TIMESTAMP"
        return f"""
        CREATE TABLE IF NOT EXISTS {self._app_state_table} (
            app_name VARCHAR(128) PRIMARY KEY,
            state {json_type} NOT NULL,
            update_time {timestamp_type} NOT NULL{default}
        )
        """

    def _user_states_table_ddl(self) -> str:
        if self._dialect == DIALECT_BIGQUERY:
            return f"""
            CREATE TABLE IF NOT EXISTS {self._user_state_table} (
                app_name STRING NOT NULL,
                user_id STRING NOT NULL,
                state JSON NOT NULL,
                update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
            )
            """
        if self._dialect == DIALECT_ORACLE:
            return f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self._user_state_table} (
                    app_name VARCHAR2(128) NOT NULL,
                    user_id VARCHAR2(128) NOT NULL,
                    state CLOB NOT NULL,
                    update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                    PRIMARY KEY (app_name, user_id)
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        if self._dialect == DIALECT_DB2:
            return f"""
            CREATE TABLE {self._user_state_table} (
                app_name VARCHAR(128) NOT NULL,
                user_id VARCHAR(128) NOT NULL,
                state CLOB(1M) NOT NULL,
                update_time TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
                PRIMARY KEY (app_name, user_id)
            )
            """
        if self._dialect == DIALECT_MSSQL:
            return f"""
            IF OBJECT_ID(N'{self._user_state_table}', N'U') IS NULL
            CREATE TABLE {self._user_state_table} (
                app_name NVARCHAR(128) NOT NULL,
                user_id NVARCHAR(128) NOT NULL,
                state NVARCHAR(MAX) NOT NULL,
                update_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                PRIMARY KEY (app_name, user_id)
            )
            """
        json_type = self._json_storage_type()
        timestamp_type = self._timestamp_storage_type()
        if self._dialect == DIALECT_SQLITE:
            default = ""
        elif self._dialect == DIALECT_SNOWFLAKE:
            default = " DEFAULT CURRENT_TIMESTAMP()"
        else:
            default = " DEFAULT CURRENT_TIMESTAMP"
        return f"""
        CREATE TABLE IF NOT EXISTS {self._user_state_table} (
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            state {json_type} NOT NULL,
            update_time {timestamp_type} NOT NULL{default},
            PRIMARY KEY (app_name, user_id)
        )
        """

    def _metadata_table_ddl(self) -> str:
        if self._dialect == DIALECT_BIGQUERY:
            return f"""
            CREATE TABLE IF NOT EXISTS {self._metadata_table} (
                key STRING NOT NULL,
                value STRING NOT NULL
            )
            """
        if self._dialect == DIALECT_ORACLE:
            return f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self._metadata_table} (
                    key VARCHAR2(128) PRIMARY KEY,
                    value VARCHAR2(512) NOT NULL
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        if self._dialect == DIALECT_DB2:
            return f"""
            CREATE TABLE {self._metadata_table} (
                "KEY" VARCHAR(128) NOT NULL PRIMARY KEY,
                value VARCHAR(512) NOT NULL
            )
            """
        if self._dialect == DIALECT_MSSQL:
            return f"""
            IF OBJECT_ID(N'{self._metadata_table}', N'U') IS NULL
            CREATE TABLE {self._metadata_table} (
                [key] NVARCHAR(128) NOT NULL PRIMARY KEY,
                value NVARCHAR(512) NOT NULL
            )
            """
        return f"""
        CREATE TABLE IF NOT EXISTS {self._metadata_table} (
            key VARCHAR(128) PRIMARY KEY,
            value VARCHAR(512) NOT NULL
        )
        """

    def _drop_app_states_table_sql(self) -> str:
        return _drop_table_sql_for_dialect(self._dialect, self._app_state_table)

    def _drop_user_states_table_sql(self) -> str:
        return _drop_table_sql_for_dialect(self._dialect, self._user_state_table)

    def _drop_metadata_table_sql(self) -> str:
        return _drop_table_sql_for_dialect(self._dialect, self._metadata_table)

    def _drop_tables_sql(self) -> "list[str]":
        """Get DROP TABLE SQL statements.

        Returns:
            List of SQL statements to drop tables and indexes.
        """
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            _drop_table_sql_for_dialect(self._dialect, self._events_table),
            _drop_table_sql_for_dialect(self._dialect, self._session_table),
        ]

    def _create_tables(self) -> None:
        """Create both sessions and events tables if they don't exist."""
        skip_indexes = self._dialect in {DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)

                _execute_table_ddl(cursor, self._dialect, self._session_table, self._sessions_table_ddl())
                conn.commit()

                if not skip_indexes:
                    _execute_index_ddl(
                        cursor,
                        self._dialect,
                        f"idx_{self._session_table}_app_user",
                        self._session_table,
                        "app_name, user_id",
                    )
                    conn.commit()

                    _execute_index_ddl(
                        cursor,
                        self._dialect,
                        f"idx_{self._session_table}_update_time",
                        self._session_table,
                        "update_time DESC",
                    )
                    conn.commit()

                _execute_table_ddl(cursor, self._dialect, self._events_table, self._events_table_ddl())
                conn.commit()

                if not skip_indexes:
                    _execute_index_ddl(
                        cursor,
                        self._dialect,
                        f"idx_{self._events_table}_session",
                        self._events_table,
                        "session_id, timestamp ASC",
                    )
                    conn.commit()

                _execute_table_ddl(cursor, self._dialect, self._app_state_table, self._app_states_table_ddl())
                conn.commit()

                _execute_table_ddl(cursor, self._dialect, self._user_state_table, self._user_states_table_ddl())
                conn.commit()

                _execute_table_ddl(cursor, self._dialect, self._metadata_table, self._metadata_table_ddl())
                conn.commit()
            finally:
                cursor.close()

    def _enable_foreign_keys(self, cursor: Any, conn: Any) -> None:
        """Enable foreign key constraints for SQLite.

        Args:
            cursor: Database cursor.
            conn: Database connection.
        """
        if self._dialect != DIALECT_SQLITE:
            return
        try:
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        except Exception:
            logger.debug("Foreign key enforcement not supported or already enabled")

    def _format_sql(self, sql: str) -> str:
        """Return SQL with dialect-appropriate positional placeholders."""
        if self._dialect == DIALECT_POSTGRESQL:
            return _postgresql_store_sql(sql)
        if self._dialect == DIALECT_ORACLE:
            return _oracle_store_sql(sql)
        return sql

    def _execute(self, cursor: Any, sql: str, params: "tuple[Any, ...] | list[Any]") -> Any:
        """Execute parameterized SQL using the current ADBC dialect's placeholder style."""
        return cursor.execute(self._format_sql(sql), params)

    def _session_list_query(
        self, app_name: str, user_id: "str | None", column: str, direction: str, limit: "int | None", offset: int
    ) -> "tuple[str, tuple[Any, ...]]":
        """Return the bounded session-list query in the current dialect's page grammar."""
        params: list[Any] = [app_name]
        where_clause = "app_name = ?"
        if user_id is not None:
            params.append(user_id)
            where_clause = f"{where_clause} AND user_id = ?"

        page_clause = ""
        if limit is not None:
            if self._dialect in {DIALECT_GENERIC, DIALECT_ORACLE, DIALECT_DB2, DIALECT_MSSQL}:
                params.extend((offset, limit))
                page_clause = "\n            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            else:
                params.extend((limit, offset))
                page_clause = "\n            LIMIT ? OFFSET ?"

        sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE {where_clause}
        ORDER BY {column} {direction}, id {direction}{page_clause}
        """
        return sql, tuple(params)

    def _json_placeholder(self) -> str:
        """Return a JSON parameter placeholder for the current dialect."""
        if self._dialect == DIALECT_POSTGRESQL:
            return "?::jsonb"
        return "?"

    def _create_session(
        self, session_id: str, app_name: str, user_id: str, state: "dict[str, Any]", owner_id: "Any | None" = None
    ) -> StoredSession:
        """Create a new session.

        Args:
            session_id: Unique session identifier.
            app_name: Application name.
            user_id: User identifier.
            state: Initial session state.
            owner_id: Optional owner ID value for owner_id_column (can be None for nullable columns).

        Returns:
            Created session record.
        """
        state_json = self._serialize_state(state)
        state_placeholder = self._json_placeholder()
        now_val = self._encode_timestamp(datetime.now(UTC))

        params: tuple[Any, ...]
        if self._owner_id_column_name:
            sql = f"""
            INSERT INTO {self._session_table}
            (id, app_name, user_id, {self._owner_id_column_name}, state, create_time, update_time)
            VALUES (?, ?, ?, ?, {state_placeholder}, ?, ?)
            """
            params = (session_id, app_name, user_id, owner_id, state_json, now_val, now_val)
        else:
            sql = f"""
            INSERT INTO {self._session_table} (id, app_name, user_id, state, create_time, update_time)
            VALUES (?, ?, ?, {state_placeholder}, ?, ?)
            """
            params = (session_id, app_name, user_id, state_json, now_val, now_val)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, params)
                conn.commit()
            finally:
                cursor.close()

        result = self._get_session(app_name, user_id, session_id)
        if result is None:
            msg = "Failed to fetch created session"
            raise RuntimeError(msg)
        return result

    def _get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: "int | timedelta | None" = None
    ) -> "StoredSession | None":
        """Get session by ID.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            renew_for: If positive, touch the session update timestamp.

        Returns:
            Session record or None if not found.
        """
        if renew_for is not None and self._calculate_expires_at(renew_for) is not None:
            now_val = self._encode_timestamp(datetime.now(UTC))
            sql = f"""
            UPDATE {self._session_table}
            SET update_time = ?
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            params: tuple[Any, ...] = (now_val, app_name, user_id, session_id)
            select_after_update = True
        else:
            sql = f"""
            SELECT id, app_name, user_id, state, create_time, update_time
            FROM {self._session_table}
            WHERE app_name = ? AND user_id = ? AND id = ?
            """
            params = (app_name, user_id, session_id)
            select_after_update = False

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    if select_after_update:
                        conn.commit()
                        self._execute(
                            cursor,
                            f"""
                            SELECT id, app_name, user_id, state, create_time, update_time
                            FROM {self._session_table}
                            WHERE app_name = ? AND user_id = ? AND id = ?
                            """,
                            (app_name, user_id, session_id),
                        )
                    row = cursor.fetchone()

                    if row is None:
                        return None

                    return StoredSession(
                        id=row[0],
                        app_name=row[1],
                        user_id=row[2],
                        state=self._deserialize_state(row[3]),
                        create_time=self._decode_timestamp(row[4]),
                        update_time=self._decode_timestamp(row[5]),
                    )
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _update_session_state(self, app_name: str, user_id: str, session_id: str, state: "dict[str, Any]") -> None:
        """Update session state.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            state: New state dictionary (replaces existing state).
        """
        state_json = self._serialize_state(state)
        now_val = self._encode_timestamp(datetime.now(UTC))
        sql = f"""
        UPDATE {self._session_table}
        SET state = {self._json_placeholder()}, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, (state_json, now_val, app_name, user_id, session_id))
                conn.commit()
            finally:
                cursor.close()

    def _delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete session and all associated events (cascade).

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
        """
        delete_events_sql = f"DELETE FROM {self._events_table} WHERE session_id = ?"
        delete_session_sql = f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._enable_foreign_keys(cursor, conn)
                self._execute(cursor, delete_events_sql, (session_id,))
                if self._dialect == DIALECT_DUCKDB:
                    conn.commit()
                self._execute(cursor, delete_session_sql, (app_name, user_id, session_id))
                conn.commit()
            finally:
                cursor.close()

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
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: Application name.
            user_id: User identifier. If None, lists all sessions for the app.
            order_by: Timestamp column to sort on.
            descending: Sort direction for the timestamp column and the id tie-break.
            limit: Maximum number of sessions to return.
            offset: Number of leading rows to skip.

        Returns:
            List of session records.
        """
        column, direction, page_limit, page_offset = normalize_session_list_options(order_by, descending, limit, offset)
        if page_limit == 0:
            return []

        sql, params = self._session_list_query(app_name, user_id, column, direction, page_limit, page_offset)

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    rows = cursor.fetchall()

                    return [
                        StoredSession(
                            id=row[0],
                            app_name=row[1],
                            user_id=row[2],
                            state=self._deserialize_state(row[3]),
                            create_time=self._decode_timestamp(row[4]),
                            update_time=self._decode_timestamp(row[5]),
                        )
                        for row in rows
                    ]
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

    def _insert_event(self, event_record: "StoredEvent") -> None:
        """Insert an event record into the events table.

        Args:
            event_record: Event record to store.
        """
        event_data = self._serialize_json_field(event_record["event_data"])
        sql = f"""
        INSERT INTO {self._events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?, ?, {self._json_placeholder()})
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(
                    cursor,
                    sql,
                    (
                        event_record["id"],
                        event_record["app_name"],
                        event_record["user_id"],
                        event_record["session_id"],
                        event_record["invocation_id"],
                        self._encode_timestamp(event_record["timestamp"]),
                        event_data,
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

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
        """Atomically insert an event and update the session's durable state.

        The event insert, state update, and refresh-SELECT are executed within
        a single connection and committed together. ADBC drivers wrap a
        variety of backends (Postgres, SQLite, DuckDB, ...) so we use a
        SELECT-after-UPDATE rather than relying on RETURNING which not every
        backend supports.

        Args:
            event_record: Event record to store.
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier whose state should be updated.
            state: Post-append durable state snapshot (``temp:`` keys already
                stripped by the service layer).
            app_state: Optional app-scoped state snapshot.
            user_state: Optional user-scoped state snapshot.
        """
        insert_sql = f"""
        INSERT INTO {self._events_table} (
            id, app_name, user_id, session_id, invocation_id, timestamp, event_data
        ) VALUES (?, ?, ?, ?, ?, ?, {self._json_placeholder()})
        """
        update_sql = f"""
        UPDATE {self._session_table}
        SET state = {self._json_placeholder()}, update_time = ?
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        select_sql = f"""
        SELECT id, app_name, user_id, state, create_time, update_time
        FROM {self._session_table}
        WHERE app_name = ? AND user_id = ? AND id = ?
        """
        delete_app_state_sql = f"DELETE FROM {self._app_state_table} WHERE app_name = ?"
        insert_app_state_sql = f"""
        INSERT INTO {self._app_state_table} (app_name, state, update_time)
        VALUES (?, {self._json_placeholder()}, ?)
        """
        delete_user_state_sql = f"DELETE FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        insert_user_state_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, {self._json_placeholder()}, ?)
        """
        state_json = self._serialize_state(state)
        event_data = self._serialize_json_field(event_record["event_data"])
        now_val = self._encode_timestamp(datetime.now(UTC))

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, update_sql, (state_json, now_val, app_name, user_id, session_id))
                self._execute(cursor, select_sql, (app_name, user_id, session_id))
                row = cursor.fetchone()
                if row is not None:
                    self._execute(
                        cursor,
                        insert_sql,
                        (
                            event_record["id"],
                            event_record["app_name"],
                            event_record["user_id"],
                            event_record["session_id"],
                            event_record["invocation_id"],
                            self._encode_timestamp(event_record["timestamp"]),
                            event_data,
                        ),
                    )
                    if app_state is not None:
                        self._execute(cursor, delete_app_state_sql, (app_name,))
                        self._execute(
                            cursor, insert_app_state_sql, (app_name, self._serialize_state(app_state), now_val)
                        )
                    if user_state is not None:
                        self._execute(cursor, delete_user_state_sql, (app_name, user_id))
                        self._execute(
                            cursor,
                            insert_user_state_sql,
                            (app_name, user_id, self._serialize_state(user_state), now_val),
                        )
                    conn.commit()
                else:
                    with contextlib.suppress(Exception):
                        conn.rollback()
            except Exception:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise
            finally:
                cursor.close()

        if row is None:
            msg = f"Session {session_id} not found during append_event_and_update_state."
            raise ValueError(msg)

        return StoredSession(
            id=row[0],
            app_name=row[1],
            user_id=row[2],
            state=self._deserialize_state(row[3]),
            create_time=self._decode_timestamp(row[4]),
            update_time=self._decode_timestamp(row[5]),
        )

    def _get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: "datetime | None" = None,
        limit: "int | None" = None,
    ) -> "list[StoredEvent]":
        """List events for a session ordered by timestamp.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.
            after_timestamp: Only return events after this time.
            limit: Maximum number of events to return.

        Returns:
            List of event records ordered by timestamp ASC.
        """
        if limit == 0:
            return []

        where_clauses = ["s.app_name = ?", "s.user_id = ?", "e.session_id = ?"]
        params: list[Any] = [app_name, user_id, session_id]

        if after_timestamp is not None:
            where_clauses.append("e.timestamp > ?")
            params.append(self._encode_timestamp(after_timestamp))

        where_clause = " AND ".join(where_clauses)
        limit_clause = ""
        if limit is not None:
            if self._dialect in {DIALECT_GENERIC, DIALECT_ORACLE, DIALECT_DB2, DIALECT_MSSQL}:
                limit_clause = " OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
            else:
                limit_clause = " LIMIT ?"
            params.append(limit)
        sql = f"""
        SELECT e.id, e.session_id, e.invocation_id, e.timestamp, e.event_data, s.app_name, s.user_id
        FROM {self._events_table} e
        JOIN {self._session_table} s ON e.session_id = s.id
        WHERE {where_clause}
        ORDER BY e.timestamp ASC{limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, params)
                    rows = cursor.fetchall()

                    return [
                        StoredEvent(
                            id=row[0],
                            session_id=row[1],
                            invocation_id=row[2] or "",
                            timestamp=self._decode_timestamp(row[3]),
                            event_data=self._deserialize_json_field(row[4]) or {},
                            app_name=row[5],
                            user_id=row[6],
                        )
                        for row in rows
                    ]
                finally:
                    cursor.close()
        except Exception as e:
            error_msg = str(e).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

    def _delete_expired_events(self, before: datetime, app_name: "str | None" = None) -> int:
        encoded_before = self._encode_timestamp(before)
        where = "WHERE timestamp < ?"
        params: tuple[Any, ...] = (encoded_before,)
        if app_name is not None:
            where += " AND app_name = ?"
            params = (encoded_before, app_name)
        count_sql = f"SELECT COUNT(*) FROM {self._events_table} {where}"
        delete_sql = f"DELETE FROM {self._events_table} {where}"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, count_sql, params)
                    row = cursor.fetchone()
                    count = int(row[0]) if row is not None else 0
                    self._execute(cursor, delete_sql, params)
                    conn.commit()
                    return count
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return 0
            raise

    def _delete_idle_sessions(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        encoded_updated_before = self._encode_timestamp(updated_before)
        where = "WHERE update_time < ?"
        params: tuple[Any, ...] = (encoded_updated_before,)
        if app_name is not None:
            where += " AND app_name = ?"
            params = (encoded_updated_before, app_name)
        count_sql = f"SELECT COUNT(*) FROM {self._session_table} {where}"
        delete_events_sql = f"""
        DELETE FROM {self._events_table}
        WHERE session_id IN (SELECT id FROM {self._session_table} {where})
        """
        delete_sessions_sql = f"DELETE FROM {self._session_table} {where}"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, count_sql, params)
                    row = cursor.fetchone()
                    count = int(row[0]) if row is not None else 0
                    self._execute(cursor, delete_events_sql, params)
                    self._execute(cursor, delete_sessions_sql, params)
                    conn.commit()
                    return count
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return 0
            raise

    def _delete_idle_user_states(self, updated_before: datetime, app_name: "str | None" = None) -> int:
        encoded_updated_before = self._encode_timestamp(updated_before)
        where = "WHERE update_time < ?"
        params: tuple[Any, ...] = (encoded_updated_before,)
        if app_name is not None:
            where += " AND app_name = ?"
            params = (encoded_updated_before, app_name)
        count_sql = f"SELECT COUNT(*) FROM {self._user_state_table} {where}"
        delete_sql = f"DELETE FROM {self._user_state_table} {where}"

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, count_sql, params)
                    row = cursor.fetchone()
                    count = int(row[0]) if row is not None else 0
                    self._execute(cursor, delete_sql, params)
                    conn.commit()
                    return count
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return 0
            raise

    def _get_app_state(self, app_name: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._app_state_table} WHERE app_name = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (app_name,))
                    row = cursor.fetchone()
                    return self._deserialize_state(row[0]) if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _get_user_state(self, app_name: str, user_id: str) -> "dict[str, Any] | None":
        sql = f"SELECT state FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (app_name, user_id))
                    row = cursor.fetchone()
                    return self._deserialize_state(row[0]) if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _upsert_app_state(self, app_name: str, state: "dict[str, Any]") -> None:
        delete_sql = f"DELETE FROM {self._app_state_table} WHERE app_name = ?"
        insert_sql = f"INSERT INTO {self._app_state_table} (app_name, state, update_time) VALUES (?, {self._json_placeholder()}, ?)"
        now_val = self._encode_timestamp(datetime.now(UTC))
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (app_name,))
                self._execute(cursor, insert_sql, (app_name, self._serialize_state(state), now_val))
                conn.commit()
            finally:
                cursor.close()

    def _upsert_user_state(self, app_name: str, user_id: str, state: "dict[str, Any]") -> None:
        delete_sql = f"DELETE FROM {self._user_state_table} WHERE app_name = ? AND user_id = ?"
        insert_sql = f"""
        INSERT INTO {self._user_state_table} (app_name, user_id, state, update_time)
        VALUES (?, ?, {self._json_placeholder()}, ?)
        """
        now_val = self._encode_timestamp(datetime.now(UTC))
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (app_name, user_id))
                self._execute(cursor, insert_sql, (app_name, user_id, self._serialize_state(state), now_val))
                conn.commit()
            finally:
                cursor.close()

    def _get_metadata(self, key: str) -> "str | None":
        key_col = _metadata_key_column(self._dialect)
        sql = f"SELECT value FROM {self._metadata_table} WHERE {key_col} = ?"
        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (key,))
                    row = cursor.fetchone()
                    return row[0] if row is not None else None
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return None
            raise

    def _set_metadata(self, key: str, value: str) -> None:
        key_col = _metadata_key_column(self._dialect)
        delete_sql = f"DELETE FROM {self._metadata_table} WHERE {key_col} = ?"
        insert_sql = f"INSERT INTO {self._metadata_table} ({key_col}, value) VALUES (?, ?)"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, delete_sql, (key,))
                self._execute(cursor, insert_sql, (key, value))
                conn.commit()
            finally:
                cursor.close()

    def _append_event(self, event_record: StoredEvent) -> None:
        """Synchronous implementation of append_event."""
        self._insert_event(event_record)


class AdbcADKMemoryStore(BaseSyncADKMemoryStore["AdbcConfig"]):
    """ADBC synchronous ADK memory store for Arrow Database Connectivity."""

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._dialect = self._detect_dialect()
        if self._vector_dimensions <= 0:
            msg = "vector_dimensions must be a positive integer"
            raise ImproperConfigurationError(msg)

    @property
    def dialect(self) -> str:
        return self._dialect

    def create_tables(self) -> None:
        """Create tables if they don't exist."""
        if not self._enabled:
            return

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

    def _pgvector_enabled(self) -> bool:
        if self._dialect != DIALECT_POSTGRESQL:
            return False
        pgvector_available = getattr(self._config, "_pgvector_available", None)
        return pgvector_available is not False

    def _ensure_postgres_vector_supported(self) -> None:
        if self._dialect != DIALECT_POSTGRESQL:
            msg = f"Vector embeddings in AdbcADKMemoryStore are only supported on PostgreSQL (current dialect: {self._dialect})."
            raise ImproperConfigurationError(msg)
        if getattr(self._config, "_pgvector_available", None) is False:
            msg = "PostgreSQL extension 'vector' (pgvector) is required for vector embeddings in AdbcADKMemoryStore."
            raise ImproperConfigurationError(msg)
        if self._vector_dimensions <= 0:
            msg = "vector_dimensions must be a positive integer."
            raise ImproperConfigurationError(msg)

    def _detect_dialect(self) -> str:
        driver_name = self._config.connection_config.get("driver_name", "")
        if (
            isinstance(driver_name, str)
            and driver_name
            and driver_kind_from_driver_name(driver_name) is None
            and not is_shared_object_driver(driver_name)
        ):
            logger.warning("Unknown ADBC driver: %s. Using generic SQL dialect.", driver_name)
            return DIALECT_GENERIC

        resolved = resolve_dialect_from_config(self._config.connection_config)
        stmt_dialect = self._config.statement_config.dialect
        if (isinstance(stmt_dialect, str) and stmt_dialect.lower() in {"oracle", "db2", "mssql", "tsql"}) or (
            resolved is None
            and isinstance(stmt_dialect, str)
            and stmt_dialect.lower() in {"sqlite", "duckdb", "bigquery", "snowflake"}
        ):
            resolved = stmt_dialect.lower()
        if is_postgres_dialect(resolved):
            return DIALECT_POSTGRESQL
        if resolved == "sqlite":
            return DIALECT_SQLITE
        if resolved == "duckdb":
            return DIALECT_DUCKDB
        if resolved == "bigquery":
            return DIALECT_BIGQUERY
        if resolved == "snowflake":
            return DIALECT_SNOWFLAKE
        if resolved == "oracle":
            return DIALECT_ORACLE
        if resolved == "db2":
            return DIALECT_DB2
        if resolved in {"mssql", "tsql"}:
            return DIALECT_MSSQL
        logger.warning("Unknown ADBC driver: %s. Using generic SQL dialect.", driver_name)
        return DIALECT_GENERIC

    def _format_sql(self, sql: str) -> str:
        """Return SQL with dialect-appropriate positional placeholders."""
        if self._dialect == DIALECT_POSTGRESQL:
            return _postgresql_store_sql(sql)
        if self._dialect == DIALECT_ORACLE:
            return _oracle_store_sql(sql)
        return sql

    def _execute(self, cursor: Any, sql: str, params: "tuple[Any, ...] | list[Any]") -> Any:
        """Execute parameterized SQL using the current ADBC dialect's placeholder style."""
        return cursor.execute(self._format_sql(sql), params)

    def _json_placeholder(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return "?::jsonb"
        return "?"

    def _serialize_json_field(self, value: Any) -> "str | None":
        if value is None:
            return None
        return to_json(value)

    def _encode_timestamp(self, value: datetime) -> Any:
        if self._dialect == DIALECT_SQLITE:
            return value.timestamp()
        return value

    def _decode_timestamp(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    def _memory_table_ddl(self) -> str:
        if self._dialect == DIALECT_POSTGRESQL:
            return self._memory_ddl_postgresql()
        if self._dialect == DIALECT_SQLITE:
            return self._memory_ddl_sqlite()
        if self._dialect == DIALECT_DUCKDB:
            return self._memory_ddl_duckdb()
        if self._dialect == DIALECT_BIGQUERY:
            return self._memory_ddl_bigquery()
        if self._dialect == DIALECT_SNOWFLAKE:
            return self._memory_ddl_snowflake()
        if self._dialect == DIALECT_ORACLE:
            return self._memory_ddl_oracle()
        if self._dialect == DIALECT_DB2:
            return self._memory_ddl_db2()
        if self._dialect == DIALECT_MSSQL:
            return self._memory_ddl_mssql()
        return self._memory_ddl_generic()

    def _memory_ddl_postgresql(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        embedding_line = (
            f"\n            embedding VECTOR({self._vector_dimensions})," if self._pgvector_enabled() else ""
        )
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            scope VARCHAR(16) NOT NULL DEFAULT 'user',
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMPTZ NOT NULL,{embedding_line}
            content_json JSONB NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSONB,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _memory_ddl_sqlite(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            app_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'user',
            event_id TEXT NOT NULL UNIQUE,
            author TEXT{owner_id_ddl},
            timestamp REAL NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at REAL NOT NULL
        )
        """

    def _memory_ddl_duckdb(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            scope VARCHAR(16) NOT NULL DEFAULT 'user',
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _memory_ddl_bigquery(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id STRING NOT NULL,
            session_id STRING NOT NULL,
            app_name STRING NOT NULL,
            user_id STRING NOT NULL,
            scope STRING NOT NULL DEFAULT 'user',
            event_id STRING NOT NULL,
            author STRING{owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json JSON NOT NULL,
            content_text STRING NOT NULL,
            metadata_json JSON,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _memory_ddl_snowflake(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            scope VARCHAR NOT NULL DEFAULT 'user',
            event_id VARCHAR NOT NULL UNIQUE,
            author VARCHAR{owner_id_ddl},
            timestamp TIMESTAMP_TZ NOT NULL,
            content_json VARIANT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json VARIANT,
            inserted_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """

    def _memory_ddl_oracle(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._memory_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                scope VARCHAR2(16) DEFAULT ''user'' NOT NULL,
                event_id VARCHAR2(128) NOT NULL UNIQUE,
                author VARCHAR2(256){owner_id_ddl},
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                content_json CLOB NOT NULL,
                content_text CLOB NOT NULL,
                metadata_json CLOB,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _memory_ddl_db2(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE {self._memory_table} (
            id VARCHAR(128) NOT NULL PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            scope VARCHAR(16) NOT NULL WITH DEFAULT 'user',
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json CLOB(1M) NOT NULL,
            content_text CLOB(1M) NOT NULL,
            metadata_json CLOB(1M),
            inserted_at TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP
        )
        """

    def _memory_ddl_mssql(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        IF OBJECT_ID(N'{self._memory_table}', N'U') IS NULL
        CREATE TABLE {self._memory_table} (
            id NVARCHAR(128) NOT NULL PRIMARY KEY,
            session_id NVARCHAR(128) NOT NULL,
            app_name NVARCHAR(128) NOT NULL,
            user_id NVARCHAR(128) NOT NULL,
            scope NVARCHAR(16) NOT NULL DEFAULT 'user',
            event_id NVARCHAR(128) NOT NULL UNIQUE,
            author NVARCHAR(256){owner_id_ddl},
            timestamp DATETIME2 NOT NULL,
            content_json NVARCHAR(MAX) NOT NULL,
            content_text NVARCHAR(MAX) NOT NULL,
            metadata_json NVARCHAR(MAX),
            inserted_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )
        """

    def _memory_ddl_generic(self) -> str:
        owner_id_ddl = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return f"""
        CREATE TABLE IF NOT EXISTS {self._memory_table} (
            id VARCHAR(128) PRIMARY KEY,
            session_id VARCHAR(128) NOT NULL,
            app_name VARCHAR(128) NOT NULL,
            user_id VARCHAR(128) NOT NULL,
            scope VARCHAR(16) NOT NULL DEFAULT 'user',
            event_id VARCHAR(128) NOT NULL UNIQUE,
            author VARCHAR(256){owner_id_ddl},
            timestamp TIMESTAMP NOT NULL,
            content_json TEXT NOT NULL,
            content_text TEXT NOT NULL,
            metadata_json TEXT,
            inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """

    def _drop_memory_table_sql(self) -> "list[str]":
        return [_drop_table_sql_for_dialect(self._dialect, self._memory_table)]

    def _create_tables(self) -> None:
        if not self._enabled:
            return

        if self._dialect == DIALECT_POSTGRESQL:
            if getattr(self._config, "_pgvector_available", None) is None:
                detect_fn = getattr(self._config, "_detect_extensions_if_needed", None)
                if callable(detect_fn):
                    with contextlib.suppress(Exception):
                        detect_fn()
            if self._enable_bm25 and getattr(self._config, "_pg_textsearch_available", None) is False:
                ensure_bm25 = getattr(self._config, "_ensure_pg_textsearch_available", None)
                if callable(ensure_bm25):
                    ensure_bm25()

        skip_indexes = self._dialect in {DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                _execute_table_ddl(cursor, self._dialect, self._memory_table, self._memory_table_ddl())
                conn.commit()

                if not skip_indexes:
                    _execute_index_ddl(
                        cursor,
                        self._dialect,
                        f"idx_{self._memory_table}_app_scope_user_time",
                        self._memory_table,
                        "app_name, scope, user_id, timestamp DESC",
                    )
                    conn.commit()

                    _execute_index_ddl(
                        cursor, self._dialect, f"idx_{self._memory_table}_scope", self._memory_table, "app_name, scope"
                    )
                    conn.commit()

                    _execute_index_ddl(
                        cursor, self._dialect, f"idx_{self._memory_table}_session", self._memory_table, "session_id"
                    )
                    conn.commit()

                    if self._dialect == DIALECT_POSTGRESQL:
                        for index_sql in self._postgres_memory_extra_index_sql():
                            cursor.execute(index_sql)
                            conn.commit()
            finally:
                cursor.close()

    def _postgres_memory_extra_index_sql(self) -> "list[str]":
        indexes: list[str] = []
        if self._use_fts:
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_fts ON {self._memory_table} USING GIN (to_tsvector('english', content_text))"
            )
        if self._enable_bm25:
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_bm25 ON {self._memory_table} USING bm25 (content_text) WITH (text_config='english')"
            )
        if not self._pgvector_enabled():
            return indexes

        if self._vector_index_type == "scann":
            if (
                not isinstance(self._scann_num_leaves, int)
                or isinstance(self._scann_num_leaves, bool)
                or self._scann_num_leaves <= 0
            ):
                msg = "extension_config['adk']['scann_num_leaves'] must be a positive integer"
                raise ValueError(msg)
            if self._scann_quantizer not in {"SQ8", "FP32"}:
                msg = "extension_config['adk']['scann_quantizer'] must be 'SQ8' or 'FP32'"
                raise ValueError(msg)
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_scann ON {self._memory_table} USING scann (embedding) WITH (num_leaves = {self._scann_num_leaves}, quantizer = '{self._scann_quantizer}')"
            )
        elif self._vector_index_type == "ivfflat":
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_ivfflat ON {self._memory_table} USING ivfflat (embedding vector_cosine_ops)"
            )
        elif self._vector_index_type == "hnsw":
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS idx_{self._memory_table}_hnsw ON {self._memory_table} USING hnsw (embedding vector_cosine_ops)"
            )
        return indexes

    def _insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        has_non_null_embedding = any(entry.get("embedding") is not None for entry in entries)
        if has_non_null_embedding:
            self._ensure_postgres_vector_supported()

        include_vector = self._pgvector_enabled()
        inserted_count = 0
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        json_placeholder = self._json_placeholder()

        if include_vector:
            if self._owner_id_column_name:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    {self._owner_id_column_name}, timestamp, embedding, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::text::vector, {json_placeholder}, ?, {json_placeholder}, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
            else:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    timestamp, embedding, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?::text::vector, {json_placeholder}, ?, {json_placeholder}, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
        elif self._owner_id_column_name:
            if use_returning:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, {json_placeholder}, ?, {json_placeholder}, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
            else:
                sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    {self._owner_id_column_name}, timestamp, content_json, content_text,
                    metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, {json_placeholder}, ?, {json_placeholder}, ?
                )
                """
        elif use_returning:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, {json_placeholder}, ?, {json_placeholder}, ?
                ) ON CONFLICT(event_id) DO NOTHING RETURNING 1
                """
        else:
            sql = f"""
                INSERT INTO {self._memory_table} (
                    id, session_id, app_name, user_id, scope, event_id, author,
                    timestamp, content_json, content_text, metadata_json, inserted_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, {json_placeholder}, ?, {json_placeholder}, ?
                )
                """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                entries_to_insert = entries
                if not use_returning:
                    event_ids = [entry["event_id"] for entry in entries if "event_id" in entry]
                    if event_ids:
                        placeholders = ", ".join("?" for _ in event_ids)
                        select_sql = f"SELECT event_id FROM {self._memory_table} WHERE event_id IN ({placeholders})"
                        self._execute(cursor, select_sql, tuple(event_ids))
                        existing_rows = cursor.fetchall()
                        existing_event_ids = {row[0] for row in existing_rows}
                        entries_to_insert = []
                        for entry in entries:
                            event_id = entry["event_id"]
                            if event_id not in existing_event_ids:
                                entries_to_insert.append(entry)
                                existing_event_ids.add(event_id)

                for entry in entries_to_insert:
                    content_json = self._serialize_json_field(entry["content_json"])
                    metadata_json = self._serialize_json_field(entry.get("metadata_json"))
                    raw_emb = entry.get("embedding")
                    embedding_literal = format_pgvector_literal(raw_emb) if raw_emb is not None else None
                    params: tuple[Any, ...]
                    if include_vector:
                        if self._owner_id_column_name:
                            params = (
                                entry["id"],
                                entry["session_id"],
                                entry["app_name"],
                                entry["user_id"],
                                entry.get("scope", "user"),
                                entry["event_id"],
                                entry.get("author"),
                                owner_id,
                                self._encode_timestamp(entry["timestamp"]),
                                embedding_literal,
                                content_json,
                                entry["content_text"],
                                metadata_json,
                                self._encode_timestamp(entry["inserted_at"]),
                            )
                        else:
                            params = (
                                entry["id"],
                                entry["session_id"],
                                entry["app_name"],
                                entry["user_id"],
                                entry.get("scope", "user"),
                                entry["event_id"],
                                entry.get("author"),
                                self._encode_timestamp(entry["timestamp"]),
                                embedding_literal,
                                content_json,
                                entry["content_text"],
                                metadata_json,
                                self._encode_timestamp(entry["inserted_at"]),
                            )
                    elif self._owner_id_column_name:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry.get("scope", "user"),
                            entry["event_id"],
                            entry.get("author"),
                            owner_id,
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    else:
                        params = (
                            entry["id"],
                            entry["session_id"],
                            entry["app_name"],
                            entry["user_id"],
                            entry.get("scope", "user"),
                            entry["event_id"],
                            entry.get("author"),
                            self._encode_timestamp(entry["timestamp"]),
                            content_json,
                            entry["content_text"],
                            metadata_json,
                            self._encode_timestamp(entry["inserted_at"]),
                        )
                    if use_returning:
                        self._execute(cursor, sql, params)
                        if cursor.fetchone():
                            inserted_count += 1
                    else:
                        try:
                            self._execute(cursor, sql, params)
                        except Exception as exc:
                            error_message = str(exc).lower()
                            if not (
                                getattr(exc, "sqlstate", None) == "23505"
                                or "unique" in error_message
                                or "duplicate" in error_message
                                or "ora-00001" in error_message
                                or "sql0803n" in error_message
                            ):
                                raise
                            self._execute(
                                cursor,
                                f"SELECT event_id FROM {self._memory_table} WHERE event_id = ?",
                                (entry["event_id"],),
                            )
                            if cursor.fetchone() is None:
                                raise
                            continue
                        inserted_count += 1
                conn.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    conn.rollback()
                raise
            finally:
                cursor.close()

        return inserted_count

    def _search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        *,
        embedding: "Sequence[float] | None" = None,
    ) -> "list[StoredMemory]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        has_query = bool(query and query.strip())
        if not has_query and embedding is None:
            return []

        effective_limit = limit if limit is not None else self._max_results

        if embedding is not None:
            self._ensure_postgres_vector_supported()
            try:
                if self._enable_bm25 and has_query:
                    ensure_bm25 = getattr(self._config, "_ensure_pg_textsearch_available", None)
                    if callable(ensure_bm25) and getattr(self._config, "_pg_textsearch_available", None) is False:
                        ensure_bm25()
                    return self._search_entries_hybrid(
                        query, app_name, user_id, effective_limit, scope_filter, embedding
                    )
                return self._search_entries_vector(app_name, user_id, effective_limit, scope_filter, embedding)
            except Exception as exc:
                error_msg = str(exc).lower()
                if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                    return []
                raise

        if self._use_fts and self._dialect != DIALECT_POSTGRESQL:
            logger.warning("ADBC memory store does not support FTS, falling back to simple search")

        where_scope, scope_params = _build_adbc_scope_where(app_name, user_id, scope_filter)
        if self._dialect == DIALECT_POSTGRESQL and self._use_fts:
            match_clause = "to_tsvector('english', content_text) @@ plainto_tsquery('english', ?)"
            search_param = query
        elif self._dialect == DIALECT_DB2:
            match_clause = "POSSTR(LOWER(content_text), LOWER(?)) > 0"
            search_param = query
        else:
            match_clause = "content_text LIKE ?"
            search_param = f"%{query}%"

        if self._dialect in {DIALECT_GENERIC, DIALECT_ORACLE, DIALECT_DB2, DIALECT_MSSQL}:
            limit_clause = "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
        else:
            limit_clause = "LIMIT ?"

        embedding_projection = ", embedding::text AS embedding" if self._pgvector_enabled() else ""
        sql = f"""
        SELECT id, session_id, app_name, user_id, scope, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at{embedding_projection}
        FROM {self._memory_table}
        WHERE {where_scope}
          AND {match_clause}
        ORDER BY timestamp DESC
        {limit_clause}
        """

        try:
            with self._config.provide_connection() as conn:
                cursor = conn.cursor()
                try:
                    self._execute(cursor, sql, (*scope_params, search_param, effective_limit))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
        except Exception as exc:
            error_msg = str(exc).lower()
            if any(pattern in error_msg for pattern in ADBC_TABLE_NOT_FOUND_PATTERNS):
                return []
            raise

        return self._rows_to_records(rows)

    def _search_entries_vector(
        self,
        app_name: str,
        user_id: str,
        limit: int,
        scope_filter: Literal["all", "user", "app"],
        embedding: "Sequence[float]",
    ) -> "list[StoredMemory]":
        where_scope, scope_params = _build_adbc_scope_where(app_name, user_id, scope_filter)
        vector_literal = format_pgvector_literal(embedding)
        sql = f"""
        SELECT id, session_id, app_name, user_id, scope, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at,
               embedding::text AS embedding
        FROM {self._memory_table}
        WHERE {where_scope} AND embedding IS NOT NULL
        ORDER BY embedding <=> ?::text::vector ASC, timestamp DESC
        LIMIT ?
        """
        params = (*scope_params, vector_literal, limit)
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return self._rows_to_records(rows)

    def _search_entries_hybrid(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int,
        scope_filter: Literal["all", "user", "app"],
        embedding: "Sequence[float]",
    ) -> "list[StoredMemory]":
        where_scope, scope_params = _build_adbc_scope_where(app_name, user_id, scope_filter)
        vector_literal = format_pgvector_literal(embedding)
        candidate_limit = max(limit * 2, 50)
        sql = f"""
        WITH vector_matches AS (
            SELECT id, RANK() OVER (ORDER BY embedding <=> ?::text::vector) AS rank_vec
            FROM {self._memory_table}
            WHERE {where_scope} AND embedding IS NOT NULL
            LIMIT ?
        ),
        text_matches AS (
            SELECT id, RANK() OVER (ORDER BY content_text <@> ?) AS rank_txt
            FROM {self._memory_table}
            WHERE {where_scope}
            LIMIT ?
        )
        SELECT m.id, m.session_id, m.app_name, m.user_id, m.scope, m.event_id, m.author,
               m.timestamp, m.content_json, m.content_text, m.metadata_json, m.inserted_at,
               m.embedding::text AS embedding,
               (COALESCE(1.0 / (60 + v.rank_vec), 0.0) + COALESCE(1.0 / (60 + t.rank_txt), 0.0)) AS rrf_score
        FROM {self._memory_table} m
        LEFT JOIN vector_matches v ON m.id = v.id
        LEFT JOIN text_matches t ON m.id = t.id
        WHERE v.id IS NOT NULL OR t.id IS NOT NULL
        ORDER BY rrf_score DESC, m.timestamp DESC
        LIMIT ?
        """
        params = (vector_literal, *scope_params, candidate_limit, query, *scope_params, candidate_limit, limit)
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()
        return self._rows_to_records(rows)

    def _delete_entries_by_session(self, session_id: str) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ? RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE session_id = ?"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, (session_id,))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    def _delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        cutoff = self._encode_timestamp(datetime.now(UTC) - timedelta(days=days))
        use_returning = self._dialect in {DIALECT_SQLITE, DIALECT_POSTGRESQL, DIALECT_DUCKDB}
        clauses = ["inserted_at < ?"]
        params: list[Any] = [cutoff]
        if app_name is not None:
            clauses.append("app_name = ?")
            params.append(app_name)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        where_sql = " AND ".join(clauses)
        if use_returning:
            sql = f"DELETE FROM {self._memory_table} WHERE {where_sql} RETURNING 1"
        else:
            sql = f"DELETE FROM {self._memory_table} WHERE {where_sql}"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            try:
                self._execute(cursor, sql, tuple(params))
                if use_returning:
                    deleted_rows = cursor.fetchall()
                    conn.commit()
                    return len(deleted_rows)
                conn.commit()
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            finally:
                cursor.close()

    def _rows_to_records(self, rows: "list[Any]") -> "list[StoredMemory]":
        records: list[StoredMemory] = []
        for row in rows:
            if isinstance(row, Mapping):
                content_json = row["content_json"]
                metadata_json = row.get("metadata_json")
                raw_embedding = row.get("embedding")
                row_id = row["id"]
                session_id = row["session_id"]
                app_name = row["app_name"]
                user_id = row["user_id"]
                scope = row.get("scope", "user")
                event_id = row["event_id"]
                author = row.get("author")
                timestamp_raw = row["timestamp"]
                content_text = row["content_text"]
                inserted_at_raw = row["inserted_at"]
            else:
                content_json = row[8]
                metadata_json = row[10]
                raw_embedding = row[_ADBC_EMBEDDING_COLUMN_INDEX] if len(row) > _ADBC_EMBEDDING_COLUMN_INDEX else None
                row_id = row[0]
                session_id = row[1]
                app_name = row[2]
                user_id = row[3]
                scope = row[4]
                event_id = row[5]
                author = row[6]
                timestamp_raw = row[7]
                content_text = row[9]
                inserted_at_raw = row[11]

            if isinstance(content_json, dict):
                content_value = content_json
            else:
                content_value = from_json(content_json if isinstance(content_json, (str, bytes)) else str(content_json))

            if metadata_json is None:
                metadata_value = None
            elif isinstance(metadata_json, dict):
                metadata_value = metadata_json
            else:
                metadata_value = from_json(
                    metadata_json if isinstance(metadata_json, (str, bytes)) else str(metadata_json)
                )

            records.append({
                "id": row_id,
                "session_id": session_id,
                "app_name": app_name,
                "user_id": user_id,
                "scope": scope,
                "event_id": event_id,
                "author": author,
                "timestamp": self._decode_timestamp(timestamp_raw),
                "content_json": content_value,
                "content_text": content_text,
                "metadata_json": metadata_value,
                "inserted_at": self._decode_timestamp(inserted_at_raw),
                "embedding": decode_adbc_pgvector(raw_embedding),
            })
        return records


def format_pgvector_literal(embedding: "Sequence[float]") -> str:
    """Serialize a sequence of floats into a canonical pgvector literal ``'[v1,v2,...]'``.

    Args:
        embedding: Sequence of numeric values representing a vector embedding.

    Returns:
        Canonical pgvector text literal string.
    """
    return "[" + ",".join(str(float(v)) for v in embedding) + "]"


_ADBC_EMBEDDING_COLUMN_INDEX: Final = 12
_PGVECTOR_BINARY_HEADER_BYTES: Final = 4
_PGVECTOR_FLOAT32_BYTES: Final = 4


def decode_adbc_pgvector(raw_embedding: Any) -> "list[float] | None":
    """Decode a PostgreSQL ``vector`` value returned over ADBC into ``list[float]``.

    Supports text literals (``'[0.1,0.2,...]'``), UTF-8 byte strings, pgvector
    binary wire frames (``>hh{dim}f``), ``PgVector`` objects, and Python sequences.

    Args:
        raw_embedding: Raw column value returned by the ADBC PostgreSQL cursor.

    Returns:
        Decoded list of floats, or ``None`` if input is ``None``.
    """
    if raw_embedding is None:
        return None
    if isinstance(raw_embedding, (bytes, bytearray, memoryview)):
        buf = bytes(raw_embedding)
        if len(buf) >= _PGVECTOR_BINARY_HEADER_BYTES:
            dim, unused = struct.unpack_from(">hh", buf, 0)
            if unused == 0 and dim >= 0 and len(buf) == _PGVECTOR_BINARY_HEADER_BYTES + _PGVECTOR_FLOAT32_BYTES * dim:
                return list(struct.unpack_from(f">{dim}f", buf, _PGVECTOR_BINARY_HEADER_BYTES))
        raw_embedding = buf.decode("utf-8")
    if isinstance(raw_embedding, str):
        stripped = raw_embedding.strip().strip("[]").strip()
        if not stripped:
            return []
        return [float(part) for part in stripped.split(",") if part.strip()]
    to_list_fn = getattr(raw_embedding, "to_list", None)
    if callable(to_list_fn):
        return [float(v) for v in cast("Sequence[Any]", to_list_fn())]
    return [float(v) for v in raw_embedding]


@lru_cache(maxsize=256)
def _postgresql_store_sql(sql: str) -> str:
    """Cache positional SQL conversion without changing quoted or commented text."""
    segments: list[str] = []
    last_end = 0
    index = 0
    for match in PARAMETER_REGEX.finditer(sql):
        if match.lastgroup == "qmark":
            index += 1
            segments.extend((sql[last_end : match.start()], f"${index}"))
            last_end = match.end()
    segments.append(sql[last_end:])
    return "".join(segments)


@lru_cache(maxsize=256)
def _oracle_store_sql(sql: str) -> str:
    """Rewrite positional ``?`` placeholders in store SQL to ``:1, :2, ...``."""
    segments: list[str] = []
    last_end = 0
    index = 0
    for match in PARAMETER_REGEX.finditer(sql):
        if match.lastgroup == "qmark":
            index += 1
            segments.extend((sql[last_end : match.start()], f":{index}"))
            last_end = match.end()
    segments.append(sql[last_end:])
    return "".join(segments)


def _metadata_key_column(dialect: str) -> str:
    if dialect == DIALECT_MSSQL:
        return "[key]"
    if dialect == DIALECT_DB2:
        return '"KEY"'
    return "key"


def _db2_table_exists(cursor: Any, table_name: str) -> bool:
    schema_name, catalog_table = _split_db2_table_name(table_name)
    cursor.execute(_DB2_TABLE_EXISTS_SQL, (schema_name, catalog_table))
    return cursor.fetchone() is not None


def _db2_index_exists(cursor: Any, index_name: str) -> bool:
    schema_name, catalog_index = _split_db2_table_name(index_name)
    cursor.execute(_DB2_INDEX_EXISTS_SQL, (schema_name, catalog_index))
    return cursor.fetchone() is not None


def _execute_table_ddl(cursor: Any, dialect: str, table_name: str, ddl: str) -> None:
    if dialect == DIALECT_DB2:
        if not _db2_table_exists(cursor, table_name):
            cursor.execute(ddl)
        return
    cursor.execute(ddl)


def _execute_index_ddl(cursor: Any, dialect: str, index_name: str, table_name: str, columns: str) -> None:
    if dialect == DIALECT_ORACLE:
        stmt = (
            f"BEGIN EXECUTE IMMEDIATE 'CREATE INDEX {index_name} ON {table_name}({columns})'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN (-955, -1408) THEN RAISE; END IF; END;"
        )
        cursor.execute(stmt)
        return
    if dialect == DIALECT_DB2:
        if not _db2_index_exists(cursor, index_name):
            cursor.execute(f"CREATE INDEX {index_name} ON {table_name}({columns})")
        return
    if dialect == DIALECT_MSSQL:
        stmt = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index_name}' "
            f"AND object_id = OBJECT_ID(N'{table_name}')) "
            f"CREATE INDEX {index_name} ON {table_name}({columns})"
        )
        cursor.execute(stmt)
        return
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns})")


def _drop_table_sql_for_dialect(dialect: str, table_name: str) -> str:
    if dialect == DIALECT_ORACLE:
        return (
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
    if dialect == DIALECT_DB2:
        schema_name, catalog_table = _split_db2_table_name(table_name)
        schema_filter = f"TABSCHEMA = '{schema_name}'" if schema_name is not None else "TABSCHEMA = CURRENT SCHEMA"
        return (
            f"BEGIN IF EXISTS (SELECT 1 FROM SYSCAT.TABLES WHERE {schema_filter} AND TABNAME = '{catalog_table}') "
            f"THEN EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; END IF; END"
        )
    if dialect == DIALECT_MSSQL:
        return f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE {table_name}"
    return f"DROP TABLE IF EXISTS {table_name}"


def _build_adbc_scope_where(
    app_name: str, user_id: str, scope_filter: Literal["all", "user", "app"]
) -> tuple[str, tuple[Any, ...]]:
    if scope_filter == "all":
        return "app_name = ? AND ((scope = 'user' AND user_id = ?) OR scope = 'app')", (app_name, user_id)
    if scope_filter == "user":
        return "app_name = ? AND scope = 'user' AND user_id = ?", (app_name, user_id)
    return "app_name = ? AND scope = 'app'", (app_name,)
