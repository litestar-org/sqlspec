"""Database configuration surfaces for SQLSpec adapters.

This module is intentionally interpreted even though compiled modules consume
its config classes. The public configuration API is stability-critical for
compiled callers: keep constructor fields, protocol attributes, migration
refresh behavior, storage capability hooks, and provider context managers
runtime-visible and backwards coherent. Move small pure helpers into compiled
modules only after proving the boundary with installed-wheel smoke coverage.
"""

import asyncio
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from inspect import Signature, signature
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeAlias, TypeVar, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.core.config_runtime import (
    build_default_statement_config,
    close_async_pool,
    close_sync_pool,
    create_async_pool,
    create_sync_pool,
    seed_runtime_driver_features,
)
from sqlspec.exceptions import MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations import AsyncMigrationTracker, SyncMigrationTracker, create_migration_commands
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
from sqlspec.typing import ConnectionT, PoolT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.core import StatementConfig
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
    from sqlspec.storage import StorageCapabilities


__all__ = (
    "ADKCompressionConfig",
    "ADKConfig",
    "ADKPartitionConfig",
    "ADKRetentionConfig",
    "ADKSqliteOptimizationConfig",
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "ConnectionT",
    "DatabaseConfigProtocol",
    "DriverT",
    "EventsConfig",
    "ExtensionConfigs",
    "FastAPIConfig",
    "FlaskConfig",
    "LifecycleConfig",
    "LitestarConfig",
    "MigrationConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "OpenTelemetryConfig",
    "PoolT",
    "PrometheusConfig",
    "SanicConfig",
    "StarletteConfig",
    "SyncConfigT",
    "SyncDatabaseConfig",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]")
SyncConfigT = TypeVar("SyncConfigT", bound="SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]")
ConfigT = TypeVar(
    "ConfigT",
    bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]",
)

DriverT = TypeVar("DriverT", bound="SyncDriverAdapterBase | AsyncDriverAdapterBase")

logger = get_logger("sqlspec.config")

DRIVER_FEATURE_LIFECYCLE_HOOKS: dict[str, str | None] = {
    "on_connection_create": "connection",
    "on_connection_destroy": "connection",
    "on_pool_create": "pool",
    "on_pool_destroying": "pool",
    "on_pool_destroy": "pool",
    "on_session_start": "session",
    "on_session_end": "session",
}


class _DriverFeatureHookWrapper:
    __slots__ = ("_callback", "_context_key", "_expects_argument")

    def __init__(self, callback: "Callable[..., Any]", context_key: "str | None", expects_argument: bool) -> None:
        self._callback = callback
        self._context_key = context_key
        self._expects_argument = expects_argument

    def __call__(self, context: "dict[str, Any]") -> None:
        if not self._expects_argument:
            self._callback()
            return
        if self._context_key is None:
            self._callback(context)
            return
        self._callback(context.get(self._context_key))


class LifecycleConfig(TypedDict):
    """Lifecycle hooks for database adapters.

    Each hook accepts a list of callables to support multiple handlers.
    """

    on_connection_create: NotRequired[list[Callable[[Any], None]]]
    on_connection_destroy: NotRequired[list[Callable[[Any], None]]]
    on_pool_create: NotRequired[list[Callable[[Any], None]]]
    on_pool_destroying: NotRequired[list[Callable[[Any], Any]]]
    on_pool_destroy: NotRequired[list[Callable[[Any], None]]]
    on_session_start: NotRequired[list[Callable[[Any], None]]]
    on_session_end: NotRequired[list[Callable[[Any], None]]]
    on_query_start: NotRequired[list[Callable[[str, dict[str, Any]], None]]]
    on_query_complete: NotRequired[list[Callable[[str, dict[str, Any], Any], None]]]
    on_error: NotRequired[list[Callable[[Exception, str, dict[str, Any]], None]]]


class MigrationConfig(TypedDict):
    """Configuration options for database migrations.

    All fields are optional with default values.
    """

    script_location: NotRequired["str | Path"]
    """Path to the migrations directory. Accepts string or Path object. Defaults to 'migrations'."""

    version_table_name: NotRequired[str]
    """Name of the table used to track applied migrations. Defaults to 'sqlspec_migrations'."""

    default_schema: NotRequired[str]
    """Schema applied to migration sessions before user migration SQL runs, when supported by the adapter."""

    version_table_schema: NotRequired[str]
    """Schema that stores the migration tracking table. Defaults to default_schema when omitted."""

    project_root: NotRequired[str]
    """Path to the project root directory. Used for relative path resolution."""

    enabled: NotRequired[bool]
    """Whether this configuration should be included in CLI operations. Defaults to True."""

    auto_sync: NotRequired[bool]
    """Enable automatic version reconciliation during upgrade. When enabled (default), SQLSpec automatically updates database tracking when migrations are renamed from timestamp to sequential format. Defaults to True."""

    strict_ordering: NotRequired[bool]
    """Enforce strict migration ordering. When enabled, prevents out-of-order migrations from being applied. Defaults to False."""

    include_extensions: NotRequired["list[str]"]
    """List of extension names whose migrations should be included. Extension migrations maintain separate versioning and are prefixed with 'ext_{name}_'.

    Note: Extensions with migration support (litestar, adk, events) are auto-included when
    their settings are present in ``extension_config``. Use ``exclude_extensions`` to opt out.
    """

    exclude_extensions: NotRequired["list[str]"]
    """
    List of extension names to exclude from automatic migration inclusion.

    When an extension is configured in ``extension_config``, its migrations are automatically
    included. Use this to prevent that for specific extensions:
    """

    transactional: NotRequired[bool]
    """Wrap migrations in transactions when supported. When enabled (default for adapters that support it), each migration runs in a transaction that is committed on success or rolled back on failure. This prevents partial migrations from leaving the database in an inconsistent state. Requires adapter support for transactional DDL. Defaults to True for PostgreSQL, SQLite, and DuckDB; False for MySQL, Oracle, and BigQuery. Individual migrations can override this with a '-- transactional: false' comment."""

    use_logger: NotRequired[bool]
    """
    Use Python logger instead of Rich console for migration output.

    When True, migration progress is logged via structlog/logging instead of being
    printed to the console with Rich formatting. This is useful for programmatic
    usage where console output is not desired.

    Can be overridden per-call via the ``use_logger`` parameter on ``migrate_up()``
    and ``migrate_down()`` methods.

    Defaults to False (Rich console output).
    """

    echo: NotRequired[bool]
    """Echo migration output to the console.

    When False, console output is suppressed. This is useful for script or CI
    environments that need quiet stdout.

    Defaults to True.
    """

    summary_only: NotRequired[bool]
    """Emit a single summary log entry for migration commands.

    When True and ``use_logger`` is enabled, per-migration output is suppressed
    in favor of a single structured summary log event.

    Defaults to False.
    """


class FlaskConfig(TypedDict):
    """Configuration options for Flask SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["flask"]:
    """

    connection_key: NotRequired[str]
    """Key for storing connection in Flask g object. Default: auto-generated from session_key."""

    session_key: NotRequired[str]
    """Key for accessing session via plugin.get_session(). Default: 'db_session'."""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'.
    - manual: No automatic commits, user handles explicitly
    - autocommit: Commits on 2xx status, rollback otherwise
    - autocommit_include_redirect: Commits on 2xx-3xx status, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: None."""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: None."""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Flask extension will not register request hooks for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter context population. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, request attributes are populated automatically.
    Set to ``False`` to explicitly disable this behavior.
    """


class LitestarConfig(TypedDict):
    """Configuration options for Litestar SQLSpec plugin.

    All fields are optional with sensible defaults.
    """

    session_table: NotRequired["bool | str"]
    """Enable session table for server-side session storage.

    - ``True``: Use default table name ('litestar_session')
    - ``"custom_name"``: Use custom table name

    When set, litestar extension migrations are auto-included to create the session table.
    If you're only using litestar for DI/connection management (not session storage),
    leave this unset to skip the migrations.
    """

    connection_key: NotRequired[str]
    """Key for storing connection in ASGI scope. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in application state. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in ASGI scope. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'"""

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True"""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``"""

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Litestar plugin will not register dependency providers for managing
    database connections, pools, and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration even when
    SQLCommenter is enabled on the driver config.
    """


class StarletteConfig(TypedDict):
    """Configuration options for Starlette SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["starlette"]:
    """

    connection_key: NotRequired[str]
    """Key for storing connection in request.state. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in app.state. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in request.state. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'

    - manual: No automatic commit/rollback
    - autocommit: Commit on 2xx, rollback otherwise
    - autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Starlette/FastAPI extension will not add middleware for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration.
    """

    sqlcommenter_framework: NotRequired[str]
    """Framework name for SQLCommenter attributes. Default: 'starlette'.
    Set to 'fastapi' when using FastAPI.
    """


class FastAPIConfig(StarletteConfig):
    """Configuration options for FastAPI SQLSpec extension.

    All fields are optional with sensible defaults. Use in ``extension_config["fastapi"]``.
    SQLCommenter defaults the framework attribute to ``"fastapi"``.
    """


class SanicConfig(TypedDict):
    """Configuration options for Sanic SQLSpec extension.

    All fields are optional with sensible defaults. Use in ``extension_config["sanic"]``.
    """

    connection_key: NotRequired[str]
    """Key for storing connection in request.ctx. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in app.ctx. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in request.ctx. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'

    - manual: No automatic commit/rollback
    - autocommit: Commit on 2xx, rollback otherwise
    - autocommit_include_redirect: Commit on 2xx-3xx, rollback otherwise
    """

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Sanic extension will not register request middleware for
    managing database connections and sessions. Users are responsible for
    managing the database lifecycle manually via their own DI solution.
    """

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: False."""

    correlation_header: NotRequired[str]
    """HTTP header to read the request correlation ID from when middleware is enabled. Default: ``X-Request-ID``."""

    correlation_headers: NotRequired[tuple[str, ...] | list[str]]
    """Additional HTTP headers to read as correlation ID fallbacks."""

    auto_trace_headers: NotRequired[bool]
    """Read standard trace context headers as correlation ID fallbacks. Default: True."""

    enable_sqlcommenter_middleware: NotRequired[bool]
    """Control automatic SQLCommenter middleware registration. Default: True.
    When the driver's :class:`~sqlspec.core.statement.StatementConfig` has
    ``enable_sqlcommenter=True``, the middleware is registered automatically.
    Set to ``False`` to explicitly disable middleware registration.
    """

    sqlcommenter_framework: NotRequired[str]
    """Framework name for SQLCommenter attributes. Default: 'sanic'."""


class ADKPartitionConfig(TypedDict):
    """Configuration for table partitioning and sharding strategies.

    Controls how ADK tables are partitioned across backends that support it.
    Backends without native partitioning support ignore these settings.
    """

    strategy: NotRequired[Literal["range", "list", "hash"]]
    """
    Partitioning strategy. Default: None (no partitioning).

    - range: Partition by range of values
    - list: Partition by discrete value lists
    - hash: Partition by hash of the partition key

    Supported by: PostgreSQL, MySQL 8+, Oracle, Spanner.
    Ignored by: SQLite, DuckDB.
    """

    partition_key: NotRequired[str]
    """Column name used as the partition key.

    For range partitioning with time-based data, this is typically a timestamp column
    like 'created_at'. For hash partitioning, this is typically the primary key.
    """

    session_partition_key: NotRequired[str]
    """Session-table partition key override for adapters that create separate ADK tables."""

    events_partition_key: NotRequired[str]
    """Event-table partition key override for adapters that create separate ADK tables."""

    memory_partition_key: NotRequired[str]
    """Memory-table partition key override for adapters that create separate ADK tables."""

    interval: NotRequired[str]
    """Partition interval for range partitioning.

    Only meaningful when strategy is 'range'.
    """

    partition_count: NotRequired[int]
    """Number of hash partitions for adapters that support hash-partitioned ADK tables."""

    initial_less_than: NotRequired[str]
    """Initial range-partition upper bound for adapters that require a seed partition."""


class ADKRetentionConfig(TypedDict):
    """Configuration for data retention and TTL policies.

    Controls automatic cleanup of expired data. Backends with native TTL support
    (CockroachDB Row-Level TTL, Spanner Row Deletion Policy) use database-level
    enforcement. Others fall back to application-level sweep queries.
    """

    session_ttl_seconds: NotRequired[int]
    """TTL for session records in seconds. Default: 0 (no expiry).

    When set, sessions older than this threshold are eligible for cleanup.
    Backends with native TTL (CockroachDB, Spanner) enforce this at the database level.
    Others require application-level cleanup via periodic sweep.
    """

    event_ttl_seconds: NotRequired[int]
    """TTL for event records in seconds. Default: 0 (no expiry).

    When set, events older than this threshold are eligible for cleanup.
    """

    memory_ttl_seconds: NotRequired[int]
    """TTL for memory entries in seconds. Default: 0 (no expiry).

    When set, memory entries older than this threshold are eligible for cleanup.
    """

    sweep_interval_seconds: NotRequired[int]
    """Interval between application-level cleanup sweeps in seconds. Default: 3600 (1 hour).

    Only used when the backend does not support native TTL enforcement.
    Set to 0 to disable automatic sweeps (manual cleanup only).
    """


class ADKCompressionConfig(TypedDict):
    """Configuration for table-level compression.

    Controls compression of ADK table storage. Support and algorithms vary by backend.
    """

    enabled: NotRequired[bool]
    """Enable table compression. Default: False.

    When True, adapters that support table-level compression will apply it
    during table creation.
    """

    algorithm: NotRequired[str]
    """
    Compression algorithm name. Backend-specific.

    When omitted, the backend default is used.
    """

    level: NotRequired[int]
    """Compression level (where supported). Higher levels trade CPU for space savings.

    Valid ranges depend on the algorithm and backend.
    """


class ADKSqliteOptimizationConfig(TypedDict):
    """SQLite-specific PRAGMA optimization settings.

    Controls SQLite performance tuning parameters applied at connection time.
    These settings are ignored by non-SQLite adapters.
    """

    cache_size: NotRequired[int]
    """SQLite page cache size. Default: -64000 (64 MB, negative means KiB).

    Larger caches reduce disk I/O for read-heavy workloads.
    Negative values specify size in KiB; positive values specify page count.
    """

    mmap_size: NotRequired[int]
    """SQLite memory-mapped I/O size in bytes. Default: 31457280 (30 MB).

    Enables memory-mapped I/O for faster reads. Set to 0 to disable.
    """

    journal_size_limit: NotRequired[int]
    """SQLite journal file size limit in bytes. Default: 67108864 (64 MB).

    Limits the size of the WAL or rollback journal file.
    Prevents unbounded journal growth in write-heavy workloads.
    """


class ADKConfig(TypedDict):
    """Configuration options for ADK session and memory store extension.

    All fields are optional with sensible defaults. Use in extension_config["adk"]:

    Configuration supports three deployment scenarios:
        1. SQLSpec manages everything (runtime + migrations)
        2. SQLSpec runtime only (external migration tools like Alembic/Flyway)
        3. Selective features (sessions OR memory, not both)
    """

    enable_sessions: NotRequired[bool]
    """Enable session store at runtime. Default: True.

    When False: session service unavailable, session store operations disabled.
    Independent of migration control - can use externally-managed tables.
    """

    enable_memory: NotRequired[bool]
    """Enable memory store at runtime. Default: True.

    When False: memory service unavailable, memory store operations disabled.
    Independent of migration control - can use externally-managed tables.
    """

    include_sessions_migration: NotRequired[bool]
    """Include session tables in SQLSpec migrations. Default: True.

    When False: session migration DDL skipped (use external migration tools).
    Decoupled from enable_sessions - allows external table management with SQLSpec runtime.
    """

    include_memory_migration: NotRequired[bool]
    """Include memory tables in SQLSpec migrations. Default: True.

    When False: memory migration DDL skipped (use external migration tools).
    Decoupled from enable_memory - allows external table management with SQLSpec runtime.
    """

    session_table: NotRequired[str]
    """Name of the sessions table. Default: 'adk_sessions'"""

    events_table: NotRequired[str]
    """Name of the events table. Default: 'adk_events'"""

    memory_table: NotRequired[str]
    """Name of the memory entries table. Default: 'adk_memory_entries'"""

    artifact_table: NotRequired[str]
    """Name of the artifact versions table. Default: 'adk_artifact_versions'"""

    artifact_storage_uri: NotRequired[str]
    """
    Base URI for artifact content storage.

    Points to a ``sqlspec/storage/`` backend where artifact binary content
    is stored. Can be a direct URI (``s3://bucket/path``, ``file:///path``)
    or a registered alias in the storage registry.
    """

    memory_use_fts: NotRequired[bool]
    """Enable full-text search when supported. Default: False.

    When True, adapters will use their native FTS capabilities where available:
    - PostgreSQL: to_tsvector/to_tsquery with GIN index
    - SQLite: FTS5 virtual table
    - DuckDB: FTS extension with match_bm25
    - Oracle: CONTAINS() with CTXSYS.CONTEXT index
    - Spanner: TOKENIZE_FULLTEXT with search index
    - MySQL: MATCH...AGAINST with FULLTEXT index

    When False, adapters use simple LIKE/ILIKE queries (works without indexes).
    """

    memory_max_results: NotRequired[int]
    """Maximum number of results for memory search queries. Default: 20.

    Limits the number of memory entries returned by search_memory().
    Can be overridden per-query via the limit parameter.
    """

    owner_id_column: NotRequired[str]
    """
    Optional owner ID column definition to link sessions/memories to a user, tenant, team, or other entity.

    Format: "column_name TYPE [NOT NULL] REFERENCES table(column) [options...]"

    The entire definition is passed through to DDL verbatim. We only parse
    the column name (first word) for use in INSERT/SELECT statements.

    This column is added to both session and memory tables for consistent
    multi-tenant isolation.

    Supports:
        - Foreign key constraints: REFERENCES table(column)
        - Nullable or NOT NULL
        - CASCADE options: ON DELETE CASCADE, ON UPDATE CASCADE
        - Dialect-specific options (DEFERRABLE, ENABLE VALIDATE, etc.)
        - Plain columns without FK (just extra column storage)
    """

    in_memory: NotRequired[bool]
    """
    Enable in-memory table storage (Oracle-specific). Default: False.

    When enabled, tables are created with the INMEMORY clause for Oracle Database,
    which stores table data in columnar format in memory for faster query performance.

    This is an Oracle-specific feature that requires:
        - Oracle Database 12.1.0.2 or higher
        - Database In-Memory option license (Enterprise Edition)
        - Sufficient INMEMORY_SIZE configured in the database instance

    Other database adapters ignore this setting.
    """

    shard_count: NotRequired[int]
    """Optional hash shard count for session/event tables to reduce hotspotting.

    When set (>1), adapters that support computed shard columns will create a
    generated shard_id using MOD(FARM_FINGERPRINT(primary_key), shard_count) and
    include it in the primary key and filters. Ignored by adapters that do not
    support computed shards.
    """

    session_table_options: NotRequired[str]
    """
    Adapter-specific table OPTIONS/clauses for the sessions table.

    Passed verbatim when supported. Ignored by
    adapters without table OPTIONS support.
    """

    events_table_options: NotRequired[str]
    """Adapter-specific table OPTIONS/clauses for the events table."""

    memory_table_options: NotRequired[str]
    """Adapter-specific table OPTIONS/clauses for the memory table."""

    expires_index_options: NotRequired[str]
    """Adapter-specific options for the expires/index used in ADK stores."""

    # --- Capability-based configuration (Chapter 2: schema-capability-config) ---

    fts_language: NotRequired[str]
    """
    Language configuration for full-text search indexing. Default: 'english'.

    Controls the language dictionary/stemmer for FTS implementations:
        - PostgreSQL: to_tsvector/to_tsquery language parameter
        - SQLite FTS5: tokenizer language for unicode61/porter
        - MySQL: FULLTEXT parser language (with ngram for CJK on 5.7.6+)
        - Oracle: CTXSYS.CONTEXT lexer language
        - Spanner: TOKENIZE_FULLTEXT language parameter
        - DuckDB: FTS stemmer language

    Only takes effect when ``memory_use_fts`` is True.

    Common values: 'english', 'simple', 'german', 'french', 'spanish',
    'portuguese', 'italian', 'dutch', 'russian', 'chinese', 'japanese', 'korean'.
    """

    schema_version: NotRequired[int]
    """
    Explicit schema version for ADK tables. Default: None (auto-detect).

    When set, locks the ADK schema to a specific version. This is useful for:
    - Preventing automatic schema upgrades in production
    - Pinning to a known-good schema during testing
    - Coordinating schema changes across multiple application instances

    When None, the ADK extension auto-detects the current schema version
    and applies any pending upgrades during initialization.
    """

    partitioning: NotRequired[ADKPartitionConfig]
    """Table partitioning configuration. Default: None (no partitioning).

    Controls how ADK tables are partitioned for improved query performance
    and data management at scale. See ``ADKPartitionConfig`` for options.

    Supported by: PostgreSQL, MySQL 8+, Oracle, Spanner.
    Ignored by: SQLite, DuckDB.
    """

    retention: NotRequired[ADKRetentionConfig]
    """Data retention and TTL configuration. Default: None (no automatic cleanup).

    Controls automatic expiry and cleanup of old session, event, and memory data.
    See ``ADKRetentionConfig`` for options.

    Backends with native TTL (CockroachDB, Spanner) use database-level enforcement.
    Others fall back to application-level sweep queries.
    """

    compression: NotRequired[ADKCompressionConfig]
    """Table compression configuration. Default: None (no compression).

    Controls table-level compression for ADK tables.
    See ``ADKCompressionConfig`` for options.
    """

    sqlite_optimization: NotRequired[ADKSqliteOptimizationConfig]
    """SQLite-specific PRAGMA optimization settings. Default: None (SQLite defaults).

    Controls SQLite performance tuning parameters. Ignored by non-SQLite adapters.
    See ``ADKSqliteOptimizationConfig`` for options.
    """


class EventsConfig(TypedDict):
    """Configuration options for the events extension.

    Use in ``extension_config["events"]``.
    """

    backend: NotRequired[Literal["listen_notify", "table_queue", "listen_notify_durable", "advanced_queue"]]
    """Backend implementation. PostgreSQL adapters default to 'listen_notify', others to 'table_queue'.

    - listen_notify: Real-time PostgreSQL LISTEN/NOTIFY (ephemeral)
    - table_queue: Durable table-backed queue with retries (all adapters)
    - listen_notify_durable: Hybrid combining both (PostgreSQL only)
    - advanced_queue: Oracle Advanced Queueing
    """

    queue_table: NotRequired[str]
    """Name of the fallback queue table. Defaults to 'sqlspec_event_queue'."""

    lease_seconds: NotRequired[int]
    """Lease duration for claimed events before they can be retried. Defaults to 30 seconds."""

    retention_seconds: NotRequired[int]
    """Retention window for acknowledged events before cleanup. Defaults to 86400 (24 hours)."""

    poll_interval: NotRequired[float]
    """Default poll interval in seconds for event consumers. Defaults to 1.0."""

    select_for_update: NotRequired[bool]
    """Use SELECT FOR UPDATE locking when claiming events. Defaults to False."""

    skip_locked: NotRequired[bool]
    """Use SKIP LOCKED for non-blocking event claims. Defaults to False."""

    json_passthrough: NotRequired[bool]
    """Skip JSON encoding/decoding for payloads. Defaults to False."""

    in_memory: NotRequired[bool]
    """
    Enable Oracle INMEMORY clause for the queue table. Ignored by other adapters. Defaults to False.

    Note: To skip events migrations,
    use ``migration_config={"exclude_extensions": ["events"]}``.
    """


class OpenTelemetryConfig(TypedDict):
    """Configuration options for OpenTelemetry integration.

    Use in ``extension_config["otel"]``.
    """

    enabled: NotRequired[bool]
    """Enable the extension. Default: True."""

    enable_spans: NotRequired[bool]
    """Enable span emission (set False to disable while keeping other settings)."""

    resource_attributes: NotRequired[dict[str, Any]]
    """Additional resource attributes passed to the tracer provider factory."""

    tracer_provider: NotRequired[Any]
    """Tracer provider instance to reuse. Mutually exclusive with ``tracer_provider_factory``."""

    tracer_provider_factory: NotRequired[Callable[[], Any]]
    """Factory returning a tracer provider. Invoked lazily when spans are needed."""


class PrometheusConfig(TypedDict):
    """Configuration options for Prometheus metrics.

    Use in ``extension_config["prometheus"]``.
    """

    enabled: NotRequired[bool]
    """Enable the extension. Default: True."""

    namespace: NotRequired[str]
    """Prometheus metric namespace. Default: ``"sqlspec"``."""

    subsystem: NotRequired[str]
    """Prometheus metric subsystem. Default: ``"driver"``."""

    registry: NotRequired[Any]
    """Custom Prometheus registry (defaults to the global registry)."""

    label_names: NotRequired[tuple[str, ...]]
    """Labels applied to metrics. Default: ("driver", "operation")."""

    duration_buckets: NotRequired[tuple[float, ...]]
    """Histogram buckets for query duration (seconds)."""


ExtensionConfigs: TypeAlias = dict[
    str,
    dict[str, Any]
    | LitestarConfig
    | FastAPIConfig
    | StarletteConfig
    | SanicConfig
    | FlaskConfig
    | ADKConfig
    | EventsConfig
    | OpenTelemetryConfig
    | PrometheusConfig,
]


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the stability-critical config contract.

    Compiled callers rely on these attributes and methods remaining
    runtime-visible while ``sqlspec.config`` stays interpreted. Changes to
    migration setup, pool/session provider behavior, storage capabilities, or
    observability bootstrap must preserve this contract or move behind a
    separately verified compiled helper boundary.
    """

    __slots__ = (
        "_migration_commands",
        "_migration_config",
        "_migration_loader",
        "_observability_runtime",
        "_storage_capabilities",
        "bind_key",
        "connection_instance",
        "driver_features",
        "extension_config",
        "observability_config",
        "statement_config",
    )

    _migration_loader: "SQLFileLoader"
    _migration_commands: "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]"
    _migration_config: "dict[str, Any] | MigrationConfig"
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    migration_tracker_type: "ClassVar[type[Any]]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    requires_staging_for_load: "ClassVar[bool]" = False
    staging_protocols: "ClassVar[tuple[str, ...]]" = ()
    default_storage_profile: "ClassVar[str | None]" = None
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed",)
    bind_key: "str | None"
    statement_config: "StatementConfig"
    connection_instance: "PoolT | None"
    extension_config: "ExtensionConfigs"
    driver_features: "dict[str, Any]"
    _storage_capabilities: "StorageCapabilities | None"
    observability_config: "ObservabilityConfig | None"
    _observability_runtime: "ObservabilityRuntime | None"

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return bool(
            self.connection_instance == other.connection_instance and self.migration_config == other.migration_config
        )

    def __repr__(self) -> str:
        parts = ", ".join([
            f"connection_instance={self.connection_instance!r}",
            f"migration_config={self.migration_config!r}",
        ])
        return f"{type(self).__name__}({parts})"

    @property
    def migration_config(self) -> "dict[str, Any] | MigrationConfig":
        """Return the current migration configuration."""
        return self._migration_config

    @migration_config.setter
    def migration_config(self, value: "dict[str, Any] | MigrationConfig | None") -> None:
        """Store migration configuration and refresh derived migration helpers."""
        object.__setattr__(self, "_migration_config", dict(cast("dict[str, Any]", value) or {}))
        if self._has_initialized_attribute("extension_config"):
            self._ensure_extension_migrations()
        if self._migration_components_ready():
            self._initialize_migration_components()

    def set_migration_config(self, config: "dict[str, Any] | MigrationConfig") -> None:
        """Attach migration configuration after initial config creation.

        This is equivalent to setting ``migration_config`` directly but provides
        a discoverable method for post-construction configuration.

        Args:
            config: Migration configuration dictionary.
        """
        self.migration_config = config

    def storage_capabilities(self) -> "StorageCapabilities":
        """Return cached storage capabilities for this configuration."""

        if self._storage_capabilities is None:
            self._storage_capabilities = self._build_storage_capabilities()
        return cast("StorageCapabilities", dict(self._storage_capabilities))

    def reset_storage_capabilities_cache(self) -> None:
        """Clear the cached capability snapshot."""

        self._storage_capabilities = None

    def _has_initialized_attribute(self, attribute_name: str) -> bool:
        """Return whether a slot-backed attribute has been initialized."""
        try:
            object.__getattribute__(self, attribute_name)
        except AttributeError:
            return False
        return True

    def _migration_components_ready(self) -> bool:
        """Return whether migration helpers have already been initialized."""
        return self._has_initialized_attribute("_migration_loader") and self._has_initialized_attribute(
            "_migration_commands"
        )

    def _ensure_extension_migrations(self) -> None:
        """Auto-include extension migrations when extension_config has them configured.

        Extensions with migration support are automatically included in
        ``migration_config["include_extensions"]`` based on their settings:

        - **litestar**: Only when ``session_table`` is set (for session storage)
        - **adk**: When any adk settings are present
        - **events**: When any events settings are present

        Use ``exclude_extensions`` to opt out of auto-inclusion.
        """
        extension_settings = cast("dict[str, Any]", self.extension_config)
        migration_config = cast("dict[str, Any]", self.migration_config)

        exclude_extensions = migration_config.get("exclude_extensions", [])
        if isinstance(exclude_extensions, tuple):
            exclude_extensions = list(exclude_extensions)  # pyright: ignore

        extensions_to_add: list[str] = []

        litestar_settings = extension_settings.get("litestar")
        if (
            litestar_settings is not None
            and "session_table" in litestar_settings
            and "litestar" not in exclude_extensions
        ):
            extensions_to_add.append("litestar")

        adk_settings = extension_settings.get("adk")
        if adk_settings is not None and "adk" not in exclude_extensions:
            from sqlspec.extensions.adk._config_utils import _validate_adk_store_registration

            _validate_adk_store_registration(self)
            extensions_to_add.append("adk")

        events_settings = extension_settings.get("events")
        if events_settings is not None and "events" not in exclude_extensions:
            extensions_to_add.append("events")

        if not extensions_to_add:
            return

        include_extensions = migration_config.get("include_extensions")
        if include_extensions is None:
            include_list: list[str] = []
            migration_config["include_extensions"] = include_list
        elif isinstance(include_extensions, tuple):
            include_list = list(include_extensions)  # pyright: ignore
            migration_config["include_extensions"] = include_list
        else:
            include_list = cast("list[str]", include_extensions)

        for ext in extensions_to_add:
            if ext not in include_list:
                include_list.append(ext)

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return default event runtime hints for this configuration."""

        return EventRuntimeHints()

    def _build_storage_capabilities(self) -> "StorageCapabilities":
        arrow_dependency_needed = self.supports_native_arrow_export or self.supports_native_arrow_import
        parquet_dependency_needed = self.supports_native_parquet_export or self.supports_native_parquet_import

        arrow_dependency_ready = self._dependency_available(ensure_pyarrow) if arrow_dependency_needed else False
        parquet_dependency_ready = self._dependency_available(ensure_pyarrow) if parquet_dependency_needed else False

        capabilities: StorageCapabilities = {
            "arrow_export_enabled": bool(self.supports_native_arrow_export and arrow_dependency_ready),
            "arrow_import_enabled": bool(self.supports_native_arrow_import and arrow_dependency_ready),
            "parquet_export_enabled": bool(self.supports_native_parquet_export and parquet_dependency_ready),
            "parquet_import_enabled": bool(self.supports_native_parquet_import and parquet_dependency_ready),
            "requires_staging_for_load": self.requires_staging_for_load,
            "staging_protocols": list(self.staging_protocols),
            "partition_strategies": list(self.storage_partition_strategies),
        }
        if self.default_storage_profile is not None:
            capabilities["default_storage_profile"] = self.default_storage_profile
        return capabilities

    def _init_observability(self, observability_config: "ObservabilityConfig | None" = None) -> None:
        """Initialize observability attributes for the configuration."""

        self.observability_config = observability_config
        self._observability_runtime = None

    def _configure_observability_extensions(self) -> None:
        """Apply extension_config hooks (otel/prometheus) to ObservabilityConfig."""

        config_map = cast("dict[str, Any]", self.extension_config)
        if not config_map:
            return
        updated = self.observability_config

        otel_config = cast("OpenTelemetryConfig | None", config_map.get("otel"))
        if otel_config and otel_config.get("enabled", True):
            from sqlspec.extensions import otel as otel_extension

            updated = otel_extension.enable_tracing(
                base_config=updated,
                resource_attributes=otel_config.get("resource_attributes"),
                tracer_provider=otel_config.get("tracer_provider"),
                tracer_provider_factory=otel_config.get("tracer_provider_factory"),
                enable_spans=otel_config.get("enable_spans", True),
            )

        prom_config = cast("PrometheusConfig | None", config_map.get("prometheus"))
        if prom_config and prom_config.get("enabled", True):
            from sqlspec.extensions import prometheus as prometheus_extension

            label_names = tuple(prom_config.get("label_names", ("driver", "operation")))
            duration_buckets = prom_config.get("duration_buckets")
            if duration_buckets is not None:
                duration_buckets = tuple(duration_buckets)

            updated = prometheus_extension.enable_metrics(
                base_config=updated,
                namespace=prom_config.get("namespace", "sqlspec"),
                subsystem=prom_config.get("subsystem", "driver"),
                registry=prom_config.get("registry"),
                label_names=label_names,
                duration_buckets=duration_buckets,
            )

        if updated is not self.observability_config:
            self.observability_config = updated

    def _promote_driver_feature_hooks(self) -> None:
        lifecycle_hooks: dict[str, list[Callable[[dict[str, Any]], None]]] = {}

        for hook_name, context_key in DRIVER_FEATURE_LIFECYCLE_HOOKS.items():
            callback = self.driver_features.pop(hook_name, None)
            if callback is None:
                continue
            callbacks = callback if isinstance(callback, (list, tuple)) else (callback,)  # pyright: ignore
            wrapped_callbacks = [self._wrap_driver_feature_hook(cb, context_key) for cb in callbacks]  # pyright: ignore
            lifecycle_hooks.setdefault(hook_name, []).extend(wrapped_callbacks)

        if not lifecycle_hooks:
            return

        lifecycle_config = cast("LifecycleConfig", lifecycle_hooks)
        override = ObservabilityConfig(lifecycle=lifecycle_config)
        if self.observability_config is None:
            self.observability_config = override
        else:
            self.observability_config = ObservabilityConfig.merge(self.observability_config, override)

    @staticmethod
    def _wrap_driver_feature_hook(
        callback: Callable[..., Any], context_key: str | None
    ) -> Callable[[dict[str, Any]], None]:
        try:
            hook_signature: Signature = signature(callback)
        except (TypeError, ValueError):  # pragma: no cover
            hook_signature = Signature()

        positional_params = [
            param
            for param in hook_signature.parameters.values()
            if param.kind in {param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD} and param.default is param.empty
        ]
        expects_argument = bool(positional_params)

        return _DriverFeatureHookWrapper(callback, context_key, expects_argument)

    def attach_observability(self, registry_config: "ObservabilityConfig | None") -> None:
        """Attach merged observability runtime composed from registry and adapter overrides."""
        merged = ObservabilityConfig.merge(registry_config, self.observability_config)
        self._observability_runtime = ObservabilityRuntime(
            merged, bind_key=self.bind_key, config_name=type(self).__name__
        )

    def get_observability_runtime(self) -> "ObservabilityRuntime":
        """Return the attached runtime, creating a disabled instance when missing."""

        if self._observability_runtime is None:
            self.attach_observability(None)
        if self._observability_runtime is None:
            msg = "ObservabilityRuntime was not set by attach_observability; this is a bug"
            raise RuntimeError(msg)
        return self._observability_runtime

    def _prepare_driver(self, driver: DriverT) -> DriverT:
        """Attach observability runtime to driver instances before returning them."""

        driver.attach_observability(self.get_observability_runtime())
        return driver

    @staticmethod
    def _dependency_available(checker: "Callable[[], None]") -> bool:
        try:
            checker()
        except MissingDependencyError:
            return False
        return True

    @abstractmethod
    def create_connection(self) -> "ConnectionT | Awaitable[ConnectionT]":
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    @abstractmethod
    def provide_session(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> "PoolT | Awaitable[PoolT]":
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def close_pool(self) -> "Awaitable[None] | None":
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self, *args: Any, **kwargs: Any
    ) -> "PoolT | Awaitable[PoolT] | AbstractContextManager[PoolT] | AbstractAsyncContextManager[PoolT]":
        """Provide pool instance."""
        raise NotImplementedError

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for this database configuration.

        Returns a dictionary of type names to objects (classes, functions, or
        other callables) that should be registered with Litestar's signature
        namespace to prevent serialization attempts on database-specific
        structures.

        Returns:
            Dictionary mapping type names to objects.
        """
        return {}

    def _initialize_migration_components(self) -> None:
        """Initialize migration loader and migration command helpers."""
        runtime = self.get_observability_runtime()
        self._migration_loader = SQLFileLoader(runtime=runtime)
        self._migration_commands = create_migration_commands(self)  # pyright: ignore

    def _ensure_migration_loader(self) -> "SQLFileLoader":
        """Get the migration SQL loader and auto-load files if needed.

        Returns:
            SQLFileLoader instance for migration files.
        """
        migration_config = self.migration_config or {}
        script_location = migration_config.get("script_location", "migrations")

        migration_path = Path(script_location)
        if migration_path.exists() and not self._migration_loader.list_files():
            self._migration_loader.load_sql(migration_path)
            logger.debug("Auto-loaded migration SQL files from %s", migration_path)

        return self._migration_loader

    def _ensure_migration_commands(self) -> "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]":
        """Get the migration commands instance.

        Returns:
            MigrationCommands instance for this config.
        """
        return self._migration_commands

    def get_migration_loader(self) -> "SQLFileLoader":
        """Get the SQL loader for migration files.

        Provides access to migration SQL files loaded from the configured
        script_location directory. Files are loaded lazily on first access.

        Returns:
            SQLFileLoader instance with migration files loaded.
        """
        return self._ensure_migration_loader()

    def load_migration_sql_files(self, *paths: "str | Path") -> None:
        """Load additional migration SQL files from specified paths.

        Args:
            *paths: One or more file paths or directory paths to load migration SQL files from.
        """

        loader = self._ensure_migration_loader()
        for path in paths:
            path_obj = Path(path)
            if path_obj.exists():
                loader.load_sql(path_obj)
                logger.debug("Loaded migration SQL files from %s", path_obj)
            else:
                logger.warning("Migration path does not exist: %s", path_obj)

    def get_migration_commands(self) -> "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]":
        """Get migration commands for this configuration.

        Returns:
            MigrationCommands instance configured for this database.
        """
        return self._ensure_migration_commands()

    @abstractmethod
    def migrate_up(
        self,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> "Awaitable[None] | None":
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest. Defaults to "head".
            allow_missing: Allow out-of-order migrations. Defaults to False.
            auto_sync: Auto-reconcile renamed migrations. Defaults to True.
            dry_run: Show what would be done without applying. Defaults to False.
            use_logger: Use Python logger instead of Rich console for output.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        raise NotImplementedError

    @abstractmethod
    def migrate_down(
        self,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> "Awaitable[None] | None":
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations. Defaults to "-1".
            dry_run: Show what would be done without applying. Defaults to False.
            use_logger: Use Python logger instead of Rich console for output.
                Defaults to False. Can be set via MigrationConfig for persistent default.
            echo: Echo output to the console. Defaults to True when unset.
            summary_only: Emit a single summary log entry when logger output is enabled.
        """
        raise NotImplementedError

    @abstractmethod
    def get_current_migration(self, verbose: bool = False) -> "Awaitable[str | None] | str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history. Defaults to False.

        Returns:
            Current migration version or None if no migrations applied.
        """
        raise NotImplementedError

    @abstractmethod
    def create_migration(self, message: str, file_type: str = "sql") -> "Awaitable[None] | None":
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py'). Defaults to 'sql'.
        """
        raise NotImplementedError

    @abstractmethod
    def init_migrations(self, directory: "str | None" = None, package: bool = True) -> "Awaitable[None] | None":
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in. Uses script_location from migration_config if not provided.
            package: Whether to create __init__.py file. Defaults to True.
        """
        raise NotImplementedError

    @abstractmethod
    def stamp_migration(self, revision: str) -> "Awaitable[None] | None":
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        raise NotImplementedError

    @abstractmethod
    def fix_migrations(
        self, dry_run: bool = False, update_database: bool = True, yes: bool = False
    ) -> "Awaitable[None] | None":
        """Convert timestamp migrations to sequential format.

        Implements hybrid versioning workflow where development uses timestamps
        and production uses sequential numbers. Creates backup before changes
        and provides rollback on errors.

        Args:
            dry_run: Preview changes without applying. Defaults to False.
            update_database: Update migration records in database. Defaults to True.
            yes: Skip confirmation prompt. Defaults to False.
        """
        raise NotImplementedError


class _SyncMigrationMixin:
    """Shared sync migration convenience methods."""

    __slots__ = ()

    def migrate_up(
        self: Any,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.upgrade(
            revision, allow_missing, auto_sync, dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )

    def migrate_down(
        self: Any,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations down to specified revision."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.downgrade(revision, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only)

    def get_current_migration(self: Any, verbose: bool = False) -> "str | None":
        """Get the current migration version."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        return commands.current(verbose=verbose)

    def create_migration(self: Any, message: str, file_type: str = "sql") -> None:
        """Create a new migration file."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.revision(message, file_type)

    def init_migrations(self: Any, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure."""
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.init(directory, package)

    def stamp_migration(self: Any, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.stamp(revision)

    def fix_migrations(self: Any, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format."""
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        commands.fix(dry_run, update_database, yes)


class _AsyncMigrationMixin:
    """Shared async migration convenience methods."""

    __slots__ = ()

    async def migrate_up(
        self: Any,
        revision: str = "head",
        allow_missing: bool = False,
        auto_sync: bool = True,
        dry_run: bool = False,
        *,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations up to specified revision."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.upgrade(
            revision, allow_missing, auto_sync, dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only
        )

    async def migrate_down(
        self: Any,
        revision: str = "-1",
        *,
        dry_run: bool = False,
        use_logger: bool = False,
        echo: bool | None = None,
        summary_only: bool | None = None,
    ) -> None:
        """Apply database migrations down to specified revision."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.downgrade(revision, dry_run=dry_run, use_logger=use_logger, echo=echo, summary_only=summary_only)

    async def get_current_migration(self: Any, verbose: bool = False) -> "str | None":
        """Get the current migration version."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        return await commands.current(verbose=verbose)

    async def create_migration(self: Any, message: str, file_type: str = "sql") -> None:
        """Create a new migration file."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.revision(message, file_type)

    async def init_migrations(self: Any, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure."""
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.init(directory, package)

    async def stamp_migration(self: Any, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.stamp(revision)

    async def fix_migrations(self: Any, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format."""
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.fix(dry_run, update_database, yes)


class NoPoolSyncConfig(_SyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for sync database configurations that do not implement a pool."""

    __slots__ = ("connection_config",)
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: dict[str, Any] | None = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.connection_instance = connection_instance
        self.connection_config = connection_config or {}
        self.extension_config = extension_config or {}
        self.migration_config = migration_config or {}
        self._init_observability(observability_config)
        self._initialize_migration_components()

        self._storage_capabilities = None
        self.statement_config = statement_config or build_default_statement_config("sqlite")

        self.driver_features = seed_runtime_driver_features(driver_features, self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    def create_pool(self) -> None:
        return None

    def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class NoPoolAsyncConfig(_AsyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for async database configurations that do not implement a pool."""

    __slots__ = ("connection_config",)
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.connection_instance = connection_instance
        self.connection_config = connection_config or {}
        self.extension_config = extension_config or {}
        self.migration_config = migration_config or {}
        self._init_observability(observability_config)
        self._initialize_migration_components()

        self.statement_config = statement_config or build_default_statement_config("sqlite")

        self._storage_capabilities = None
        self.driver_features = seed_runtime_driver_features(driver_features, self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class SyncDatabaseConfig(_SyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for sync database configurations with connection pooling."""

    __slots__ = ("_pool_lock", "connection_config")
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker
    _connection_context_class: "ClassVar[type[Any]]"
    _session_factory_class: "ClassVar[type[Any]]"
    _session_context_class: "ClassVar[type[Any]]"
    _default_statement_config: "ClassVar[StatementConfig]"

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        self.bind_key = bind_key
        self.connection_instance = connection_instance
        self.connection_config = connection_config or {}
        self.extension_config = extension_config or {}
        self.migration_config = migration_config or {}
        self._init_observability(observability_config)
        self._initialize_migration_components()

        self.statement_config = statement_config or build_default_statement_config("postgres")

        self._storage_capabilities = None
        self.driver_features = seed_runtime_driver_features(driver_features, self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()
        self._pool_lock = threading.Lock()

    def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        existing_pool = self.connection_instance
        if existing_pool is not None:
            return existing_pool

        created_pool = create_sync_pool(
            None,
            self._pool_lock,
            lambda: self.connection_instance,
            self._create_pool,
            self.get_observability_runtime().emit_pool_create_sync,
        )
        self.connection_instance = created_pool
        return cast("PoolT", created_pool)

    def close_pool(self) -> None:
        """Close the connection pool."""
        pool = self.connection_instance
        runtime = self.get_observability_runtime()
        close_sync_pool(pool, self._close_pool, runtime.emit_pool_destroy_sync, runtime.emit_pool_destroying_sync)
        self.connection_instance = None

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        return self.create_pool()

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        return cast("AbstractContextManager[ConnectionT]", self._connection_context_class(self))

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        handler = self._session_factory_class(self)
        return cast(
            "AbstractContextManager[DriverT]",
            self._session_context_class(
                acquire_connection=handler.acquire_connection,
                release_connection=handler.release_connection,
                statement_config=statement_config or self.statement_config or self._default_statement_config,
                driver_features=self.driver_features,
                prepare_driver=self._prepare_driver,
            ),
        )

    @abstractmethod
    def _create_pool(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool(self) -> None:
        """Actual pool destruction implementation."""
        raise NotImplementedError


class AsyncDatabaseConfig(_AsyncMigrationMixin, DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for async database configurations with connection pooling."""

    __slots__ = ("_pool_lock", "connection_config")
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker
    _connection_context_class: "ClassVar[type[Any]]"
    _session_factory_class: "ClassVar[type[Any]]"
    _session_context_class: "ClassVar[type[Any]]"
    _default_statement_config: "ClassVar[StatementConfig]"

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        connection_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        self.bind_key = bind_key
        self.connection_instance = connection_instance
        self.connection_config = connection_config or {}
        self.extension_config = extension_config or {}
        self.migration_config = migration_config or {}
        self._init_observability(observability_config)
        self._initialize_migration_components()

        self.statement_config = statement_config or build_default_statement_config("postgres")

        self._storage_capabilities = None
        self.driver_features = seed_runtime_driver_features(driver_features, self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()
        self._pool_lock = asyncio.Lock()

    async def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        existing_pool = self.connection_instance
        if existing_pool is not None:
            return existing_pool

        created_pool = await create_async_pool(
            None,
            self._pool_lock,
            lambda: self.connection_instance,
            self._create_pool,
            self.get_observability_runtime().emit_pool_create_async,
        )
        self.connection_instance = created_pool
        return cast("PoolT", created_pool)

    async def close_pool(self) -> None:
        """Close the connection pool."""
        pool = self.connection_instance
        runtime = self.get_observability_runtime()
        await close_async_pool(
            pool, self._close_pool, runtime.emit_pool_destroy_async, runtime.emit_pool_destroying_async
        )
        self.connection_instance = None

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        return await self.create_pool()

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        return cast("AbstractAsyncContextManager[ConnectionT]", self._connection_context_class(self))

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        handler = self._session_factory_class(self)
        return cast(
            "AbstractAsyncContextManager[DriverT]",
            self._session_context_class(
                acquire_connection=handler.acquire_connection,
                release_connection=handler.release_connection,
                statement_config=statement_config or self.statement_config or self._default_statement_config,
                driver_features=self.driver_features,
                prepare_driver=self._prepare_driver,
            ),
        )

    @abstractmethod
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError
