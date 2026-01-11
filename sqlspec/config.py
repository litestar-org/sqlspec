from abc import ABC, abstractmethod
from collections.abc import Callable
from inspect import Signature, signature
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeAlias, TypeVar, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.exceptions import MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations import AsyncMigrationTracker, SyncMigrationTracker, create_migration_commands
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
    from sqlspec.storage import StorageCapabilities


__all__ = (
    "ADKConfig",
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
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
    "PrometheusConfig",
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

ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")
DriverT = TypeVar("DriverT", bound="SyncDriverAdapterBase | AsyncDriverAdapterBase")

logger = get_logger("sqlspec.config")

DRIVER_FEATURE_LIFECYCLE_HOOKS: dict[str, str | None] = {
    "on_connection_create": "connection",
    "on_connection_destroy": "connection",
    "on_pool_create": "pool",
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
    """List of extension names to exclude from automatic migration inclusion.

    When an extension is configured in ``extension_config``, its migrations are automatically
    included. Use this to prevent that for specific extensions:

    Example:
        migration_config={
            "exclude_extensions": ["events"]  # Use ephemeral listen_notify, skip queue table
        }
    """

    transactional: NotRequired[bool]
    """Wrap migrations in transactions when supported. When enabled (default for adapters that support it), each migration runs in a transaction that is committed on success or rolled back on failure. This prevents partial migrations from leaving the database in an inconsistent state. Requires adapter support for transactional DDL. Defaults to True for PostgreSQL, SQLite, and DuckDB; False for MySQL, Oracle, and BigQuery. Individual migrations can override this with a '-- transactional: false' comment."""


class FlaskConfig(TypedDict):
    """Configuration options for Flask SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["flask"]:

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "flask": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        )

    Notes:
        This TypedDict provides type safety for extension config.
        Flask extension uses g object for request-scoped storage.
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


class StarletteConfig(TypedDict):
    """Configuration options for Starlette and FastAPI extensions.

    All fields are optional with sensible defaults. Use in extension_config["starlette"]:

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "starlette": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        )

    Notes:
        Both Starlette and FastAPI extensions use the "starlette" key.
        This TypedDict provides type safety for extension config.
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
    """Additional HTTP status codes that trigger commit. Default: set()

    Example:
        extra_commit_statuses={201, 202}
    """

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()

    Example:
        extra_rollback_statuses={409}
    """

    disable_di: NotRequired[bool]
    """Disable built-in dependency injection. Default: False.
    When True, the Starlette/FastAPI extension will not add middleware for managing
    database connections and sessions. Users are responsible for managing the
    database lifecycle manually via their own DI solution.
    """


class FastAPIConfig(StarletteConfig):
    """Configuration options for FastAPI SQLSpec extension.

    All fields are optional with sensible defaults. Use in extension_config["fastapi"]:

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "fastapi": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
    """


class ADKConfig(TypedDict):
    """Configuration options for ADK session and memory store extension.

    All fields are optional with sensible defaults. Use in extension_config["adk"]:

    Configuration supports three deployment scenarios:
    1. SQLSpec manages everything (runtime + migrations)
    2. SQLSpec runtime only (external migration tools like Alembic/Flyway)
    3. Selective features (sessions OR memory, not both)

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "adk": {
                    "session_table": "my_sessions",
                    "events_table": "my_events",
                    "memory_table": "my_memories",
                    "memory_use_fts": True,
                    "owner_id_column": "tenant_id INTEGER REFERENCES tenants(id)"
                }
            }
        )

    Notes:
        This TypedDict provides type safety for extension config but is not required.
        You can use plain dicts as well.
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
    """Name of the sessions table. Default: 'adk_sessions'

    Examples:
        "agent_sessions"
        "my_app_sessions"
        "tenant_acme_sessions"
    """

    events_table: NotRequired[str]
    """Name of the events table. Default: 'adk_events'

    Examples:
        "agent_events"
        "my_app_events"
        "tenant_acme_events"
    """

    memory_table: NotRequired[str]
    """Name of the memory entries table. Default: 'adk_memory_entries'

    Examples:
        "agent_memories"
        "my_app_memories"
        "tenant_acme_memories"
    """

    memory_use_fts: NotRequired[bool]
    """Enable full-text search when supported. Default: False.

    When True, adapters will use their native FTS capabilities where available:
    - PostgreSQL: to_tsvector/to_tsquery with GIN index
    - SQLite: FTS5 virtual table
    - DuckDB: FTS extension with match_bm25
    - Oracle: CONTAINS() with CTXSYS.CONTEXT index
    - BigQuery: SEARCH() function (requires search index)
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
    """Optional owner ID column definition to link sessions/memories to a user, tenant, team, or other entity.

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

    Examples:
        PostgreSQL with UUID FK:
            "account_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE"

        MySQL with BIGINT FK:
            "user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT"

        Oracle with NUMBER FK:
            "user_id NUMBER(10) REFERENCES users(id) ENABLE VALIDATE"

        SQLite with INTEGER FK:
            "tenant_id INTEGER NOT NULL REFERENCES tenants(id)"

        Nullable FK (optional relationship):
            "workspace_id UUID REFERENCES workspaces(id) ON DELETE SET NULL"

        No FK (just extra column):
            "organization_name VARCHAR(128) NOT NULL"

        Deferred constraint (PostgreSQL):
            "user_id UUID REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED"

    Notes:
        - Column name (first word) is extracted for INSERT/SELECT queries
        - Rest of definition is passed through to CREATE TABLE DDL
        - Database validates the DDL syntax (fail-fast on errors)
        - Works with all database dialects (PostgreSQL, MySQL, SQLite, Oracle, etc.)
    """

    in_memory: NotRequired[bool]
    """Enable in-memory table storage (Oracle-specific). Default: False.

    When enabled, tables are created with the INMEMORY clause for Oracle Database,
    which stores table data in columnar format in memory for faster query performance.

    This is an Oracle-specific feature that requires:
        - Oracle Database 12.1.0.2 or higher
        - Database In-Memory option license (Enterprise Edition)
        - Sufficient INMEMORY_SIZE configured in the database instance

    Other database adapters ignore this setting.

    Examples:
        Oracle with in-memory enabled:
            config = OracleAsyncConfig(
                connection_config={"dsn": "oracle://..."},
                extension_config={
                    "adk": {
                        "in_memory": True
                    }
                }
            )

    Notes:
        - Improves query performance for analytics (10-100x faster)
        - Tables created with INMEMORY clause
        - Requires Oracle Database In-Memory option license
        - Ignored by non-Oracle adapters
    """

    shard_count: NotRequired[int]
    """Optional hash shard count for session/event tables to reduce hotspotting.

    When set (>1), adapters that support computed shard columns will create a
    generated shard_id using MOD(FARM_FINGERPRINT(primary_key), shard_count) and
    include it in the primary key and filters. Ignored by adapters that do not
    support computed shards.
    """

    session_table_options: NotRequired[str]
    """Adapter-specific table OPTIONS/clauses for the sessions table.

    Passed verbatim when supported (e.g., Spanner columnar/tiered storage). Ignored by
    adapters without table OPTIONS support.
    """

    events_table_options: NotRequired[str]
    """Adapter-specific table OPTIONS/clauses for the events table."""

    memory_table_options: NotRequired[str]
    """Adapter-specific table OPTIONS/clauses for the memory table."""

    expires_index_options: NotRequired[str]
    """Adapter-specific options for the expires/index used in ADK stores."""


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
    """Enable Oracle INMEMORY clause for the queue table. Ignored by other adapters. Defaults to False.

    Note: To skip events migrations (e.g., when using ephemeral 'listen_notify' backend),
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
    | FlaskConfig
    | ADKConfig
    | EventsConfig
    | OpenTelemetryConfig
    | PrometheusConfig,
]


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    __slots__ = (
        "_migration_commands",
        "_migration_loader",
        "_observability_runtime",
        "_storage_capabilities",
        "bind_key",
        "connection_instance",
        "driver_features",
        "extension_config",
        "migration_config",
        "observability_config",
        "statement_config",
    )

    _migration_loader: "SQLFileLoader"
    _migration_commands: "SyncMigrationCommands[Any] | AsyncMigrationCommands[Any]"
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    requires_staging_for_load: "ClassVar[bool]" = False
    staging_protocols: "ClassVar[tuple[str, ...]]" = ()
    default_storage_profile: "ClassVar[str | None]" = None
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed",)
    bind_key: "str | None"
    statement_config: "StatementConfig"
    connection_instance: "PoolT | None"
    migration_config: "dict[str, Any] | MigrationConfig"
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

    def storage_capabilities(self) -> "StorageCapabilities":
        """Return cached storage capabilities for this configuration."""

        if self._storage_capabilities is None:
            self._storage_capabilities = self._build_storage_capabilities()
        return cast("StorageCapabilities", dict(self._storage_capabilities))

    def reset_storage_capabilities_cache(self) -> None:
        """Clear the cached capability snapshot."""

        self._storage_capabilities = None

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
        except (TypeError, ValueError):  # pragma: no cover - builtins without signatures
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
        assert self._observability_runtime is not None
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
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> "Awaitable[None] | None":
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest. Defaults to "head".
            allow_missing: Allow out-of-order migrations. Defaults to False.
            auto_sync: Auto-reconcile renamed migrations. Defaults to True.
            dry_run: Show what would be done without applying. Defaults to False.
        """
        raise NotImplementedError

    @abstractmethod
    def migrate_down(self, revision: str = "-1", *, dry_run: bool = False) -> "Awaitable[None] | None":
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations. Defaults to "-1".
            dry_run: Show what would be done without applying. Defaults to False.
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


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
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
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._ensure_extension_migrations()
        self._init_observability(observability_config)
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self._storage_capabilities = None
        self.driver_features.setdefault("storage_capabilities", self.storage_capabilities())
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

    def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: Allow out-of-order migrations.
            auto_sync: Auto-reconcile renamed migrations.
            dry_run: Show what would be done without applying.
        """
        commands = self._ensure_migration_commands()
        commands.upgrade(revision, allow_missing, auto_sync, dry_run)

    def migrate_down(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations.
            dry_run: Show what would be done without applying.
        """
        commands = self._ensure_migration_commands()
        commands.downgrade(revision, dry_run=dry_run)

    def get_current_migration(self, verbose: bool = False) -> "str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            Current migration version or None if no migrations applied.
        """
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        return commands.current(verbose=verbose)

    def create_migration(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        commands = self._ensure_migration_commands()
        commands.revision(message, file_type)

    def init_migrations(self, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = self._ensure_migration_commands()
        assert directory is not None
        commands.init(directory, package)

    def stamp_migration(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        commands = self._ensure_migration_commands()
        commands.stamp(revision)

    def fix_migrations(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        commands = self._ensure_migration_commands()
        commands.fix(dry_run, update_database, yes)


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
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
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._ensure_extension_migrations()
        self._init_observability(observability_config)
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}
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

    async def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: Allow out-of-order migrations.
            auto_sync: Auto-reconcile renamed migrations.
            dry_run: Show what would be done without applying.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.upgrade(revision, allow_missing, auto_sync, dry_run)

    async def migrate_down(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations.
            dry_run: Show what would be done without applying.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.downgrade(revision, dry_run=dry_run)

    async def get_current_migration(self, verbose: bool = False) -> "str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            Current migration version or None if no migrations applied.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        return await commands.current(verbose=verbose)

    async def create_migration(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.revision(message, file_type)

    async def init_migrations(self, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        assert directory is not None
        await commands.init(directory, package)

    async def stamp_migration(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.stamp(revision)

    async def fix_migrations(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.fix(dry_run, update_database, yes)


class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for sync database configurations with connection pooling."""

    __slots__ = ("connection_config",)
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

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
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._ensure_extension_migrations()
        self._init_observability(observability_config)
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="postgres", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self._storage_capabilities = None
        self.driver_features.setdefault("storage_capabilities", self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()

    def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.connection_instance is not None:
            return self.connection_instance
        self.connection_instance = self._create_pool()
        self.get_observability_runtime().emit_pool_create(self.connection_instance)
        return self.connection_instance

    def close_pool(self) -> None:
        """Close the connection pool."""
        pool = self.connection_instance
        self._close_pool()
        if pool is not None:
            self.get_observability_runtime().emit_pool_destroy(pool)
        self.connection_instance = None

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        return self.connection_instance

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

    @abstractmethod
    def _create_pool(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool(self) -> None:
        """Actual pool destruction implementation."""
        raise NotImplementedError

    def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: Allow out-of-order migrations.
            auto_sync: Auto-reconcile renamed migrations.
            dry_run: Show what would be done without applying.
        """
        commands = self._ensure_migration_commands()
        commands.upgrade(revision, allow_missing, auto_sync, dry_run)

    def migrate_down(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations.
            dry_run: Show what would be done without applying.
        """
        commands = self._ensure_migration_commands()
        commands.downgrade(revision, dry_run=dry_run)

    def get_current_migration(self, verbose: bool = False) -> "str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            Current migration version or None if no migrations applied.
        """
        commands = cast("SyncMigrationCommands[Any]", self._ensure_migration_commands())
        return commands.current(verbose=verbose)

    def create_migration(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        commands = self._ensure_migration_commands()
        commands.revision(message, file_type)

    def init_migrations(self, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = self._ensure_migration_commands()
        assert directory is not None
        commands.init(directory, package)

    def stamp_migration(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        commands = self._ensure_migration_commands()
        commands.stamp(revision)

    def fix_migrations(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        commands = self._ensure_migration_commands()
        commands.fix(dry_run, update_database, yes)


class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for async database configurations with connection pooling."""

    __slots__ = ("connection_config",)
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

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
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._ensure_extension_migrations()
        self._init_observability(observability_config)
        self._initialize_migration_components()

        if statement_config is None:
            self.statement_config = StatementConfig(
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
                dialect="postgres",
            )
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}
        self._storage_capabilities = None
        self.driver_features.setdefault("storage_capabilities", self.storage_capabilities())
        self._promote_driver_feature_hooks()
        self._configure_observability_extensions()

    async def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.connection_instance is not None:
            return self.connection_instance
        self.connection_instance = await self._create_pool()
        self.get_observability_runtime().emit_pool_create(self.connection_instance)
        return self.connection_instance

    async def close_pool(self) -> None:
        """Close the connection pool."""
        pool = self.connection_instance
        await self._close_pool()
        if pool is not None:
            self.get_observability_runtime().emit_pool_destroy(pool)
        self.connection_instance = None

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

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

    @abstractmethod
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError

    async def migrate_up(
        self, revision: str = "head", allow_missing: bool = False, auto_sync: bool = True, dry_run: bool = False
    ) -> None:
        """Apply database migrations up to specified revision.

        Args:
            revision: Target revision or "head" for latest.
            allow_missing: Allow out-of-order migrations.
            auto_sync: Auto-reconcile renamed migrations.
            dry_run: Show what would be done without applying.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.upgrade(revision, allow_missing, auto_sync, dry_run)

    async def migrate_down(self, revision: str = "-1", *, dry_run: bool = False) -> None:
        """Apply database migrations down to specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations.
            dry_run: Show what would be done without applying.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.downgrade(revision, dry_run=dry_run)

    async def get_current_migration(self, verbose: bool = False) -> "str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            Current migration version or None if no migrations applied.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        return await commands.current(verbose=verbose)

    async def create_migration(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py').
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.revision(message, file_type)

    async def init_migrations(self, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        if directory is None:
            migration_config = self.migration_config or {}
            directory = str(migration_config.get("script_location") or "migrations")

        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        assert directory is not None
        await commands.init(directory, package)

    async def stamp_migration(self, revision: str) -> None:
        """Mark database as being at a specific revision without running migrations.

        Args:
            revision: The revision to stamp.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.stamp(revision)

    async def fix_migrations(self, dry_run: bool = False, update_database: bool = True, yes: bool = False) -> None:
        """Convert timestamp migrations to sequential format.

        Args:
            dry_run: Preview changes without applying.
            update_database: Update migration records in database.
            yes: Skip confirmation prompt.
        """
        commands = cast("AsyncMigrationCommands[Any]", self._ensure_migration_commands())
        await commands.fix(dry_run, update_database, yes)
