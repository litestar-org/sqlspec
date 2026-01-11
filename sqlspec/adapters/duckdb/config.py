"""DuckDB database configuration with connection pooling."""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.adapters.duckdb.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
)
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, DuckDBExceptionHandler, DuckDBSessionContext
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.observability import ObservabilityConfig
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
__all__ = (
    "DuckDBConfig",
    "DuckDBConnectionParams",
    "DuckDBDriverFeatures",
    "DuckDBExtensionConfig",
    "DuckDBPoolParams",
    "DuckDBSecretConfig",
)
EXTENSION_FLAG_KEYS: "tuple[str, ...]" = (
    "allow_community_extensions",
    "allow_unsigned_extensions",
    "enable_external_access",
)


class DuckDBConnectionParams(TypedDict):
    """DuckDB connection parameters.

    Mirrors the keyword arguments accepted by duckdb.connect so callers can drive every DuckDB
    configuration switch directly through SQLSpec. All keys are optional and forwarded verbatim
    to DuckDB, either as top-level parameters or via the nested ``config`` dictionary when DuckDB
    expects them there.
    """

    database: NotRequired[str]
    read_only: NotRequired[bool]
    config: NotRequired["dict[str, Any]"]
    memory_limit: NotRequired[str]
    threads: NotRequired[int]
    temp_directory: NotRequired[str]
    max_temp_directory_size: NotRequired[str]
    autoload_known_extensions: NotRequired[bool]
    autoinstall_known_extensions: NotRequired[bool]
    allow_community_extensions: NotRequired[bool]
    allow_unsigned_extensions: NotRequired[bool]
    extension_directory: NotRequired[str]
    custom_extension_repository: NotRequired[str]
    autoinstall_extension_repository: NotRequired[str]
    allow_persistent_secrets: NotRequired[bool]
    enable_external_access: NotRequired[bool]
    secret_directory: NotRequired[str]
    enable_object_cache: NotRequired[bool]
    parquet_metadata_cache: NotRequired[str]
    enable_external_file_cache: NotRequired[bool]
    checkpoint_threshold: NotRequired[str]
    enable_progress_bar: NotRequired[bool]
    progress_bar_time: NotRequired[float]
    enable_logging: NotRequired[bool]
    log_query_path: NotRequired[str]
    logging_level: NotRequired[str]
    preserve_insertion_order: NotRequired[bool]
    default_null_order: NotRequired[str]
    default_order: NotRequired[str]
    ieee_floating_point_ops: NotRequired[bool]
    binary_as_string: NotRequired[bool]
    arrow_large_buffer_size: NotRequired[bool]
    errors_as_json: NotRequired[bool]
    extra: NotRequired["dict[str, Any]"]


class DuckDBPoolParams(DuckDBConnectionParams):
    """Complete pool configuration for DuckDB adapter.

    Extends DuckDBConnectionParams with pool sizing and lifecycle settings so SQLSpec can manage
    per-thread DuckDB connections safely while honoring DuckDB's thread-safety constraints.
    """

    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]


class DuckDBExtensionConfig(TypedDict):
    """DuckDB extension configuration for auto-management."""

    name: str
    """Name of the extension to install/load."""

    version: NotRequired[str]
    """Specific version of the extension."""

    repository: NotRequired[str]
    """Repository for the extension (core, community, or custom URL)."""

    force_install: NotRequired[bool]
    """Force reinstallation of the extension."""


class DuckDBSecretConfig(TypedDict):
    """DuckDB secret configuration for AI/API integrations."""

    secret_type: str
    """Type of secret (e.g., 'openai', 'aws', 'azure', 'gcp')."""

    name: str
    """Name of the secret."""

    value: "dict[str, Any]"
    """Secret configuration values."""

    scope: NotRequired[str]
    """Scope of the secret (LOCAL or PERSISTENT)."""


class DuckDBDriverFeatures(TypedDict):
    """TypedDict for DuckDB driver features configuration.

    Attributes:
        extensions: List of extensions to install/load on connection creation.
        secrets: List of secrets to create for AI/API integrations.
        on_connection_create: Callback executed when connection is created.
        json_serializer: Custom JSON serializer for dict/list parameter conversion.
            Defaults to sqlspec.utils.serializers.to_json if not provided.
        enable_uuid_conversion: Enable automatic UUID string conversion.
            When True (default), UUID strings are automatically converted to UUID objects.
            When False, UUID strings are treated as regular strings.
        extension_flags: Connection-level flags (e.g., allow_community_extensions) applied
            via SET statements immediately after connection creation.
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
            Provides pub/sub capabilities via table-backed queue (DuckDB has no native pub/sub).
            Requires extension_config["events"] for migration setup.
        events_backend: Event channel backend selection.
            Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
            DuckDB does not have native pub/sub, so table_queue is the only backend.
            Defaults to "table_queue".
    """

    extensions: NotRequired[Sequence[DuckDBExtensionConfig]]
    secrets: NotRequired[Sequence[DuckDBSecretConfig]]
    on_connection_create: NotRequired["Callable[[DuckDBConnection], DuckDBConnection | None]"]
    json_serializer: NotRequired["Callable[[Any], str]"]
    enable_uuid_conversion: NotRequired[bool]
    extension_flags: NotRequired["dict[str, Any]"]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class DuckDBConnectionContext:
    """Context manager for DuckDB connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "DuckDBConfig") -> None:
        self._config = config
        self._ctx: Any = None

    def __enter__(self) -> DuckDBConnection:
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return cast("DuckDBConnection", self._ctx.__enter__())

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))
        return None


class _DuckDBConnectionHook:
    __slots__ = ("_hook",)

    def __init__(self, hook: "Callable[[Any], None]") -> None:
        self._hook = hook

    def __call__(self, context: "dict[str, Any]") -> None:
        connection = context.get("connection")
        if connection is None:
            return
        self._hook(connection)


class _DuckDBSessionConnectionHandler:
    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "DuckDBConfig") -> None:
        self._config = config
        self._ctx: Any = None

    def acquire_connection(self) -> "DuckDBConnection":
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return cast("DuckDBConnection", self._ctx.__enter__())

    def release_connection(self, _conn: "DuckDBConnection") -> None:
        if self._ctx is None:
            return
        self._ctx.__exit__(None, None, None)
        self._ctx = None


class DuckDBConfig(SyncDatabaseConfig[DuckDBConnection, DuckDBConnectionPool, DuckDBDriver]):
    """DuckDB configuration with connection pooling.

    This configuration supports DuckDB's features including:

    - Connection pooling
    - Extension management and installation
    - Secret management for API integrations
    - Auto configuration settings
    - Arrow integration
    - Direct file querying capabilities
    - Configurable type handlers for JSON serialization and UUID conversion

    DuckDB Connection Pool Configuration:
    - Default pool size is 1-4 connections (DuckDB uses single connection by default)
    - Connection recycling is set to 24 hours by default (set to 0 to disable)
    - Shared memory databases use `:memory:shared_db` for proper concurrency

    Type Handler Configuration via driver_features:
    - `json_serializer`: Custom JSON serializer for dict/list parameters.
      Defaults to `sqlspec.utils.serializers.to_json` if not provided.
      Example: `json_serializer=msgspec.json.encode(...).decode('utf-8')`

    - `enable_uuid_conversion`: Enable automatic UUID string conversion (default: True).
      When True, UUID strings in query results are automatically converted to UUID objects.
      When False, UUID strings are treated as regular strings.

    Example:
        >>> import msgspec
        >>> from sqlspec.adapters.duckdb import DuckDBConfig
        >>>
        >>> # Custom JSON serializer
        >>> def custom_json(obj):
        ...     return msgspec.json.encode(obj).decode("utf-8")
        >>>
        >>> config = DuckDBConfig(
        ...     connection_config={"database": ":memory:"},
        ...     driver_features={
        ...         "json_serializer": custom_json,
        ...         "enable_uuid_conversion": False,
        ...     },
        ... )
    """

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed", "rows_per_chunk", "manifest")

    def __init__(
        self,
        *,
        connection_config: "DuckDBPoolParams | dict[str, Any] | None" = None,
        connection_instance: "DuckDBConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "DuckDBDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize DuckDB configuration.

        Args:
            connection_config: Connection and pool configuration parameters
            connection_instance: Pre-created pool instance
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: DuckDB-specific driver features including json_serializer
                and enable_uuid_conversion options
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        connection_config = normalize_connection_config(connection_config)
        connection_config.setdefault("database", ":memory:shared_db")

        if connection_config.get("database") in {":memory:", ""}:
            connection_config["database"] = ":memory:shared_db"

        extension_flags: dict[str, Any] = {}
        for key in tuple(connection_config.keys()):
            if key in EXTENSION_FLAG_KEYS:
                extension_flags[key] = connection_config.pop(key)

        features: dict[str, Any] = dict(driver_features) if driver_features else {}
        user_connection_hook = cast("Callable[[Any], None] | None", features.pop("on_connection_create", None))
        features.setdefault("enable_uuid_conversion", True)
        serializer = features.setdefault("json_serializer", to_json)

        if extension_flags:
            existing_flags = cast("dict[str, Any]", features.get("extension_flags", {}))
            merged_flags = {**existing_flags, **extension_flags}
            features["extension_flags"] = merged_flags

        local_observability = observability_config
        if user_connection_hook is not None:
            lifecycle_override = ObservabilityConfig(
                lifecycle={"on_connection_create": [_DuckDBConnectionHook(user_connection_hook)]}
            )
            local_observability = ObservabilityConfig.merge(local_observability, lifecycle_override)

        statement_config = statement_config or build_statement_config(
            json_serializer=cast("Callable[[Any], str]", serializer)
        )
        statement_config = apply_driver_features(statement_config, features)

        super().__init__(
            bind_key=bind_key,
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=local_observability,
            **kwargs,
        )

    def _create_pool(self) -> DuckDBConnectionPool:
        """Create connection pool from configuration."""
        connection_config = build_connection_config(self.connection_config)

        extensions = self.driver_features.get("extensions", None)
        secrets = self.driver_features.get("secrets", None)
        extension_flags = self.driver_features.get("extension_flags", None)
        extensions_dicts = [dict(ext) for ext in extensions] if extensions else None
        secrets_dicts = [dict(secret) for secret in secrets] if secrets else None
        extension_flags_dict = dict(extension_flags) if extension_flags else None

        pool_recycle_seconds = self.connection_config.get("pool_recycle_seconds")
        health_check_interval = self.connection_config.get("health_check_interval")
        pool_kwargs: dict[str, Any] = {}
        if pool_recycle_seconds is not None:
            pool_kwargs["pool_recycle_seconds"] = pool_recycle_seconds
        if health_check_interval is not None:
            pool_kwargs["health_check_interval"] = health_check_interval

        return DuckDBConnectionPool(
            connection_config=connection_config,
            extensions=extensions_dicts,
            extension_flags=extension_flags_dict,
            secrets=secrets_dicts,
            **pool_kwargs,
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance:
            self.connection_instance.close()

    def create_connection(self) -> DuckDBConnection:
        """Get a DuckDB connection from the pool.

        This method ensures the pool is created and returns a connection
        from the pool. The connection is checked out from the pool and must
        be properly managed by the caller.

        Returns:
            DuckDBConnection: A connection from the pool

        Note:
            For automatic connection management, prefer using provide_connection()
            or provide_session() which handle returning connections to the pool.
            The caller is responsible for returning the connection to the pool
            using pool.release(connection) when done.
        """
        pool = self.provide_pool()

        return pool.acquire()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "DuckDBConnectionContext":
        """Provide a pooled DuckDB connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A DuckDB connection context manager.
        """
        return DuckDBConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "DuckDBSessionContext":
        """Provide a DuckDB driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A DuckDB driver session context manager.
        """
        handler = _DuckDBSessionConnectionHandler(self)

        return DuckDBSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for DuckDB types.

        This provides all DuckDB-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "DuckDBConnectionContext": DuckDBConnectionContext,
            "DuckDBConnection": DuckDBConnection,
            "DuckDBConnectionParams": DuckDBConnectionParams,
            "DuckDBConnectionPool": DuckDBConnectionPool,
            "DuckDBCursor": DuckDBCursor,
            "DuckDBDriver": DuckDBDriver,
            "DuckDBDriverFeatures": DuckDBDriverFeatures,
            "DuckDBExceptionHandler": DuckDBExceptionHandler,
            "DuckDBExtensionConfig": DuckDBExtensionConfig,
            "DuckDBPoolParams": DuckDBPoolParams,
            "DuckDBSecretConfig": DuckDBSecretConfig,
            "DuckDBSessionContext": DuckDBSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults optimized for DuckDB."""

        return EventRuntimeHints(poll_interval=0.15, lease_seconds=15)
