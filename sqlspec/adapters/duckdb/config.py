"""DuckDB database configuration with connection pooling."""

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.duckdb._typing import DuckDBConnection, DuckDBCursor, DuckDBSessionContext
from sqlspec.adapters.duckdb.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
)
from sqlspec.adapters.duckdb.driver import DuckDBDriver, DuckDBExceptionHandler
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig
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
    """DuckDB connection parameters forwarded to ``duckdb.connect`` or its ``config`` dictionary."""

    database: NotRequired[str]
    """Database file path, ``':memory:shared_db'`` shared memory identifier, or MotherDuck URI."""
    read_only: NotRequired[bool]
    """Open the database file in read-only mode without taking a write lock."""
    config: NotRequired["dict[str, Any]"]
    """Raw DuckDB engine configuration dictionary merged with modeled settings."""
    memory_limit: NotRequired[str]
    """Maximum buffer-manager memory ceiling (for example ``'4GB'``)."""
    memory_ceiling_fraction: NotRequired[float]
    """Fraction of container or host memory used as an upper ceiling when ``memory_limit`` is unset or exceeds available memory (defaults to ``0.8``)."""
    threads: NotRequired[int]
    """Number of DuckDB execution worker threads."""
    temp_directory: NotRequired[str]
    """Directory path used when spilling out-of-core intermediates to disk."""
    reject_tmpfs_temp_directory: NotRequired[bool]
    """Raise ``ImproperConfigurationError`` when ``temp_directory`` resides on an in-memory ``tmpfs`` or ``ramfs`` mount (defaults to ``False``)."""
    max_temp_directory_size: NotRequired[str]
    """Maximum disk space cap for ``temp_directory`` spill files (for example ``'20GB'``)."""
    autoload_known_extensions: NotRequired[bool]
    """Automatically load installed DuckDB extensions when referenced by SQL syntax."""
    autoinstall_known_extensions: NotRequired[bool]
    """Automatically download and install known DuckDB extensions on first reference."""
    allow_community_extensions: NotRequired[bool]
    """Permit installing extensions from the DuckDB Community Extensions repository."""
    allow_unsigned_extensions: NotRequired[bool]
    """Permit loading unsigned local or third-party DuckDB extension binaries."""
    extension_directory: NotRequired[str]
    """Custom filesystem directory where DuckDB stores downloaded extensions."""
    custom_extension_repository: NotRequired[str]
    """Custom HTTP or local repository URL searched when installing extensions."""
    autoinstall_extension_repository: NotRequired[str]
    """Repository URL used specifically for automatic on-demand extension installs."""
    allow_persistent_secrets: NotRequired[bool]
    """Allow reading and writing persistent secrets on disk."""
    enable_external_access: NotRequired[bool]
    """Allow SQL statements to access local files, HTTP URLs, and object stores."""
    secret_directory: NotRequired[str]
    """Filesystem directory storing persistent DuckDB secret files."""
    enable_object_cache: NotRequired[bool]
    """Cache parsed Parquet metadata and object-store footers in memory."""
    parquet_metadata_cache: NotRequired[bool]
    """Enable caching of Parquet file footers across repeated scans."""
    enable_external_file_cache: NotRequired[bool]
    """Cache remote HTTP and object-store byte ranges in DuckDB's buffer pool."""
    checkpoint_threshold: NotRequired[str]
    """WAL size threshold triggering an automatic database checkpoint (for example ``'16MB'``)."""
    enable_progress_bar: NotRequired[bool]
    """Display interactive terminal progress bars during long-running queries."""
    progress_bar_time: NotRequired[int]
    """Milliseconds elapsed before DuckDB displays the query progress bar."""
    enable_logging: NotRequired[bool]
    """Enable DuckDB internal query and engine logging."""
    log_query_path: NotRequired[str]
    """Filesystem path receiving logged SQL queries when logging is enabled."""
    logging_level: NotRequired[str]
    """Minimum severity level recorded by DuckDB's internal logger."""
    preserve_insertion_order: NotRequired[bool]
    """Preserve input row ordering during table scans and bulk operations."""
    default_null_order: NotRequired[str]
    """Default ``NULLS FIRST`` or ``NULLS LAST`` ordering when ``ORDER BY`` omits null placement."""
    default_order: NotRequired[str]
    """Default ``ASC`` or ``DESC`` sort direction when ``ORDER BY`` omits direction."""
    ieee_floating_point_ops: NotRequired[bool]
    """Follow IEEE-754 floating-point division-by-zero semantics instead of returning ``NULL``."""
    binary_as_string: NotRequired[bool]
    """Interpret untyped Parquet binary columns as ``VARCHAR`` strings."""
    arrow_large_buffer_size: NotRequired[bool]
    """Emit 64-bit ``LargeUtf8`` and ``LargeBinary`` Arrow buffers on Arrow exports."""
    errors_as_json: NotRequired[bool]
    """Format DuckDB exception payloads as structured JSON strings."""
    extra: NotRequired["dict[str, Any]"]
    """Additional DuckDB configuration options merged into ``config``."""


class DuckDBPoolParams(DuckDBConnectionParams):
    """Complete pool configuration for DuckDB adapter.

    Extends DuckDBConnectionParams with lifecycle settings consumed by SQLSpec's
    thread-local DuckDB connection manager.
    """

    pool_recycle_seconds: NotRequired[int]
    """Maximum connection lifetime in seconds before recycling (defaults to ``86400``; ``0`` disables)."""
    health_check_interval: NotRequired[float]
    """Idle seconds between lightweight connection health checks."""


class DuckDBExtensionConfig(TypedDict):
    """DuckDB extension configuration for auto-management."""

    name: str
    """Name of the extension to install/load."""

    version: NotRequired[str]
    """Specific version of the extension."""

    repository: NotRequired[str]
    """Repository for the extension (core, community, or custom URL)."""

    repository_url: NotRequired[str]
    """Custom repository URL for the extension."""

    force_install: NotRequired[bool]
    """Force reinstallation of the extension."""

    install: NotRequired[bool]
    """Force an explicit install_extension() call even for a name-only config."""

    required: NotRequired[bool]
    """When True, install/load failure raises instead of best-effort WARNING."""

    storage_protocols: NotRequired[Sequence[str]]
    """Additional URI schemes handled by this extension's configured filesystem.

    Addresses using these schemes are passed unchanged to DuckDB after the
    extension loads. DuckDB owns credential selection and reports transfer
    errors. Registered Python filesystems are discovered automatically.
    """


class DuckDBSecretConfig(TypedDict):
    """DuckDB secret configuration for AI/API integrations."""

    secret_type: str
    """Type of secret."""

    name: str
    """Name of the secret. DuckDB matches secret names case-insensitively."""

    value: NotRequired["dict[str, Any]"]
    """Secret configuration values, applied only when the secret is created or ``replace`` is set."""

    value_provider: NotRequired["Callable[[], dict[str, Any]]"]
    """Callable returning dynamic secret configuration values (such as short-lived OAuth or STS tokens)."""

    refresh_interval_seconds: NotRequired[float]
    """Interval in seconds after which a secret backed by ``value_provider`` is refreshed via ``CREATE OR REPLACE SECRET`` (defaults to ``1800.0`` when ``value_provider`` is set)."""

    provider: NotRequired[str]
    """Secret provider, such as config or credential_chain."""

    scope: NotRequired[str]
    """Optional path or storage-prefix scope for the secret."""

    persistent: NotRequired[bool]
    """Persist the secret to DuckDB's configured secret directory."""

    replace: NotRequired[bool]
    """When True, overwrite an existing secret of the same name, such as after rotating credentials. Default False."""

    required: NotRequired[bool]
    """When True, raise instead of skipping the secret with a warning. Default False.

    Failures are a creation error, a secret that is not visible after creation, and an
    existing secret whose type, provider, declared scope or unredacted declared settings
    such as ``key_id``, ``region`` or ``endpoint`` differ from the declaration.
    """


class DuckDBDriverFeatures(TypedDict):
    """DuckDB driver feature flags controlling extension/secret management, codecs, and statement profiling."""

    extensions: NotRequired[Sequence[DuckDBExtensionConfig]]
    """Extensions installed and loaded automatically when each pooled connection opens."""
    secrets: NotRequired[Sequence[DuckDBSecretConfig]]
    """Secrets provisioned on new connections for object stores and external catalogs."""
    on_connection_create: NotRequired["Callable[[DuckDBConnection], DuckDBConnection | None]"]
    """Callback invoked when a new ``DuckDBPyConnection`` is opened."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Serializer converting dict/list parameters to JSON text (defaults to ``sqlspec.utils.serializers.to_json``)."""
    enable_uuid_conversion: NotRequired[bool]
    """Convert ``UUID`` parameters to strings on bind while returning ``UUID`` objects from ``UUID`` columns (defaults to ``True``)."""
    extension_flags: NotRequired["dict[str, Any]"]
    """Startup-only extension security flags merged into ``connection_config`` before ``duckdb.connect``."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec table-backed event channel support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event queue backend name (``'poll_queue'``)."""
    enable_statement_profiling: NotRequired[bool]
    """Capture per-statement DuckDB JSON operator profiles on ``StatementEvent.profile`` (defaults to ``False``)."""


class DuckDBConnectionContext(SyncPoolConnectionContext):
    """Context manager for DuckDB connections."""

    __slots__ = ()


class _DuckDBSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class DuckDBConfig(SyncDatabaseConfig[DuckDBConnection, DuckDBConnectionPool, DuckDBDriver]):
    """DuckDB configuration with connection pooling.

    Supports extensions, secrets, Arrow transfers, and direct file queries.
    Connections recycle after 24 hours by default; set recycling to 0 to disable.
    Shared memory databases use ``:memory:shared_db``.

    Set ``driver_features["json_serializer"]`` to a serializer that returns text
    for dict/list parameters. The default is ``sqlspec.utils.serializers.to_json``.

    ``driver_features["enable_uuid_conversion"]`` converts UUID parameters to
    strings by default. Disable it to pass UUID parameters through unchanged.
    UUID result columns always return UUID objects; VARCHAR columns remain strings.
    """

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed", "rows_per_chunk", "manifest")
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
    _connection_context_class: "ClassVar[type[DuckDBConnectionContext]]" = DuckDBConnectionContext
    _session_factory_class: "ClassVar[type[_DuckDBSessionConnectionHandler]]" = _DuckDBSessionConnectionHandler
    _session_context_class: "ClassVar[type[DuckDBSessionContext]]" = DuckDBSessionContext
    _default_statement_config = default_statement_config

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
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        connection_config = normalize_connection_config(connection_config)
        database = (
            connection_config.pop("database", None)
            or connection_config.pop("db", None)
            or connection_config.pop("path", None)
            or connection_config.pop("file", None)
        )
        if database is not None:
            connection_config["database"] = database
        connection_config.setdefault("database", ":memory:shared_db")

        if connection_config.get("database") in {":memory:", ""}:
            connection_config["database"] = ":memory:shared_db"

        features: dict[str, Any] = dict(driver_features) if driver_features else {}
        self._user_connection_hook = cast(
            "Callable[[DuckDBConnection], DuckDBConnection | None] | None", features.pop("on_connection_create", None)
        )
        features.setdefault("enable_uuid_conversion", True)
        serializer = features.setdefault("json_serializer", to_json)

        feature_flags = cast("dict[str, Any]", features.get("extension_flags", {}))
        for key, value in feature_flags.items():
            connection_config.setdefault(key, value)
        declared_flags = {key: connection_config[key] for key in EXTENSION_FLAG_KEYS if key in connection_config}
        if feature_flags or declared_flags:
            features["extension_flags"] = {**feature_flags, **declared_flags}

        statement_config = statement_config or build_statement_config(
            json_serializer=cast("Callable[[Any], str]", serializer)
        )
        statement_config, features = apply_driver_features(statement_config, features)

        super().__init__(
            bind_key=bind_key,
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> DuckDBConnection:
        """Open a standalone connection owned by the caller.

        The connection carries the same settings, extensions, and secrets the
        pool applies, but it is not the pool's thread-local connection, so
        closing it leaves the pool usable.

        Returns:
            DuckDBConnection: A newly opened connection.
        """
        return self.provide_pool().new_connection()

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

    def _create_pool(self) -> DuckDBConnectionPool:
        """Create connection pool from configuration."""
        connection_config = build_connection_config(self.connection_config)

        extensions = cast("list[dict[str, Any]] | None", self.driver_features.get("extensions", None))
        secrets = cast("list[dict[str, Any]] | None", self.driver_features.get("secrets", None))
        extensions_dicts = [dict(ext) for ext in extensions] if extensions else None
        secrets_dicts = [dict(secret) for secret in secrets] if secrets else None

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
            secrets=secrets_dicts,
            on_connection_create=self._user_connection_hook,
            enable_statement_profiling=bool(self.driver_features.get("enable_statement_profiling", False)),
            **pool_kwargs,
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance:
            self.connection_instance.close()

    def _prepare_driver(self, driver: DuckDBDriver) -> DuckDBDriver:
        driver = super()._prepare_driver(driver)
        if self.connection_instance is not None:
            driver.driver_features.update(self.connection_instance._storage_settings(driver.connection))
        return driver
