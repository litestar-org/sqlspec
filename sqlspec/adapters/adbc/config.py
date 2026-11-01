"""ADBC database configuration."""

import contextlib
import importlib.metadata
import math
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, Self, TypedDict, cast

from sqlspec.adapters.adbc._typing import AdbcConnection, AdbcCursor, AdbcSessionContext
from sqlspec.adapters.adbc.core import (
    apply_driver_features,
    build_connection_config,
    cleanup_duckdb_profile_path,
    configure_adbc_duckdb_profiling,
    create_duckdb_profile_path,
    detect_postgres_extensions,
    get_statement_config,
    is_postgres_dialect,
    normalize_connection_aliases,
    resolve_dialect_from_config,
    resolve_dialect_name,
    resolve_driver_connect_func,
    resolve_driver_name_from_config,
)
from sqlspec.adapters.adbc.driver import AdbcDriver, AdbcExceptionHandler
from sqlspec.adapters.adbc.pool import AdbcConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import StatementConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import DatabaseConnectionError, ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.observability import ObservabilityConfig


__all__ = ("AdbcConfig", "AdbcConnectionParams", "AdbcDriverFeatures", "AdbcPoolConfig")


class AdbcPoolConfig(TypedDict):
    """Optional PostgreSQL clone-pool bounds; an empty mapping enables defaults."""

    max_size: NotRequired[int]
    """Maximum simultaneous clone leases, excluding the original seed (default 4)."""
    timeout: NotRequired[float]
    """Maximum seconds to wait for a lease (default 30.0)."""


class AdbcConnectionParams(TypedDict):
    """ADBC connection parameters across PyPI-installable Arrow Database Connectivity drivers."""

    uri: NotRequired[str]
    """Backend connection URI (such as ``postgresql://``, ``sqlite://``, ``duckdb://``, ``grpc+tls://``, ``oracle://``, ``db2://``, or ``quack://``)."""
    dsn: NotRequired[str]
    """Connection string alias normalized to ``uri`` when ``uri`` is unset."""
    url: NotRequired[str]
    """Connection URL alias normalized to ``uri`` when ``uri`` is unset."""
    connection_string: NotRequired[str]
    """Connection string alias normalized to ``uri`` when ``uri`` is unset."""
    db: NotRequired[str]
    """Database path or identifier alias normalized for file-backed and embedded ADBC drivers."""
    path: NotRequired[str]
    """Filesystem database path for ``adbc-driver-sqlite`` or ``adbc-driver-duckdb``."""
    driver: NotRequired[str]
    """Short driver alias (such as ``'postgresql'``, ``'sqlite'``, ``'duckdb'``, ``'flightsql'``, ``'gizmosql'``, ``'snowflake'``, ``'bigquery'``, ``'oracle'``, ``'db2'``, or ``'quack'``) or shared library path."""
    driver_name: NotRequired[str]
    """Explicit ADBC Python module connect path or shared library name overriding URI inference."""
    db_kwargs: NotRequired[dict[str, Any]]
    """Driver-specific ``adbc_driver_manager.AdbcDatabase`` options passed to ``dbapi.connect``."""
    conn_kwargs: NotRequired[dict[str, Any]]
    """Driver-specific ``adbc_driver_manager.AdbcConnection`` options passed to ``dbapi.connect``."""
    entrypoint: NotRequired[str]
    """C symbol entrypoint name when loading a shared-object ADBC driver directly."""
    profile: NotRequired[str]
    """Named credential or configuration profile forwarded to cloud ADBC drivers."""
    auth_type: NotRequired[str]
    """Authentication mechanism (for example ``'external'`` OAuth/SSO on ``adbc-driver-gizmosql`` or Snowflake auth modes)."""
    adbc_driver_manager_entrypoint: NotRequired[str]
    """Explicit ``adbc_driver_manager`` C initialization entrypoint symbol."""
    autocommit: NotRequired[bool]
    """Enable autocommit mode on newly opened DB-API ADBC connections."""
    isolation_level: NotRequired[str]
    """Transaction isolation level applied to the ADBC connection."""
    batch_size: NotRequired[int]
    """Target Arrow ``RecordBatch`` row count during streaming reads and ingests."""
    query_timeout: NotRequired[float]
    """Per-statement execution timeout in seconds forwarded to supporting ADBC backends."""
    connection_timeout: NotRequired[float]
    """Connection establishment timeout in seconds."""
    ssl_mode: NotRequired[str]
    """TLS verification mode forwarded to PostgreSQL and compatible ADBC drivers."""
    ssl_cert: NotRequired[str]
    """Client TLS certificate path for mutual TLS connections."""
    ssl_key: NotRequired[str]
    """Client TLS private key path paired with ``ssl_cert``."""
    ssl_ca: NotRequired[str]
    """Root CA bundle path used to verify server TLS certificates."""
    username: NotRequired[str]
    """Database authentication username lifted into ``db_kwargs`` or ``connect()`` as required by the driver."""
    password: NotRequired[str]
    """Database authentication password lifted into ``db_kwargs`` or ``connect()`` as required by the driver."""
    token: NotRequired[str]
    """Bearer, OAuth refresh, or Quack/FlightSQL authentication token."""
    auth_token_provider: NotRequired[Callable[[], str]]
    """Callable returning a fresh password or bearer token on each ``create_connection`` call."""
    project_id: NotRequired[str]
    """Google Cloud project ID mapped to ``adbc.bigquery.sql.project_id`` for ``adbc-driver-bigquery``."""
    dataset_id: NotRequired[str]
    """Default BigQuery dataset ID mapped to ``adbc.bigquery.sql.dataset_id``."""
    account: NotRequired[str]
    """Snowflake account identifier for ``adbc-driver-snowflake``."""
    warehouse: NotRequired[str]
    """Snowflake virtual warehouse name activated on connect."""
    database: NotRequired[str]
    """Target catalog or database name for Snowflake, PostgreSQL, or FlightSQL backends."""
    schema: NotRequired[str]
    """Default schema name for Snowflake or multi-schema ADBC backends."""
    role: NotRequired[str]
    """Session role activated on connect for Snowflake or role-aware backends."""
    authorization_header: NotRequired[str]
    """Explicit HTTP/gRPC ``Authorization`` header value for FlightSQL and GizmoSQL servers."""
    grpc_options: NotRequired[dict[str, Any]]
    """Low-level gRPC channel options forwarded to FlightSQL, GizmoSQL, or Quack transports."""
    grpc_max_message_size: NotRequired[int]
    """Maximum inbound gRPC message size in bytes for Arrow Flight transfers."""
    fetch_timeout: NotRequired[float]
    """Timeout in seconds when fetching Arrow Flight result streams."""
    gizmosql_backend: NotRequired[str]
    """Underlying SQL engine dialect hint when connecting through ``adbc-driver-gizmosql`` or ``adbc-driver-flightsql``."""
    tls_skip_verify: NotRequired[bool]
    """Disable server certificate verification on TLS-enabled FlightSQL or GizmoSQL endpoints."""
    tls_root_certs: NotRequired[str]
    """PEM-encoded root certificate bundle for FlightSQL or GizmoSQL TLS verification."""
    mtls_cert_chain: NotRequired[str]
    """PEM-encoded client certificate chain for FlightSQL or GizmoSQL mTLS."""
    mtls_private_key: NotRequired[str]
    """PEM-encoded client private key paired with ``mtls_cert_chain``."""
    with_cookie_middleware: NotRequired[bool]
    """Enable stateful session cookie middleware on FlightSQL/GizmoSQL connections."""
    headers: NotRequired[dict[str, str]]
    """Custom gRPC metadata headers attached to FlightSQL or GizmoSQL requests."""
    extra: NotRequired[dict[str, Any]]
    """Additional driver options merged into ``db_kwargs``."""


class AdbcDriverFeatures(TypedDict):
    """ADBC driver feature flags controlling Arrow type handling, dialect probes, and DuckDB profiling."""

    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Serializer converting dict/list parameters to JSON text (defaults to ``sqlspec.utils.serializers.to_json``)."""
    enable_cast_detection: NotRequired[bool]
    """Detect explicit SQL casts such as ``::jsonb`` or ``::vector`` during parameter preparation (defaults to ``True``)."""
    enable_strict_type_coercion: NotRequired[bool]
    """Raise an error on unsupported parameter types instead of best-effort conversion (defaults to ``False``)."""
    enable_arrow_extension_types: NotRequired[bool]
    """Preserve PyArrow extension type metadata when materializing Arrow results (defaults to ``True``)."""
    enable_pgvector: NotRequired[bool]
    """Probe ``pg_extension`` for ``vector`` on PostgreSQL ADBC connections (defaults to ``True`` when ``pgvector`` is installed)."""
    enable_paradedb: NotRequired[bool]
    """Probe ``pg_extension`` for ParadeDB ``pg_search`` on PostgreSQL ADBC connections (defaults to ``True``)."""
    enable_pg_textsearch: NotRequired[bool]
    """Probe ``pg_extension`` for ``pg_textsearch`` BM25 support on PostgreSQL ADBC connections (defaults to ``True``)."""
    enable_lowercase_column_names: NotRequired[bool]
    """Normalize unquoted uppercase column names to lowercase (defaults to ``True`` on Oracle, Db2, and Snowflake ADBC backends)."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec table-backed event channel support."""
    on_connection_create: "NotRequired[Callable[[AdbcConnection], None]]"
    """Callback invoked with the raw ADBC connection immediately after ``connect()``."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event queue backend name (``'poll_queue'``)."""
    enable_statement_profiling: NotRequired[bool]
    """Capture per-statement DuckDB JSON operator profiles on ``StatementEvent.profile`` when using DuckDB ADBC (defaults to ``False``)."""


class AdbcConnectionContext(SyncPoolConnectionContext):
    """Context manager for ADBC connections."""

    __slots__ = ("_connection",)

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._connection: AdbcConnection | None = None

    def __enter__(self) -> "AdbcConnection":
        self._connection = self._config.create_connection()
        return cast("AdbcConnection", self._connection)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._connection is not None:
            if self._connection is not self._config.connection_instance:
                self._connection.close()
            self._connection = None
        return None


class _AdbcSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_connection", "_driver_features", "_origin_pool", "_profile_path")

    def __init__(self, config: "AdbcConfig") -> None:
        super().__init__(config)
        self._connection: AdbcConnection | None = None
        self._driver_features: dict[str, Any] = dict(config.driver_features)
        self._profile_path: Path | None = None
        self._origin_pool: AdbcConnectionPool | None = None

    @property
    def driver_features(self) -> "dict[str, Any]":
        return self._driver_features

    def acquire_connection(self) -> "AdbcConnection":
        self._origin_pool = self._config.provide_pool()
        connection: AdbcConnection = (
            self._origin_pool.acquire() if self._origin_pool is not None else self._config.create_connection()
        )
        self._connection = connection
        try:
            if self._config._resolved_dialect == "duckdb" and bool(
                self._driver_features.get("enable_statement_profiling", False)
            ):
                self._profile_path = create_duckdb_profile_path()
                configure_adbc_duckdb_profiling(connection, self._profile_path)
                self._driver_features["_duckdb_profile_path"] = str(self._profile_path)
        except BaseException as exc:
            with contextlib.suppress(BaseException):
                self.release_connection(connection, exc_type=type(exc), exc_val=exc, exc_tb=exc.__traceback__)
            raise
        return connection

    def release_connection(self, _conn: "AdbcConnection", **kwargs: Any) -> None:
        if self._connection is None:
            return
        connection = self._connection
        pool, self._origin_pool = self._origin_pool, None
        profile_path = self._profile_path
        self._connection = None
        self._profile_path = None
        raw_path = self._driver_features.pop("_duckdb_profile_path", None)
        try:
            if pool is not None:
                driver = kwargs.get("driver")
                discard = kwargs.get("exc_type") is not None or bool(driver is not None and driver.has_streams)
                pool.release(connection, discard=discard)
            elif connection is not self._config.connection_instance:
                connection.close()
        finally:
            try:
                cleanup_duckdb_profile_path(profile_path)
            finally:
                if isinstance(raw_path, str):
                    cleanup_duckdb_profile_path(Path(raw_path))


class AdbcConfig(SyncDatabaseConfig[AdbcConnection, AdbcConnectionPool | AdbcConnection | None, AdbcDriver]):
    """ADBC configuration for Arrow Database Connectivity.

    ADBC provides an interface for connecting to multiple database systems
    with Arrow-native data transfer.

    Supports multiple database backends including PostgreSQL, SQLite, DuckDB,
    BigQuery, and Snowflake with automatic driver detection and loading.
    """

    driver_type: ClassVar[type[AdbcDriver]] = AdbcDriver
    connection_type: "ClassVar[type[AdbcConnection]]" = AdbcConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_reliable_rowcount: ClassVar[bool] = False
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed", "rows_per_chunk")
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[AdbcConnectionContext]]" = AdbcConnectionContext
    _session_factory_class: "ClassVar[type[_AdbcSessionConnectionHandler]]" = _AdbcSessionConnectionHandler
    _session_context_class: "ClassVar[type[AdbcSessionContext]]" = AdbcSessionContext
    _default_statement_config = StatementConfig()
    __slots__ = (
        "_default_session_config",
        "_paradedb_available",
        "_pg_textsearch_available",
        "_pgvector_available",
        "_pool_close_started",
        "_pool_config",
        "_resolved_dialect",
        "_user_connection_hook",
    )

    def __init__(
        self,
        *,
        connection_config: "AdbcConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "AdbcConnection | None" = None,
        pool_config: "AdbcPoolConfig | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: StatementConfig | None = None,
        driver_features: "AdbcDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize configuration.

        Args:
            connection_config: Connection configuration parameters
            connection_instance: Pre-created connection instance to use instead of creating new one
            pool_config: Optional PostgreSQL clone pooling; None preserves unpooled ownership.
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Driver feature configuration (AdbcDriverFeatures)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        self.connection_config = normalize_connection_aliases(normalize_connection_config(connection_config))
        self._pgvector_available: bool | None = None
        self._paradedb_available: bool | None = None
        self._pg_textsearch_available: bool | None = None

        self._resolved_dialect = resolve_dialect_from_config(self.connection_config)

        if statement_config is None:
            statement_config = get_statement_config(self._resolved_dialect)

        explicit_lowercase = bool(driver_features and "enable_lowercase_column_names" in driver_features)
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        if not explicit_lowercase and self._resolved_dialect in {"oracle", "db2", "snowflake"}:
            features_dict["enable_lowercase_column_names"] = True
        self._user_connection_hook = cast(
            "Callable[[AdbcConnection], None] | None", features_dict.pop("on_connection_create", None)
        )
        self._default_session_config = get_statement_config(self._resolved_dialect)
        self._pool_config = cast("AdbcPoolConfig", dict(pool_config)) if pool_config is not None else None
        self._pool_close_started = False
        self._validate_pool_settings(connection_instance)

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_pool(self) -> "AdbcConnectionPool | None":
        """Publish one lazy generation, rejecting a drain until every native owner closes."""
        if self._pool_config is None:
            return None
        while True:
            existing = self.connection_instance
            if isinstance(existing, AdbcConnectionPool):
                existing.check_pid()
            with self._pool_lock:
                if self.connection_instance is not existing:
                    continue
                if existing is not None and not isinstance(existing, AdbcConnectionPool):
                    msg = "A pooled ADBC configuration cannot own an injected raw connection."
                    raise ImproperConfigurationError(msg)
                if existing is not None and not existing.is_closed:
                    if self._pool_close_started:
                        msg = "ADBC pool is draining; retry shutdown before starting another generation."
                        raise DatabaseConnectionError(msg)
                    return existing
                if existing is not None:
                    self.connection_instance = None
                    self._pool_close_started = False
                    created = None
                else:
                    created = self._create_pool()
                    self.connection_instance = created
                    self._pool_close_started = False
            runtime = self.get_observability_runtime()
            if existing is not None:
                runtime.emit_pool_destroy_sync(existing)
                continue
            runtime.emit_pool_create_sync(created)
            return created

    def provide_pool(self, *args: Any, **kwargs: Any) -> "AdbcConnectionPool | None":
        """Return the current enabled pool generation, or None when pooling is disabled."""
        return self.create_pool()

    def close_pool(self) -> None:
        """Drain an owned generation, retaining native cleanup failures for retry."""
        self._shutdown_pool(retire=True)

    def derive_pool(self, **overrides: Any) -> Self:
        """Derive independent ownership while preserving explicit pool settings."""
        pool_config = overrides.pop("pool_config", self._pool_config)
        derived = super().derive_pool(**overrides)
        derived._pool_config = cast("AdbcPoolConfig", dict(pool_config)) if pool_config is not None else None
        derived._validate_pool_settings(None)
        return derived

    def _create_pool(self) -> "AdbcConnectionPool | None":
        if self._pool_config is None:
            return None
        return AdbcConnectionPool(self.create_connection, **self._pool_config)

    def _close_pool(self) -> None:
        self._shutdown_pool(retire=False)

    def _shutdown_pool(self, *, retire: bool) -> None:
        if self._pool_config is None:
            return
        while True:
            pool = self.connection_instance
            if pool is None:
                return
            if not isinstance(pool, AdbcConnectionPool):
                msg = "Pooled ADBC shutdown requires an owned pool generation."
                raise ImproperConfigurationError(msg)
            pool.check_pid()
            with self._pool_lock:
                if self.connection_instance is not pool:
                    continue
                first_close = not self._pool_close_started
                self._pool_close_started = True
                break
        runtime = self.get_observability_runtime()
        if first_close:
            runtime.emit_pool_destroying_sync(pool)
        pool.close()
        if retire and pool.is_closed:
            with self._pool_lock:
                retired = self.connection_instance is pool
                if retired:
                    self.connection_instance = None
                    self._pool_close_started = False
            if retired:
                runtime.emit_pool_destroy_sync(pool)

    def _validate_pool_settings(self, connection_instance: "AdbcConnection | None") -> None:
        if self._pool_config is None:
            return
        max_size = self._pool_config.get("max_size", 4)
        timeout = self._pool_config.get("timeout", 30.0)
        if isinstance(max_size, bool) or not isinstance(max_size, int) or max_size <= 0:
            msg = "ADBC pool max_size must be a positive integer."
            raise ImproperConfigurationError(msg)
        if not math.isfinite(timeout) or timeout < 0:
            msg = "ADBC pool timeout must be finite and nonnegative."
            raise ImproperConfigurationError(msg)
        if resolve_driver_name_from_config(self.connection_config) != "adbc_driver_postgresql.dbapi.connect":
            msg = "ADBC pooling supports only the native PostgreSQL driver."
            raise ImproperConfigurationError(msg)
        conn_kwargs = self.connection_config.get("conn_kwargs") or {}
        native_autocommit = conn_kwargs.get("adbc.connection.autocommit", False)
        if self.connection_config.get("autocommit", False) or native_autocommit not in (False, "false", "0", 0):
            msg = "ADBC pooling requires autocommit=False."
            raise ImproperConfigurationError(msg)
        if connection_instance is not None or self._user_connection_hook is not None:
            msg = "ADBC pooling does not support injected connections or connection initialization hooks."
            raise ImproperConfigurationError(msg)
        for distribution in ("adbc-driver-manager", "adbc-driver-postgresql"):
            try:
                installed = importlib.metadata.version(distribution)
            except importlib.metadata.PackageNotFoundError as exc:
                msg = f"ADBC pooling requires {distribution}>=1.12.0."
                raise ImproperConfigurationError(msg) from exc
            match = re.fullmatch(
                r"(\d+)\.(\d+)(?:\.(\d+))?((?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?(?:\+[a-zA-Z0-9.-]+)?)",
                installed,
            )
            release = tuple(int(part or 0) for part in match.groups()[:3]) if match is not None else (0, 0, 0)
            suffix = match.group(4) if match is not None else ""
            if release < (1, 12, 0) or (release == (1, 12, 0) and suffix.startswith(("a", "b", "rc", ".dev"))):
                msg = f"ADBC pooling requires {distribution}>=1.12.0; installed version is {installed}."
                raise ImproperConfigurationError(msg)
        if not callable(getattr(AdbcConnection, "adbc_clone", None)):
            msg = "ADBC pooling requires the public Connection.adbc_clone capability."
            raise ImproperConfigurationError(msg)

    @property
    def supports_migration_schemas(self) -> bool:  # type: ignore[override]
        """Migration schema support is only available for PostgreSQL-backed ADBC drivers."""
        return is_postgres_dialect(self._resolved_dialect)

    @property
    def pg_textsearch_available(self) -> bool:
        """Return True if the pg_textsearch extension is available."""
        return bool(self._pg_textsearch_available)

    def create_connection(self) -> AdbcConnection:
        """Create and return a new connection using the specified driver.

        Returns:
            A new connection instance, or ``connection_instance`` when configured.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        if self._pool_config is None and self.connection_instance is not None:
            return cast("AdbcConnection", self.connection_instance)

        connection: AdbcConnection | None = None
        try:
            driver_name = cast(
                "str | None", self.connection_config.get("driver_name") or self.connection_config.get("driver")
            )
            db_kwargs = self.connection_config.get("db_kwargs")
            nested_uri = db_kwargs.get("uri") if isinstance(db_kwargs, dict) else None
            uri = cast("str | None", self.connection_config.get("uri") or nested_uri)
            connection = resolve_driver_connect_func(driver_name, uri)(
                **build_connection_config(self.connection_config)
            )
            if self._user_connection_hook is not None:
                self._user_connection_hook(cast("AdbcConnection", connection))
            return cast("AdbcConnection", connection)
        except BaseException as e:
            if connection is not None:
                with contextlib.suppress(BaseException):
                    connection.close()
            if not isinstance(e, Exception):
                raise
            err_driver_name = (
                self.connection_config.get("driver_name") or self.connection_config.get("driver") or "Unknown"
            )
            msg = f"Could not configure connection using driver '{err_driver_name}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AdbcConnectionContext":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A connection context manager.
        """
        return AdbcConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "AdbcSessionContext":
        """Provide a driver session context manager.

        On first call with a PostgreSQL backend, detects pgvector/paradedb
        extensions and updates the dialect accordingly.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields an AdbcDriver instance.
        """
        if self._pool_config is not None:
            self.create_pool()
        self._detect_extensions_if_needed()
        statement_config = resolve_runtime_statement_config(
            statement_config, self.statement_config, self._default_session_config
        )
        handler = _AdbcSessionConnectionHandler(self)

        return AdbcSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config,
            driver_features=handler.driver_features,
            prepare_driver=self._prepare_driver,
            driver_class=AdbcDriver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for AdbcConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "AdbcConnectionContext": AdbcConnectionContext,
            "AdbcConnection": AdbcConnection,
            "AdbcConnectionParams": AdbcConnectionParams,
            "AdbcCursor": AdbcCursor,
            "AdbcDriver": AdbcDriver,
            "AdbcDriverFeatures": AdbcDriverFeatures,
            "AdbcExceptionHandler": AdbcExceptionHandler,
            "AdbcSessionContext": AdbcSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults suitable for ADBC warehouses."""

        return EventRuntimeHints(poll_interval=2.0, lease_seconds=60, retention_seconds=172_800)

    def _detect_extensions_if_needed(self) -> None:
        """Detect postgres extensions on first call, caching results.

        Only queries ``pg_extension`` when the resolved dialect is PostgreSQL
        and detection has not yet run (``_pgvector_available is None``).
        """
        if self._pgvector_available is not None:
            return

        dialect = resolve_dialect_name(self.statement_config.dialect)
        if not is_postgres_dialect(dialect):
            self._pgvector_available = False
            self._paradedb_available = False
            self._pg_textsearch_available = False
            return

        probe_names = build_postgres_extension_probe_names(self.driver_features)
        if not probe_names:
            self._pgvector_available = False
            self._paradedb_available = False
            self._pg_textsearch_available = False
            return

        connection = self.create_connection()
        try:
            pgvector_available, paradedb_available, pg_textsearch_available = detect_postgres_extensions(
                connection,
                enable_pgvector="vector" in probe_names,
                enable_paradedb="pg_search" in probe_names,
                enable_pg_textsearch="pg_textsearch" in probe_names,
            )
        finally:
            if connection is not self.connection_instance:
                connection.close()

        detected_extensions: set[str] = set()
        if pgvector_available:
            detected_extensions.add("vector")
        if paradedb_available:
            detected_extensions.add("pg_search")
        if pg_textsearch_available:
            detected_extensions.add("pg_textsearch")
        self.statement_config, self._pgvector_available, self._paradedb_available = resolve_postgres_extension_state(
            self.statement_config, self.driver_features, detected_extensions
        )
        self._pg_textsearch_available = is_postgres_extension_active(self.driver_features, "pg_textsearch")
