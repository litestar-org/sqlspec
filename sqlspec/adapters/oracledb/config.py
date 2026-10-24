"""OracleDB database configuration with direct field-based configuration."""

import contextlib
from collections.abc import Awaitable, Callable, Mapping
from inspect import isawaitable
from pathlib import Path
from ssl import TLSVersion
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, Self, TypedDict, cast

from sqlspec.adapters.oracledb._json_handlers import register_json_handlers
from sqlspec.adapters.oracledb._typing import (
    OracleAsyncConnection,
    OracleAsyncConnectionPool,
    OracleAsyncCursor,
    OracleAsyncSessionContext,
    OracleSyncConnection,
    OracleSyncConnectionPool,
    OracleSyncCursor,
    OracleSyncSessionContext,
)
from sqlspec.adapters.oracledb._typing import OracleAuthMode as AuthMode
from sqlspec.adapters.oracledb._typing import OraclePoolGetMode as PoolGetMode
from sqlspec.adapters.oracledb._typing import OraclePurity as Purity
from sqlspec.adapters.oracledb._typing import oracledb_module as oracledb
from sqlspec.adapters.oracledb._uuid_handlers import register_uuid_handlers
from sqlspec.adapters.oracledb._vector_handlers import register_numpy_handlers
from sqlspec.adapters.oracledb.core import (
    VALID_ORACLE_DRIVER_FEATURE_KEYS,
    OracleEndpoint,
    OracleSessionContextConfig,
    OracleTopologyConfig,
    apply_driver_features,
    build_connection_config,
    default_statement_config,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
    extract_oracle_wallet_zip,
    release_extracted_wallet_dir,
    retain_extracted_wallet_dir,
)
from sqlspec.adapters.oracledb.data_dictionary import (
    OracleVersionCache,
    resolve_oracle_connection_compatible_major,
    resolve_oracle_connection_major,
)
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, MigrationConfig, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OracleEndpoint",
    "OraclePoolParams",
    "OracleSessionContextConfig",
    "OracleSyncConfig",
    "OracleTopologyConfig",
)


OracleAccessToken = str | tuple[str, str] | Callable[..., str | tuple[str, str]]
OracleAppContext = tuple[str, str, str]
OracleProtocol = Literal["tcp", "tcps"]
OracleServerType = Literal["dedicated", "shared", "pooled"]
OraclePoolBoundary = Literal["statement", "transaction"]
OracleVectorReturnFormat = Literal["array", "list", "numpy"]
OracleEventsBackend = Literal["aq", "poll_queue", "txeventq"]
_ORACLE_VECTOR_MIN_MAJOR = 23


class OracleConnectionParams(TypedDict):
    """OracleDB connection parameters."""

    dsn: NotRequired[str]
    pool_alias: NotRequired[str]
    user: NotRequired[str]
    proxy_user: NotRequired[str]
    password: NotRequired[str]
    newpassword: NotRequired[str]
    wallet_password: NotRequired[str]
    access_token: NotRequired[OracleAccessToken]
    host: NotRequired[str]
    port: NotRequired[int]
    protocol: NotRequired[OracleProtocol]
    https_proxy: NotRequired[str]
    https_proxy_port: NotRequired[int]
    service_name: NotRequired[str]
    instance_name: NotRequired[str]
    sid: NotRequired[str]
    server_type: NotRequired[OracleServerType]
    cclass: NotRequired[str]
    purity: NotRequired[Purity]
    expire_time: NotRequired[int]
    externalauth: NotRequired[bool]
    mode: NotRequired[AuthMode]
    wallet_location: NotRequired[str]
    config_dir: NotRequired[str]
    tcp_connect_timeout: NotRequired[float]
    retry_count: NotRequired[int]
    retry_delay: NotRequired[int]
    ssl_server_dn_match: NotRequired[bool]
    ssl_server_cert_dn: NotRequired[str]
    events: NotRequired[bool]
    disable_oob: NotRequired[bool]
    stmtcachesize: NotRequired[int]
    edition: NotRequired[str]
    tag: NotRequired[str]
    matchanytag: NotRequired[bool]
    appcontext: NotRequired[list[OracleAppContext]]
    shardingkey: NotRequired[list[Any]]
    supershardingkey: NotRequired[list[Any]]
    debug_jdwp: NotRequired[str]
    connection_id_prefix: NotRequired[str]
    ssl_context: NotRequired[Any]
    sdu: NotRequired[int]
    pool_boundary: NotRequired[OraclePoolBoundary]
    use_tcp_fast_open: NotRequired[bool]
    ssl_version: NotRequired[TLSVersion]
    program: NotRequired[str]
    machine: NotRequired[str]
    terminal: NotRequired[str]
    osuser: NotRequired[str]
    driver_name: NotRequired[str]
    use_sni: NotRequired[bool]
    thick_mode_dsn_passthrough: NotRequired[bool]
    extra_auth_params: NotRequired[dict[str, Any]]
    pool_name: NotRequired[str]
    on_connect_callback: NotRequired[Callable[..., Any]]
    handle: NotRequired[int]
    extra: NotRequired[dict[str, Any]]
    topology: NotRequired[OracleTopologyConfig | dict[str, Any]]
    wallet_zip: NotRequired[str | Path | bytes]
    tns_alias: NotRequired[str]


class OraclePoolParams(OracleConnectionParams):
    """OracleDB pool parameters."""

    thick_mode: NotRequired[bool]
    lib_dir: NotRequired[str]
    pool_class: NotRequired[type[Any]]
    params: NotRequired[oracledb.PoolParams]
    min: NotRequired[int]
    max: NotRequired[int]
    increment: NotRequired[int]
    connectiontype: NotRequired[type[Any]]
    getmode: NotRequired[PoolGetMode]
    homogeneous: NotRequired[bool]
    timeout: NotRequired[int]
    wait_timeout: NotRequired[int]
    max_lifetime_session: NotRequired[int]
    session_callback: NotRequired[Callable[..., Any]]
    max_sessions_per_shard: NotRequired[int]
    soda_metadata_cache: NotRequired[bool]
    ping_interval: NotRequired[int]
    ping_timeout: NotRequired[int]


class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags.

    enable_numpy_vectors: Enable automatic NumPy array ↔ Oracle VECTOR conversion.
     Requires NumPy and Oracle Database 23ai or higher with VECTOR data type support.
     Defaults to True when NumPy is installed.
     Provides automatic bidirectional conversion between NumPy ndarrays and Oracle VECTOR columns.
     Supports float32, float64, int8, and uint8 dtypes.
    enable_lowercase_column_names: Normalize implicit Oracle uppercase column names to lowercase.
     Targets unquoted Oracle identifiers that default to uppercase while preserving quoted case-sensitive aliases.
     Defaults to True for compatibility with schema libraries expecting snake_case fields.
    enable_uuid_binary: Enable automatic UUID ↔ RAW(16) binary conversion.
     When True (default), Python UUID objects are automatically converted to/from
     RAW(16) binary format for optimal storage efficiency (16 bytes vs 36 bytes).
     Applies only to RAW(16) columns; other RAW sizes remain unchanged.
     Uses Python's stdlib uuid module (no external dependencies).
     Defaults to True for improved type safety and storage efficiency.
    vector_return_format: Return type for VECTOR column reads. One of:
     - "numpy" (default when NumPy is installed): np.ndarray, zero-copy compute path.
     - "list": list[float|int], best for code that expects native Python sequences.
     - "array": array.array, zero-copy oracledb passthrough.
     Defaults to "numpy" when NumPy is installed, otherwise "list".
     Sparse VECTOR columns always bind and return python-oracledb ``SparseVector`` values.
    oracle_varchar2_byte_limit: Threshold (in UTF-8 bytes) above which ``str``
     parameters are auto-coerced to ``DB_TYPE_CLOB``. Defaults to 4000 (the
     Oracle SQL VARCHAR2 limit). Databases with ``MAX_STRING_SIZE=EXTENDED``
     may set this to 32767 to keep larger strings as VARCHAR2.
    oracle_raw_byte_limit: Threshold (in bytes) above which ``bytes`` parameters
     are auto-coerced to ``DB_TYPE_BLOB``. Defaults to 2000 (the Oracle SQL
     RAW limit).
    arraysize: Optional per-cursor row fetch buffer size. When absent, the
     python-oracledb cursor default is left unchanged.
    prefetchrows: Optional per-cursor prefetch row count. When absent, the
     python-oracledb cursor default is left unchanged.
    fetch_lobs: Optional per-statement LOB fetch mode. ``False`` returns
     supported LOB values as ``str``/``bytes`` instead of LOB locators.
    fetch_decimals: Optional per-statement NUMBER fetch mode. ``True`` returns
     decimal values where python-oracledb supports them.
    on_connection_create: Callback executed when a connection is acquired from pool.
     For sync: Callable[[OracleSyncConnection, str], None] - receives connection and tag
     For async: Callable[[OracleAsyncConnection, str], Awaitable[None]]
     Called after internal setup (numpy vectors, UUID handlers).
    enable_events: Enable SQLSpec event queue support.
     Defaults to True when extension_config["events"] is configured.
     Provides pub/sub capabilities via Oracle Advanced Queuing or table-backed fallback.
     Requires extension_config["events"] for migration setup when using poll_queue backend.
     This is separate from connection_config["events"], which enables python-oracledb
     Thick mode database event notifications for HA and continuous query notification.
    events_backend: Event channel backend selection.
     Options: "aq", "poll_queue", "txeventq"
     - "aq": Oracle Advanced Queuing (native messaging, requires DBMS_AQADM privileges)
     - "txeventq": Oracle Transactional Event Queues (native messaging, requires
       DBMS_AQADM privileges; provisioned via DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE)
     - "poll_queue": Durable table-backed queue with lease-based retries and acknowledgements
     Defaults to "poll_queue" (works on all Oracle editions without special privileges).
    enable_direct_path_load: Route load_from_arrow through Connection.direct_path_load.
     Thin-mode only; falls back to executemany when the API is absent or the
     connection is in Thick mode. Defaults to True; set to False to force
     executemany.
    session_context_hook: Optional declarative session context configuration
     applied on checkout and cleared on release.
    dbms_cloud_credential: Optional default ``DBMS_CLOUD`` credential name used
     by ``load_from_storage`` for server-side object-store ingestion via
     ``DBMS_CLOUD.COPY_DATA``.
    Native pipeline execution is runtime-gated by async Thin-mode driver API support,
     Oracle Database 26ai or newer, and the SQLSPEC_ORACLE_DISABLE_PIPELINE environment
     override; there is no adapter config switch that can force-enable unsupported
     pipeline execution.
    """

    enable_numpy_vectors: NotRequired[bool]
    enable_lowercase_column_names: NotRequired[bool]
    enable_uuid_binary: NotRequired[bool]
    vector_return_format: NotRequired[OracleVectorReturnFormat]
    oracle_varchar2_byte_limit: NotRequired[int]
    oracle_raw_byte_limit: NotRequired[int]
    arraysize: NotRequired[int]
    prefetchrows: NotRequired[int]
    fetch_lobs: NotRequired[bool]
    fetch_decimals: NotRequired[bool]
    on_connection_create: NotRequired[Callable[..., Any]]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[OracleEventsBackend]
    enable_direct_path_load: NotRequired[bool]
    session_context_hook: NotRequired[OracleSessionContextConfig | dict[str, Any]]
    enable_session_tracing: NotRequired[bool]
    dbms_cloud_credential: NotRequired[str | None]


class OracleSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for Oracle sync connections."""

    __slots__ = ("_conn",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._conn: OracleSyncConnection | None = None

    def __enter__(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        conn = self._config.connection_instance.acquire()
        self._conn = conn
        session_hook = self._config.driver_features.get("session_context_hook")
        if session_hook is not None:
            try:
                execute_session_checkout_sync(conn, session_hook)
            except BaseException:
                self._conn = None
                self._config.connection_instance.release(conn)
                raise
        return cast("OracleSyncConnection", conn)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._conn is not None:
            conn = self._conn
            self._conn = None
            session_hook = self._config.driver_features.get("session_context_hook")
            try:
                if session_hook is not None:
                    execute_session_release_sync(conn, session_hook)
            finally:
                if self._config.connection_instance:
                    self._config.connection_instance.release(conn)
        return None


class _OracleSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_conn",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._conn: OracleSyncConnection | None = None

    def acquire_connection(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        conn = self._config.connection_instance.acquire()
        self._conn = conn
        session_hook = self._config.driver_features.get("session_context_hook")
        if session_hook is not None:
            try:
                execute_session_checkout_sync(conn, session_hook)
            except BaseException:
                self._conn = None
                self._config.connection_instance.release(conn)
                raise
        return cast("OracleSyncConnection", conn)

    def release_connection(self, _conn: "OracleSyncConnection", **kwargs: Any) -> None:
        if self._conn is None:
            return
        conn = self._conn
        self._conn = None
        session_hook = self._config.driver_features.get("session_context_hook")
        try:
            if session_hook is not None:
                execute_session_release_sync(conn, session_hook)
        finally:
            if self._config.connection_instance:
                self._config.connection_instance.release(conn)


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "OracleSyncConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections."""

    __slots__ = ("_extracted_wallet_dir", "_oracle_version_cache", "_pool_session_callback", "_user_connection_hook")

    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    connection_type: "ClassVar[type[OracleSyncConnection]]" = OracleSyncConnection
    migration_tracker_type: "ClassVar[type[OracleSyncMigrationTracker]]" = OracleSyncMigrationTracker
    supports_transactional_ddl: ClassVar[bool] = False
    supports_migration_schemas: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[OracleSyncConnectionContext]]" = OracleSyncConnectionContext
    _session_factory_class: "ClassVar[type[_OracleSyncSessionConnectionHandler]]" = _OracleSyncSessionConnectionHandler
    _session_context_class: "ClassVar[type[OracleSyncSessionContext]]" = OracleSyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleSyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            **kwargs: Additional keyword arguments.
        """
        normalized_conn = dict(normalize_connection_config(connection_config))
        self._extracted_wallet_dir: Path | None = None
        pending_wallet_dir: Path | None = None
        if normalized_conn.get("wallet_zip") is not None:
            raw_wallet_source = normalized_conn.pop("wallet_zip")
            source_is_existing_dir = isinstance(raw_wallet_source, (str, Path)) and Path(raw_wallet_source).is_dir()
            wallet_dir = extract_oracle_wallet_zip(
                raw_wallet_source, wallet_password=normalized_conn.get("wallet_password")
            )
            if not source_is_existing_dir or wallet_dir.resolve() != Path(raw_wallet_source).resolve():
                pending_wallet_dir = wallet_dir
            normalized_conn.setdefault("config_dir", str(wallet_dir))
            normalized_conn.setdefault("wallet_location", str(wallet_dir))
        connection_config = build_connection_config(normalized_conn)
        self._oracle_version_cache = OracleVersionCache()
        self._pool_session_callback = cast(
            "Callable[[OracleSyncConnection, str], None] | None", connection_config.pop("session_callback", None)
        )
        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[OracleSyncConnection, str], None] | None = features_dict.pop(
            "on_connection_create", None
        )

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )
        if pending_wallet_dir is not None:
            self._extracted_wallet_dir = pending_wallet_dir
            retain_extracted_wallet_dir(pending_wallet_dir)

    def create_connection(self) -> "OracleSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        return self.connection_instance.acquire()

    def provide_pool(self) -> "OracleSyncConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def derive_pool(
        self,
        *,
        connection_config_overrides: "Mapping[str, Any] | None" = None,
        driver_features_overrides: "Mapping[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: "str | None" = None,
        **overrides: Any,
    ) -> Self:
        """Clone this OracleSyncConfig with overridden connection/pool or feature settings and ``connection_instance=None``."""
        new_conn_cfg: dict[str, Any] = dict(self.connection_config)
        new_driver_features: dict[str, Any] = dict(self.driver_features)
        if self._pool_session_callback is not None and "session_callback" not in new_conn_cfg:
            new_conn_cfg["session_callback"] = self._pool_session_callback
        if self._user_connection_hook is not None and "on_connection_create" not in new_driver_features:
            new_driver_features["on_connection_create"] = self._user_connection_hook
        if connection_config_overrides:
            new_conn_cfg.update(connection_config_overrides)
        if driver_features_overrides:
            new_driver_features.update(driver_features_overrides)
        for key, val in overrides.items():
            if key in VALID_ORACLE_DRIVER_FEATURE_KEYS:
                new_driver_features[key] = val
            else:
                new_conn_cfg[key] = val
        derived = type(self)(
            connection_config=new_conn_cfg,
            connection_instance=None,
            migration_config=cast("dict[str, Any] | None", dict(self.migration_config))
            if migration_config is None
            else cast("dict[str, Any] | None", migration_config),
            statement_config=self.statement_config if statement_config is None else statement_config,
            driver_features=new_driver_features,
            bind_key=self.bind_key if bind_key is None else bind_key,
            extension_config=cast("ExtensionConfigs", dict(self.extension_config))
            if extension_config is None
            else extension_config,
            observability_config=self.observability_config if observability_config is None else observability_config,
        )
        if (
            self._extracted_wallet_dir is not None
            and derived._extracted_wallet_dir is None
            and str(self._extracted_wallet_dir)
            in {derived.connection_config.get("config_dir"), derived.connection_config.get("wallet_location")}
        ):
            derived._extracted_wallet_dir = self._extracted_wallet_dir
            retain_extracted_wallet_dir(self._extracted_wallet_dir)
        return derived

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleDB types.

        Provides OracleDB-specific types for Litestar framework recognition.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OracleEndpoint": OracleEndpoint,
            "OraclePoolParams": OraclePoolParams,
            "OracleSessionContextConfig": OracleSessionContextConfig,
            "OracleSyncConnectionContext": OracleSyncConnectionContext,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleSyncSessionContext": OracleSyncSessionContext,
            "OracleTopologyConfig": OracleTopologyConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""
        config = dict(self.connection_config)
        thick_mode = config.pop("thick_mode", False)
        lib_dir = config.pop("lib_dir", None)
        if (thick_mode or lib_dir is not None or config.get("soda_metadata_cache")) and oracledb.is_thin_mode():
            client_config = {}
            if lib_dir is not None:
                client_config["lib_dir"] = lib_dir
            if config.get("config_dir") is not None:
                client_config["config_dir"] = config["config_dir"]
            oracledb.init_oracle_client(**client_config)

        config.pop("threaded", None)
        config["session_callback"] = self._init_connection

        return oracledb.create_pool(**config)

    def _init_connection(self, connection: "OracleSyncConnection", tag: str) -> None:
        """Initialize connection with type handlers and cached server metadata.

        Args:
            connection: Oracle connection to initialize.
            tag: Connection tag for session state.
        """
        server_major = resolve_oracle_connection_major(connection, self._oracle_version_cache)
        compatible_major = resolve_oracle_connection_compatible_major(connection, self._oracle_version_cache)
        enable_vectors = bool(self.driver_features.get("enable_numpy_vectors", True))

        conn_any = cast("Any", connection)
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_major = server_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_compatible_major = compatible_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_vector_return_format = self.driver_features.get("vector_return_format")
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_enable_numpy_vectors = enable_vectors

        if enable_vectors and (server_major is None or server_major >= _ORACLE_VECTOR_MIN_MAJOR):
            register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        if self._pool_session_callback is not None:
            self._pool_session_callback(connection, tag)

        if self._user_connection_hook is not None:
            self._user_connection_hook(connection, tag)

    def _prepare_driver(self, driver: "OracleSyncDriver") -> "OracleSyncDriver":
        """Attach the pool-scoped version cache alongside the observability runtime."""
        driver = super()._prepare_driver(driver)
        driver._oracle_version_cache = self._oracle_version_cache
        return driver

    def _close_pool(self) -> None:
        """Close the actual connection pool and release any extracted temporary wallet directory."""
        try:
            if self.connection_instance:
                self.connection_instance.close()
                self.connection_instance = None
            self._oracle_version_cache.reset()
        finally:
            if self._extracted_wallet_dir is not None:
                wallet_dir = self._extracted_wallet_dir
                self._extracted_wallet_dir = None
                release_extracted_wallet_dir(wallet_dir)


class OracleAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Oracle connections."""

    __slots__ = ()

    async def __aenter__(self) -> Any:
        conn = await super().__aenter__()
        session_hook = self._config.driver_features.get("session_context_hook")
        if session_hook is not None:
            try:
                await execute_session_checkout_async(conn, session_hook)
            except BaseException:
                await super().__aexit__(None, None, None)
                raise
        return conn

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        conn = self._connection
        session_hook = self._config.driver_features.get("session_context_hook") if conn is not None else None
        try:
            if conn is not None and session_hook is not None:
                await execute_session_release_async(conn, session_hook)
        finally:
            await super().__aexit__(exc_type, exc_val, exc_tb)
        return None


class _OracleAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ()

    async def acquire_connection(self) -> Any:
        conn = await super().acquire_connection()
        session_hook = self._config.driver_features.get("session_context_hook")
        if session_hook is not None:
            try:
                await execute_session_checkout_async(conn, session_hook)
            except BaseException:
                await super().release_connection(conn)
                raise
        return conn

    async def release_connection(self, _conn: Any, **kwargs: Any) -> None:
        conn = self._connection
        session_hook = self._config.driver_features.get("session_context_hook") if conn is not None else None
        try:
            if conn is not None and session_hook is not None:
                await execute_session_release_async(conn, session_hook)
        finally:
            await super().release_connection(_conn, **kwargs)


class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "OracleAsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections."""

    __slots__ = ("_extracted_wallet_dir", "_oracle_version_cache", "_pool_session_callback", "_user_connection_hook")

    connection_type: "ClassVar[type[OracleAsyncConnection]]" = OracleAsyncConnection
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    migration_tracker_type: "ClassVar[type[OracleAsyncMigrationTracker]]" = OracleAsyncMigrationTracker
    supports_transactional_ddl: ClassVar[bool] = False
    supports_migration_schemas: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[OracleAsyncConnectionContext]]" = OracleAsyncConnectionContext
    _session_factory_class: "ClassVar[type[_OracleAsyncSessionConnectionHandler]]" = (
        _OracleAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[OracleAsyncSessionContext]]" = OracleAsyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleAsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            **kwargs: Additional keyword arguments.
        """
        normalized_conn = dict(normalize_connection_config(connection_config))
        self._extracted_wallet_dir: Path | None = None
        pending_wallet_dir: Path | None = None
        if normalized_conn.get("wallet_zip") is not None:
            raw_wallet_source = normalized_conn.pop("wallet_zip")
            source_is_existing_dir = isinstance(raw_wallet_source, (str, Path)) and Path(raw_wallet_source).is_dir()
            wallet_dir = extract_oracle_wallet_zip(
                raw_wallet_source, wallet_password=normalized_conn.get("wallet_password")
            )
            if not source_is_existing_dir or wallet_dir.resolve() != Path(raw_wallet_source).resolve():
                pending_wallet_dir = wallet_dir
            normalized_conn.setdefault("config_dir", str(wallet_dir))
            normalized_conn.setdefault("wallet_location", str(wallet_dir))
        connection_config = build_connection_config(normalized_conn)
        self._oracle_version_cache = OracleVersionCache()
        self._pool_session_callback = cast(
            "Callable[[OracleAsyncConnection, str], Any] | None", connection_config.pop("session_callback", None)
        )

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[OracleAsyncConnection, str], Awaitable[None]] | None = features_dict.pop(
            "on_connection_create", None
        )

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )
        if pending_wallet_dir is not None:
            self._extracted_wallet_dir = pending_wallet_dir
            retain_extracted_wallet_dir(pending_wallet_dir)

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return cast("OracleAsyncConnection", await self.connection_instance.acquire())

    async def provide_pool(self) -> "OracleAsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def derive_pool(
        self,
        *,
        connection_config_overrides: "Mapping[str, Any] | None" = None,
        driver_features_overrides: "Mapping[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: "str | None" = None,
        **overrides: Any,
    ) -> Self:
        """Clone this OracleAsyncConfig with overridden connection/pool or feature settings and ``connection_instance=None``."""
        new_conn_cfg: dict[str, Any] = dict(self.connection_config)
        new_driver_features: dict[str, Any] = dict(self.driver_features)
        if self._pool_session_callback is not None and "session_callback" not in new_conn_cfg:
            new_conn_cfg["session_callback"] = self._pool_session_callback
        if self._user_connection_hook is not None and "on_connection_create" not in new_driver_features:
            new_driver_features["on_connection_create"] = self._user_connection_hook
        if connection_config_overrides:
            new_conn_cfg.update(connection_config_overrides)
        if driver_features_overrides:
            new_driver_features.update(driver_features_overrides)
        for key, val in overrides.items():
            if key in VALID_ORACLE_DRIVER_FEATURE_KEYS:
                new_driver_features[key] = val
            else:
                new_conn_cfg[key] = val
        derived = type(self)(
            connection_config=new_conn_cfg,
            connection_instance=None,
            migration_config=cast("dict[str, Any] | None", dict(self.migration_config))
            if migration_config is None
            else cast("dict[str, Any] | None", migration_config),
            statement_config=self.statement_config if statement_config is None else statement_config,
            driver_features=new_driver_features,
            bind_key=self.bind_key if bind_key is None else bind_key,
            extension_config=cast("ExtensionConfigs", dict(self.extension_config))
            if extension_config is None
            else extension_config,
            observability_config=self.observability_config if observability_config is None else observability_config,
        )
        if (
            self._extracted_wallet_dir is not None
            and derived._extracted_wallet_dir is None
            and str(self._extracted_wallet_dir)
            in {derived.connection_config.get("config_dir"), derived.connection_config.get("wallet_location")}
        ):
            derived._extracted_wallet_dir = self._extracted_wallet_dir
            retain_extracted_wallet_dir(self._extracted_wallet_dir)
        return derived

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleAsyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleAsyncConnectionContext": OracleAsyncConnectionContext,
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleAsyncSessionContext": OracleAsyncSessionContext,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OracleEndpoint": OracleEndpoint,
            "OraclePoolParams": OraclePoolParams,
            "OracleSessionContextConfig": OracleSessionContextConfig,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleTopologyConfig": OracleTopologyConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)

    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""
        config = dict(self.connection_config)
        thick_mode = config.pop("thick_mode", False)
        lib_dir = config.pop("lib_dir", None)
        if thick_mode or lib_dir is not None:
            msg = "OracleAsyncConfig only supports Thin mode; use OracleSyncConfig for Thick mode."
            raise ImproperConfigurationError(msg)

        config.pop("threaded", None)
        config["session_callback"] = self._init_connection

        return oracledb.create_pool_async(**config)

    async def _init_connection(self, connection: "OracleAsyncConnection", tag: str) -> None:
        """Initialize async connection with type handlers and cached server metadata.

        Args:
            connection: Oracle async connection to initialize.
            tag: Connection tag for session state.
        """
        server_major = resolve_oracle_connection_major(connection, self._oracle_version_cache)
        compatible_major = resolve_oracle_connection_compatible_major(connection, self._oracle_version_cache)
        enable_vectors = bool(self.driver_features.get("enable_numpy_vectors", True))

        conn_any = cast("Any", connection)
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_major = server_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_compatible_major = compatible_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_vector_return_format = self.driver_features.get("vector_return_format")
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_enable_numpy_vectors = enable_vectors

        if enable_vectors and (server_major is None or server_major >= _ORACLE_VECTOR_MIN_MAJOR):
            register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        if self._pool_session_callback is not None:
            session_callback_result = self._pool_session_callback(connection, tag)
            if isawaitable(session_callback_result):
                await session_callback_result

        if self._user_connection_hook is not None:
            hook_result = self._user_connection_hook(connection, tag)
            if isawaitable(hook_result):
                await hook_result

    def _prepare_driver(self, driver: "OracleAsyncDriver") -> "OracleAsyncDriver":
        """Attach the pool-scoped version cache alongside the observability runtime."""
        driver = super()._prepare_driver(driver)
        driver._oracle_version_cache = self._oracle_version_cache
        return driver

    async def _close_pool(self) -> None:
        """Close the actual async connection pool and release any extracted temporary wallet directory."""
        try:
            if self.connection_instance:
                await self.connection_instance.close()
                self.connection_instance = None
            self._oracle_version_cache.reset()
        finally:
            if self._extracted_wallet_dir is not None:
                wallet_dir = self._extracted_wallet_dir
                self._extracted_wallet_dir = None
                release_extracted_wallet_dir(wallet_dir)
