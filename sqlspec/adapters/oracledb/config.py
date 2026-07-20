"""OracleDB database configuration with direct field-based configuration."""

from collections.abc import Awaitable, Callable
from inspect import isawaitable
from ssl import TLSVersion
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

import oracledb
from oracledb import AuthMode, PoolGetMode, Purity
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb._json_handlers import register_json_handlers  # pyright: ignore[reportPrivateUsage]
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
from sqlspec.adapters.oracledb._uuid_handlers import register_uuid_handlers
from sqlspec.adapters.oracledb._vector_handlers import register_numpy_handlers  # pyright: ignore[reportPrivateUsage]
from sqlspec.adapters.oracledb.core import apply_driver_features, default_statement_config
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.data_dictionary.dialects.oracle import parse_oracle_version_components
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.core import StatementConfig


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OraclePoolParams",
    "OracleSyncConfig",
)


OracleAccessToken = str | tuple[str, str] | Callable[..., str | tuple[str, str]]
OracleAppContext = tuple[str, str, str]
OracleProtocol = Literal["tcp", "tcps"]
OracleServerType = Literal["dedicated", "shared", "pooled"]
OraclePoolBoundary = Literal["statement", "transaction"]
OracleVectorReturnFormat = Literal["array", "list", "numpy"]
OracleEventsBackend = Literal["aq", "poll_queue", "txeventq"]


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


class OraclePoolParams(OracleConnectionParams):
    """OracleDB pool parameters."""

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


class OracleSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for Oracle sync connections."""

    __slots__ = ("_conn",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._conn: OracleSyncConnection | None = None

    def __enter__(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        self._conn = self._config.connection_instance.acquire()
        return cast("OracleSyncConnection", self._conn)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._conn:
            if self._config.connection_instance:
                self._config.connection_instance.release(self._conn)
            self._conn = None
        return None


class _OracleSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_conn",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._conn: OracleSyncConnection | None = None

    def acquire_connection(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        self._conn = self._config.connection_instance.acquire()
        return cast("OracleSyncConnection", self._conn)

    def release_connection(self, _conn: "OracleSyncConnection", **kwargs: Any) -> None:
        if self._conn is None:
            return
        if self._config.connection_instance:
            self._config.connection_instance.release(self._conn)
        self._conn = None


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "OracleSyncConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections."""

    __slots__ = ("_oracle_version_cache", "_pool_session_callback", "_user_connection_hook")

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
        connection_config = normalize_connection_config(connection_config)
        self._oracle_version_cache = OracleVersionCache()
        self._pool_session_callback = cast(
            "Callable[[OracleSyncConnection, str], None] | None", connection_config.pop("session_callback", None)
        )
        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
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
            "OraclePoolParams": OraclePoolParams,
            "OracleSyncConnectionContext": OracleSyncConnectionContext,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleSyncSessionContext": OracleSyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)
    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""
        config = dict(self.connection_config)

        config.pop("threaded", None)
        config["session_callback"] = self._init_connection

        return oracledb.create_pool(**config)

    def _init_connection(self, connection: "OracleSyncConnection", tag: str) -> None:
        """Initialize connection with type handlers and cached server metadata.

        Registers vector, JSON, and UUID handlers. Vector and JSON handlers run
        unconditionally — both gate any optional dependencies (NumPy in the
        vector case) internally. UUID registration remains gated for
        backwards compatibility with existing user configurations.

        Caches ``connection._sqlspec_oracle_major`` so the JSON input handler
        can pick the right binding path (``DB_TYPE_JSON`` on 21c+, OSON-encoded
        ``DB_TYPE_BLOB`` on 19c-20c, JSON-string ``DB_TYPE_CLOB`` on 12c-18c)
        without re-querying server metadata on every bind. Caches
        ``connection._sqlspec_vector_return_format`` so the vector output
        handler can dispatch to ``numpy`` / ``list`` / ``array`` without
        re-reading driver-feature defaults on every fetch.

        Args:
            connection: Oracle connection to initialize.
            tag: Connection tag for session state.
        """
        register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        # Stash detected major version on the connection so the JSON input handler
        # can pick the right binding path without per-bind metadata queries.
        setattr(connection, "_sqlspec_oracle_major", _resolve_connection_major(self._oracle_version_cache, connection))
        # Stash the vector-read format so the VECTOR output handler can
        # dispatch without re-reading driver-feature defaults on every fetch.
        setattr(connection, "_sqlspec_vector_return_format", self.driver_features.get("vector_return_format"))

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
        """Close the actual connection pool."""
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None
        self._oracle_version_cache.reset()



class OracleAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Oracle connections."""

    __slots__ = ()


class _OracleAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ()


# mypyc annotations are unnecessary here because adapter config modules stay interpreted.
class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "OracleAsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections."""

    __slots__ = ("_oracle_version_cache", "_pool_session_callback", "_user_connection_hook")

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
        connection_config = normalize_connection_config(connection_config)
        self._oracle_version_cache = OracleVersionCache()
        self._pool_session_callback = cast(
            "Callable[[OracleAsyncConnection, str], Any] | None", connection_config.pop("session_callback", None)
        )

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
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
            "OraclePoolParams": OraclePoolParams,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)
    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""
        config = dict(self.connection_config)

        config.pop("threaded", None)
        config["session_callback"] = self._init_connection

        return oracledb.create_pool_async(**config)

    async def _init_connection(self, connection: "OracleAsyncConnection", tag: str) -> None:
        """Initialize async connection with type handlers and cached server metadata.

        Registers vector, JSON, and UUID handlers. Vector and JSON registration
        is unconditional — both gate any optional dependencies (NumPy in the
        vector case) internally. Caches ``connection._sqlspec_oracle_major`` so
        the JSON input handler can pick the right binding path on every bind
        without round-tripping server metadata. Caches
        ``connection._sqlspec_vector_return_format`` so the vector output
        handler dispatches to the user-selected return type without re-reading
        driver-feature defaults on every fetch.

        Args:
            connection: Oracle async connection to initialize.
            tag: Connection tag for session state.
        """
        register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        # Stash detected major version on the connection so the JSON input handler
        # can pick the right binding path without per-bind metadata queries.
        setattr(connection, "_sqlspec_oracle_major", _resolve_connection_major(self._oracle_version_cache, connection))
        # Stash the vector-read format so the VECTOR output handler can
        # dispatch without re-reading driver-feature defaults on every fetch.
        setattr(connection, "_sqlspec_vector_return_format", self.driver_features.get("vector_return_format"))

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
        """Close the actual async connection pool."""
        if self.connection_instance:
            await self.connection_instance.close()
            self.connection_instance = None
        self._oracle_version_cache.reset()



def _resolve_connection_major(cache: "OracleVersionCache", connection: Any) -> "int | None":
    """Resolve the Oracle server major for connection-setup type handlers.

    Prefers the pool-scoped version cache once a driver has resolved it through
    the data-dictionary path. Before that, the major is parsed from
    ``connection.version`` — a connection attribute populated at connect time, so
    no query is issued — using the same version parser the data dictionary uses.
    Returns ``None`` when unavailable; callers treat ``None`` as "assume 21c+".
    """
    if cache.resolved and cache.version is not None:
        return cache.version.major
    try:
        version_str = connection.version
    except AttributeError:
        return None
    if not version_str:
        return None
    components = parse_oracle_version_components(str(version_str))
    return components[0] if components is not None else None
