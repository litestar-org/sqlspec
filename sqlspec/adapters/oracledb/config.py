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
    """OracleDB connection parameters forwarded to ``python-oracledb``."""

    dsn: NotRequired[str]
    """Easy Connect string, TNS alias, or full connect descriptor."""
    pool_alias: NotRequired[str]
    """Named Thick-mode pool alias registered in the driver cache."""
    user: NotRequired[str]
    """Database username for authentication."""
    proxy_user: NotRequired[str]
    """Proxy target username appended inside ``[proxy_user]`` credentials."""
    password: NotRequired[str]
    """Database password for the authenticating user."""
    newpassword: NotRequired[str]
    """Replacement password applied atomically during connection establishment."""
    wallet_password: NotRequired[str]
    """Passphrase decrypting an encrypted PEM ``ewallet.pem`` or PKCS#12 ``ewallet.p12`` wallet."""
    access_token: NotRequired[OracleAccessToken]
    """OAuth 2.0 or IAM bearer token string, ``(token, private_key)`` tuple, or refresh callable."""
    host: NotRequired[str]
    """Oracle listener hostname or IP address when building a connect descriptor."""
    port: NotRequired[int]
    """Oracle listener TCP/TCPS port (defaults to ``1521``)."""
    protocol: NotRequired[OracleProtocol]
    """Transport protocol (``'tcp'`` or ``'tcps'`` for TLS/mTLS)."""
    https_proxy: NotRequired[str]
    """HTTPS proxy hostname used when tunneling TCPS connections."""
    https_proxy_port: NotRequired[int]
    """HTTPS proxy TCP port used with ``https_proxy``."""
    service_name: NotRequired[str]
    """Oracle database service name registered with the listener."""
    instance_name: NotRequired[str]
    """Specific RAC instance name targeted within the service."""
    sid: NotRequired[str]
    """Legacy Oracle System Identifier used when ``service_name`` is unavailable."""
    server_type: NotRequired[OracleServerType]
    """Server handler mode (``'dedicated'``, ``'shared'``, or ``'pooled'`` for DRCP)."""
    cclass: NotRequired[str]
    """Database Resident Connection Pooling (DRCP) connection class name."""
    purity: NotRequired[Purity]
    """DRCP session purity (``PURITY_NEW`` or ``PURITY_SELF``)."""
    expire_time: NotRequired[int]
    """Dead-connection detection probe interval in minutes."""
    externalauth: NotRequired[bool]
    """Authenticate via OS credentials or an Oracle external password store wallet."""
    mode: NotRequired[AuthMode]
    """Administrative privilege mode such as ``AUTH_MODE_SYSDBA`` or ``AUTH_MODE_SYSOPER``."""
    wallet_location: NotRequired[str]
    """Directory path containing ``ewallet.pem`` or ``ewallet.p12`` for mTLS."""
    config_dir: NotRequired[str]
    """Directory path containing ``tnsnames.ora`` and ``sqlnet.ora`` configuration files."""
    tcp_connect_timeout: NotRequired[float]
    """Maximum seconds to wait when establishing the initial TCP socket."""
    retry_count: NotRequired[int]
    """Number of connection-level retry attempts across listener endpoints."""
    retry_delay: NotRequired[int]
    """Delay in seconds between connection retry attempts."""
    ssl_server_dn_match: NotRequired[bool]
    """Verify that the server certificate distinguished name matches the target service."""
    ssl_server_cert_dn: NotRequired[str]
    """Expected distinguished name on the Oracle server TLS certificate."""
    events: NotRequired[bool]
    """Enable Thick-mode Continuous Query Notification and FAN HA event delivery."""
    disable_oob: NotRequired[bool]
    """Disable TCP out-of-band break messages on networks that drop urgent packets."""
    stmtcachesize: NotRequired[int]
    """Per-connection cursor statement cache size."""
    edition: NotRequired[str]
    """Edition-Based Redefinition (EBR) edition name activated on connect."""
    tag: NotRequired[str]
    """Pool session state tag requested when acquiring a connection."""
    matchanytag: NotRequired[bool]
    """Allow acquiring a pooled connection with a different tag when an exact match is unavailable."""
    appcontext: NotRequired[list[OracleAppContext]]
    """Initial ``(namespace, name, value)`` application context entries set on connect."""
    shardingkey: NotRequired[list[Any]]
    """Primary sharding key values routing the connection to an Oracle shard."""
    supershardingkey: NotRequired[list[Any]]
    """Composite super-sharding key values for multi-tier sharded topologies."""
    debug_jdwp: NotRequired[str]
    """JDWP ``host:port`` endpoint attached for PL/SQL and Java stored-procedure debugging."""
    connection_id_prefix: NotRequired[str]
    """Prefix prepended to driver-generated end-to-end diagnostic connection identifiers."""
    ssl_context: NotRequired[Any]
    """Custom ``ssl.SSLContext`` overriding default Thin-mode TLS verification."""
    sdu: NotRequired[int]
    """Oracle Net Session Data Unit buffer size in bytes."""
    pool_boundary: NotRequired[OraclePoolBoundary]
    """Implicit DRCP/PRCP release boundary (``'statement'`` or ``'transaction'``)."""
    use_tcp_fast_open: NotRequired[bool]
    """Enable TCP Fast Open (TFO) for Oracle Autonomous Database connections."""
    ssl_version: NotRequired[TLSVersion]
    """Minimum or pinned ``ssl.TLSVersion`` negotiated for TCPS connections."""
    program: NotRequired[str]
    """Client program name reported in ``V$SESSION.PROGRAM``."""
    machine: NotRequired[str]
    """Client host name reported in ``V$SESSION.MACHINE``."""
    terminal: NotRequired[str]
    """Client terminal identifier reported in ``V$SESSION.TERMINAL``."""
    osuser: NotRequired[str]
    """Operating system username reported in ``V$SESSION.OSUSER``."""
    driver_name: NotRequired[str]
    """Client driver label reported in ``V$SESSION_CONNECT_INFO.CLIENT_DRIVER``."""
    use_sni: NotRequired[bool]
    """Send TLS Server Name Indication (SNI) to bypass double-handshake listener negotiation."""
    thick_mode_dsn_passthrough: NotRequired[bool]
    """Forward the raw DSN string unchanged to Oracle Client libraries in Thick mode."""
    extra_auth_params: NotRequired[dict[str, Any]]
    """Additional provider parameters passed to cloud token authentication plugins."""
    pool_name: NotRequired[str]
    """Explicit server-side DRCP/PRCP pool name when multiple pools exist."""
    on_connect_callback: NotRequired[Callable[..., Any]]
    """Driver-level callback invoked by ``python-oracledb`` immediately on physical connect."""
    handle: NotRequired[int]
    """Raw OCI service context pointer wrapped in Thick mode."""
    extra: NotRequired[dict[str, Any]]
    """Additional keyword arguments merged into ``oracledb.connect`` or ``create_pool``."""
    topology: NotRequired[OracleTopologyConfig | dict[str, Any]]
    """Multi-endpoint RAC/Data Guard topology descriptor compiled into a TNS ``DESCRIPTION``."""
    wallet_zip: NotRequired[str | Path | bytes]
    """Autonomous Database wallet ZIP path, directory, or raw bytes extracted into a secure temporary directory."""
    tns_alias: NotRequired[str]
    """Alias resolved from ``tnsnames.ora`` inside ``config_dir`` or an extracted ``wallet_zip``."""


class OraclePoolParams(OracleConnectionParams):
    """OracleDB session pool parameters extending ``OracleConnectionParams``."""

    thick_mode: NotRequired[bool]
    """Initialize Oracle Client Thick mode via ``oracledb.init_oracle_client()`` before creating the pool."""
    lib_dir: NotRequired[str]
    """Filesystem path to Oracle Instant Client libraries implying Thick mode."""
    pool_class: NotRequired[type[Any]]
    """Custom ``ConnectionPool`` subclass passed to ``python-oracledb``."""
    params: NotRequired[oracledb.PoolParams]
    """Pre-constructed ``oracledb.PoolParams`` instance merged with keyword settings."""
    min: NotRequired[int]
    """Minimum number of sessions maintained in the pool."""
    max: NotRequired[int]
    """Maximum number of concurrent sessions permitted in the pool."""
    increment: NotRequired[int]
    """Number of new sessions opened at once when all pooled sessions are busy."""
    connectiontype: NotRequired[type[Any]]
    """Custom ``oracledb.Connection`` subclass instantiated for pooled sessions."""
    getmode: NotRequired[PoolGetMode]
    """Behavior when the pool is at capacity (``POOL_GETMODE_WAIT``, ``NOWAIT``, ``FORCEGET``, or ``TIMEDWAIT``)."""
    homogeneous: NotRequired[bool]
    """Use identical database credentials across all pooled sessions (defaults to ``True``)."""
    timeout: NotRequired[int]
    """Idle seconds after which surplus sessions above ``min`` are closed."""
    wait_timeout: NotRequired[int]
    """Maximum milliseconds to wait for an available session under ``POOL_GETMODE_TIMEDWAIT``."""
    max_lifetime_session: NotRequired[int]
    """Maximum lifetime in seconds before a pooled session is retired."""
    session_callback: NotRequired[Callable[..., Any]]
    """Callback ``(connection, requested_tag)`` invoked when a pooled session is acquired."""
    max_sessions_per_shard: NotRequired[int]
    """Maximum pooled sessions allocated to any single Oracle shard."""
    soda_metadata_cache: NotRequired[bool]
    """Enable Thick-mode SODA collection metadata caching across pooled sessions."""
    ping_interval: NotRequired[int]
    """Idle seconds elapsed before a pooled session is health-checked on checkout."""
    ping_timeout: NotRequired[int]
    """Maximum milliseconds allowed for a checkout health-check ping before replacing the session."""


class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags controlling VECTOR/UUID codecs, LOB coercion, and enterprise hooks.

    Native async pipeline execution is runtime-gated on Oracle Database 26ai+ in Thin mode and can be disabled via ``SQLSPEC_ORACLE_DISABLE_PIPELINE``.
    """

    enable_numpy_vectors: NotRequired[bool]
    """Register bidirectional NumPy ``ndarray`` handlers for Oracle 23ai+ ``VECTOR`` columns (defaults to ``True`` when NumPy is installed)."""
    enable_lowercase_column_names: NotRequired[bool]
    """Normalize unquoted uppercase Oracle column names to lowercase while preserving quoted aliases (defaults to ``True``)."""
    enable_uuid_binary: NotRequired[bool]
    """Convert Python ``UUID`` values to and from ``RAW(16)`` columns."""
    vector_return_format: NotRequired[OracleVectorReturnFormat]
    """Decoded type for dense ``VECTOR`` columns (``'numpy'``, ``'list'``, or ``'array'``)."""
    oracle_varchar2_byte_limit: NotRequired[int]
    """UTF-8 byte threshold above which ``str`` parameters bind as ``DB_TYPE_CLOB`` (defaults to ``4000``)."""
    oracle_raw_byte_limit: NotRequired[int]
    """Byte threshold above which ``bytes`` parameters bind as ``DB_TYPE_BLOB`` (defaults to ``2000``)."""
    arraysize: NotRequired[int]
    """Per-cursor ``arraysize`` row fetch buffer override."""
    prefetchrows: NotRequired[int]
    """Per-cursor ``prefetchrows`` initial round-trip prefetch count override."""
    fetch_lobs: NotRequired[bool]
    """Return LOB columns as locator objects when ``True`` or inline ``str``/``bytes`` when ``False``."""
    fetch_decimals: NotRequired[bool]
    """Return ``NUMBER`` columns as ``decimal.Decimal`` values when ``True``."""
    on_connection_create: NotRequired[Callable[..., Any]]
    """Hook ``(connection, tag)`` invoked after built-in JSON, VECTOR, and UUID handlers are registered."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channels (separate from ``connection_config['events']`` FAN/CQN notifications)."""
    events_backend: NotRequired[OracleEventsBackend]
    """Event queue backend (``'poll_queue'`` default, ``'aq'``, or ``'txeventq'``)."""
    enable_direct_path_load: NotRequired[bool]
    """Route ``load_from_arrow`` through Thin-mode ``Connection.direct_path_load`` with ``executemany`` fallback (defaults to ``True``)."""
    session_context_hook: NotRequired[OracleSessionContextConfig | dict[str, Any]]
    """Declarative ``DBMS_SESSION`` / ``CLIENT_IDENTIFIER`` context applied on checkout and cleared on release."""
    enable_session_tracing: NotRequired[bool]
    """Piggyback active correlation and trace identifiers onto ``module``, ``action``, ``client_identifier``, and ``econtext_id`` in zero extra RTTs."""
    dbms_cloud_credential: NotRequired[str | None]
    """Default ``DBMS_CLOUD`` credential name used for server-side ``DBMS_CLOUD.COPY_DATA`` object-store loads."""


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
